package worker

import (
	"bufio"
	"encoding/json"
	"fmt"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"io"
	"io/ioutil"
	"log"
	"net"
	"os"
	"os/exec"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
        "math/rand"
        "os/signal"

	"github.com/satori/go.uuid"
	"gopkg.in/yaml.v2"

	"github.com/lhzd863/autoflow/glog"
        "github.com/lhzd863/autoflow/db"
	"github.com/lhzd863/autoflow/util"

        "github.com/lhzd863/autoflow-worker/gproto"
	"github.com/lhzd863/autoflow-worker/module"
)

var (
	LogF       string
	HomeDir    string
	ProcessNum int
	Ip         string
	Port       string
	WorkerId   string
	conf       *module.MetaWorkerConf
	jobpool    = db.NewMemDB()
        mgr        *MgrWorkerServer
)

type WorkerServer struct {
	sync.RWMutex
}

func NewWorkerServer(paraMap map[string]interface{}) *WorkerServer {
	conf = new(module.MetaWorkerConf)
	yamlFile, err := ioutil.ReadFile(paraMap["cfg"].(string))
	if err != nil {
		log.Printf("error: %s", err)
		return &WorkerServer{}
	}
	err = yaml.UnmarshalStrict(yamlFile, conf)
	if err != nil {
		log.Printf("error: %s", err)
		return &WorkerServer{}
	}
	LogF = conf.HomeDir + "/worker_${" + util.ENV_VAR_DATE + "}.log"
	if ok, _ := util.PathExists(conf.HomeDir + "/LOG"); !ok {
		os.Mkdir(conf.HomeDir+"/LOG", os.ModePerm)
	}

	WorkerId = conf.Name
	Ip = conf.Ip
	Port = conf.Port
	HomeDir = conf.HomeDir
        mgr = NewMgrWorkerServer(paraMap)

	m := &WorkerServer{}
	return m
}

func (ws *WorkerServer) Ping(ctx context.Context, in *gproto.Req) (*gproto.Res, error) {
	return &gproto.Res{Status_Txt: "ok", Status_Code: 200, Data: "{}"}, nil
}

func (ws *WorkerServer) JobExecLog(stream gproto.Worker_JobExecLogServer) error {
        return nil
}

func (ws *WorkerServer) JobStart(ctx context.Context, in *gproto.Req) (*gproto.Res, error) {
	p := new(module.MetaParaWorkerJobBean)
	err := json.Unmarshal([]byte(in.JsonStr), &p)
	if err != nil {
		_, cfile, cline, _ := runtime.Caller(1)
		glog.Glog(LogF, fmt.Sprintf("%v %v %v", cfile, cline, err))
	}
	retcdstr, err := ws.executeJob(p)
	var status_code int32
	status_code = 0
	status_txt := ""
	if err != nil {
		var retcd int32
		retcd1, err := strconv.ParseInt(retcdstr, 10, 64)
		retcd = int32(retcd1)
		if err != nil {
			retcd = 1
		}
		status_code = retcd
		status_txt = fmt.Sprint(err)
	}
	return &gproto.Res{Status_Txt: status_txt, Status_Code: status_code, Data: "{}"}, nil
}

func (ws *WorkerServer) executeJob(job *module.MetaParaWorkerJobBean) (string, error) {
	m := new(module.MetaWorkerJobBean)
	u1 := uuid.Must(uuid.NewV4(), nil)
	m.Id = fmt.Sprint(u1)
	m.Sys = job.Sys
	m.Job = job.Job
	m.Ip = Ip
	m.Port = Port
	m.WorkerId = WorkerId
	m.Cmd = job.Cmd
	timeStr0 := time.Now().Format("2006-01-02 15:04:05")
	m.StartTime = timeStr0
	m.Parameter = job.Parameter
	m.Retry = job.Retry
	m.Context = job.Context
	m.Timeout = job.Timeout

        jobpool.Add(m.Id, m)
	defer jobpool.Remove(m.Id)

        exitChan := make(chan int)
        StopFlag := 0
        go func() {
                <-exitChan
                StopFlag = 1
                mgr.WorkerJobRunningRegisterRemove(m)
        }()
        go func() {
                st := time.Now().Unix()
                et := time.Now().Unix()
                for {
                        if StopFlag == 1 {
                                break
                        }
                        et = time.Now().Unix()
                        if et-st > 30 {
                                mgr.WorkerJobRunningRegister(m)
                                st = time.Now().Unix()
                        }
                        rand.Seed(time.Now().UnixNano())
                        ri := rand.Intn(2)
                        time.Sleep(time.Duration(ri) * time.Second)
                }
        }()

	loc, _ := time.LoadLocation("Local")
	timeLayout := "2006-01-02 15:04:05"
	ctxUnixTime, _ := time.ParseInLocation(timeLayout, m.Context, loc)
	ctxDateStr := ctxUnixTime.Format("20060102")
	ctxTimeStr := ctxUnixTime.Format("150405")
	ctxTimeStampStr := ctxUnixTime.Format("20060102150405")

	logDir := fmt.Sprintf("%v/LOG/%v/%v/%v", HomeDir, ctxDateStr, ctxTimeStr, job.Sys)

	exist, err := util.PathExists(logDir)
	if err != nil {
		return "1", fmt.Errorf("failed to path exists: %v", err)
	}
	if !exist {
		os.MkdirAll(logDir, os.ModePerm)
	}
	f := fmt.Sprintf("%v/%v_%v.log", logDir, m.Job, ctxTimeStampStr)
	for i := 0; i < len(m.Cmd); i++ {
		glog.Glog(f, fmt.Sprintf("#%v.%v %v ID(%v) step.%v will start.", job.Sys, job.Job, job.Context, m.Id, i))
		c := m.Cmd[i].(string)
		regt := regexp.MustCompile("\\$\\{" + ENV_VAR_CTX_DATE + "\\}")
		c = regt.ReplaceAllString(c, ctxDateStr)

		regt = regexp.MustCompile("\\$\\{" + ENV_VAR_CTX_TIME + "\\}")
		c = regt.ReplaceAllString(c, ctxTimeStr)

		regt = regexp.MustCompile("\\$\\{" + ENV_VAR_CTX_TIMESTAMP + "\\}")
		c = regt.ReplaceAllString(c, ctxTimeStampStr)

		regt = regexp.MustCompile("\\$\\{" + ENV_VAR_CTX_CTL + "\\}")
		c = regt.ReplaceAllString(c, fmt.Sprintf("%v.%v.%v.%v", m.Sys, m.Job, ctxDateStr, ctxTimeStr))

		regt = regexp.MustCompile("\\$\\{" + ENV_VAR_CTX_SYS + "\\}")
		c = regt.ReplaceAllString(c, m.Sys)

		regt = regexp.MustCompile("\\$\\{" + ENV_VAR_CTX_JOB + "\\}")
		c = regt.ReplaceAllString(c, m.Job)

		regt = regexp.MustCompile("\\$\\{" + EVN_VAR_CTX_STR + "\\}")
		c = regt.ReplaceAllString(c, m.Context)
		for j := len(job.Parameter) - 1; j >= 0; j-- {
			kv := new(module.KVBean)
			err := json.Unmarshal([]byte(job.Parameter[j].(string)), &kv)
			if err != nil {
				glog.Glog(f, fmt.Sprintf("parse kvbean error.%v", err))
				continue
			}
			//repalce variable
			vt := "\\$\\{" + kv.K + "\\}"
			reg := regexp.MustCompile(vt)
			c = reg.ReplaceAllString(c, kv.V)
		}
		for j := 0; j < len(job.Parameter); j++ {
			kv := new(module.KVBean)
			err := json.Unmarshal([]byte(job.Parameter[j].(string)), &kv)
			if err != nil {
				glog.Glog(f, fmt.Sprintf("parse kvbean error.%v", err))
				continue
			}
			//set env
			err = os.Setenv(kv.K, kv.V)
			glog.Glog(f, fmt.Sprintf("%v = %v", kv.K, kv.V))
			if err != nil {
				glog.Glog(f, fmt.Sprintf("job %v %v set env %v=%v error.%v", job.Sys, job.Job, kv.K, kv.V, err))
				continue
			}
		}
		//force env
		glog.Glog(f, fmt.Sprintf("Set force default env."))
		glog.Glog(f, fmt.Sprintf("%v=%v", ENV_VAR_CTX_DATE, ctxDateStr))
		os.Setenv(ENV_VAR_CTX_DATE, ctxDateStr)

		glog.Glog(f, fmt.Sprintf("%v=%v", ENV_VAR_CTX_TIME, ctxTimeStr))
		os.Setenv(ENV_VAR_CTX_TIME, ctxTimeStr)

		glog.Glog(f, fmt.Sprintf("%v=%v", ENV_VAR_CTX_TIMESTAMP, ctxTimeStampStr))
		os.Setenv(ENV_VAR_CTX_TIMESTAMP, ctxTimeStampStr)

		glog.Glog(f, fmt.Sprintf("%v=%v.%v.%v.%v", ENV_VAR_CTX_CTL, m.Sys, m.Job, ctxDateStr, ctxTimeStr))
		os.Setenv(ENV_VAR_CTX_CTL, fmt.Sprintf("%v.%v.%v.%v", m.Sys, m.Job, ctxDateStr, ctxTimeStr))

		glog.Glog(f, fmt.Sprintf("%v=%v", ENV_VAR_CTX_SYS, m.Sys))
		os.Setenv(ENV_VAR_CTX_SYS, m.Sys)

		glog.Glog(f, fmt.Sprintf("%v=%v", ENV_VAR_CTX_JOB, m.Job))
		os.Setenv(ENV_VAR_CTX_JOB, m.Job)

		glog.Glog(f, fmt.Sprintf("%v=%v", EVN_VAR_CTX_STR, m.Context))
		os.Setenv(EVN_VAR_CTX_STR, m.Context)
		var n int8 = 0
		if m.Retry < 1 {
			glog.Glog(f, fmt.Sprintf("%v.%v %v ID(%v) retry time lt 1.%v", m.Sys, m.Job, m.Context, m.Id, err))
			return "1", fmt.Errorf("%v.%v %v ID(%v) retry time lt 1.%v", m.Sys, m.Job, m.Context, m.Id, err)
		}
		retcdstr := "0"
		for ; n < m.Retry; n++ {
			timeStr := time.Now().Format("20060102150405")
			jobLogF := fmt.Sprintf("%v/%v_%v_%v_%v.log", logDir, strings.ToLower(m.Job), i, n, timeStr)
			retcdstr, err = ws.executeCmd(m.Id, c, jobLogF)
			if err != nil {
				glog.Glog(f, fmt.Sprintf("%v.%v %v ID(%v) running fail %v time.", m.Sys, m.Job, m.Context, m.Id, n))
				continue
			}
			break
		}
		if err != nil {
			glog.Glog(f, fmt.Sprintf("%v.%v %v ID(%v) step.%v run fail.%v", m.Sys, m.Job, m.Context, m.Id, i, err))
			glog.Glog(f, fmt.Sprintf("\n"))
			return retcdstr, err
		}
		glog.Glog(f, fmt.Sprintf("%v.%v %v ID(%v) step.%v run successfully.", m.Sys, m.Job, m.Context, m.Id, i))
	}
	glog.Glog(f, fmt.Sprintf("\n"))
	return "0", nil
}

func (ws *WorkerServer) executeCmd(id string, c string, logf string) (string, error) {
	glog.Glog(logf, fmt.Sprintf("%v", id))
	glog.Glog(logf, fmt.Sprintf("%v", c))

	j := jobpool.Get(id).(*module.MetaWorkerJobBean)

	var cmd *exec.Cmd
	if j.Timeout > 0 {
		glog.Glog(logf, fmt.Sprintf("%v.%v set timeout %v.", j.Sys, j.Job, j.Timeout))
		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(j.Timeout)*time.Second)
		defer cancel()
		cmdarray := []string{"-c", c}
		cmd = exec.CommandContext(ctx, "/bin/bash", cmdarray...)
	} else {
		cmd = exec.Command("/bin/bash", "-c", c)
	}
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	j.Command = cmd
	j.CmdRunning = c
        jobpool.Add(id,j)

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		_, cfile, cline, _ := runtime.Caller(1)
		glog.Glog(logf, fmt.Sprintf("%v %v %v", cfile, cline, err))
		return "1", fmt.Errorf("%v", err)
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		_, cfile, cline, _ := runtime.Caller(1)
		glog.Glog(logf, fmt.Sprintf("%v %v %v", cfile, cline, err))
		return "1", fmt.Errorf("%v", err)
	}
	cmd.Start()

	reader := bufio.NewReader(stdout)
	go func() {
		for {
			line, err2 := reader.ReadString('\n')
			if err2 != nil || io.EOF == err2 {
				break
			}
			glog.Glog(logf, fmt.Sprintf("%v", line))
		}
	}()
	readererr := bufio.NewReader(stderr)
	go func() {
		for {
			line, err2 := readererr.ReadString('\n')
			if err2 != nil || io.EOF == err2 {
				break
			}
			glog.Glog(logf, fmt.Sprintf("%v", line))
		}

	}()

	cmd.Wait()
	retcd := string(fmt.Sprintf("%v", cmd.ProcessState.Sys().(syscall.WaitStatus).ExitStatus()))
	retcd = strings.Replace(retcd, " ", "", -1)
	retcd = strings.Replace(retcd, "\n", "", -1)
	if retcd != "0" {
		glog.Glog(logf, fmt.Sprintf("%v", retcd))
		return fmt.Sprintf("%v", cmd.ProcessState.Sys().(syscall.WaitStatus).ExitStatus()), fmt.Errorf("%v", retcd)
	}
	return "0", nil
}

func (ws *WorkerServer) JobStop(ctx context.Context, in *gproto.Req) (*gproto.Res, error) {
	p := new(module.MetaWorkerStopJobBean)
	err := json.Unmarshal([]byte(in.JsonStr), &p)
	if err != nil {
		return &gproto.Res{Status_Txt: fmt.Sprint(err), Status_Code: 1, Data: "{}"}, nil
	}
	flag := 0
        for k := range jobpool.MemMap {
                j := (jobpool.MemMap[k]).(*module.MetaWorkerJobBean)
                if k == p.Id {
                        //jpo.Cmd.Process.Kill()
                        flag = 1
                        glog.Glog(LogF, fmt.Sprintf("stop %v.%v pid(%v).", j.Sys, j.Job, j.Command.Process.Pid))
                        syscall.Kill(-j.Command.Process.Pid, syscall.SIGKILL)
                        jobpool.Remove(p.Id)
                        break
                }
        }
	if flag == 0 {
		return &gproto.Res{Status_Txt: fmt.Sprintf("no id(%v) exists.", p.Id), Status_Code: 1, Data: "{}"}, nil
	}
	return &gproto.Res{Status_Txt: "", Status_Code: 0, Data: "{}"}, nil
}

func (ws *WorkerServer) JobStatus(ctx context.Context, in *gproto.Req) (*gproto.Res, error) {
	p := new(module.MetaWorkerJobBean)
	err := json.Unmarshal([]byte(in.JsonStr), &p)
	if err != nil {
		glog.Glog(LogF, fmt.Sprint(err))
		return &gproto.Res{Status_Txt: fmt.Sprint(err), Status_Code: 1, Data: "{}"}, nil
	}
	var jsonstr []byte
	var retlst = make([]interface{}, 0)
        for k := range jobpool.MemMap {
                j := (jobpool.MemMap[k]).(*module.MetaWorkerJobBean)
                if j.Sys == p.Sys && j.Job == p.Job {
                        glog.Glog(LogF, fmt.Sprintf("status %v.%v.", j.Sys, j.Job))
                        jsonstr, _ = json.Marshal(j)
                        retlst = append(retlst, string(jsonstr))
                }
        }
	arrstr, _ := json.Marshal(retlst)
	return &gproto.Res{Status_Txt: "", Status_Code: 0, Data: string(arrstr)}, nil
}

func (ws *WorkerServer) Main() bool {
	var wg util.WaitGroupWrapper
        StopFlag := 0
        m := new(module.MetaWorkerHeartBean)
        u1 := uuid.Must(uuid.NewV4(),nil)
        m.Id = fmt.Sprint(u1)
        timeStr0 := time.Now().Format("2006-01-02 15:04:05")
        m.StartTime = timeStr0
        m.WorkerId = WorkerId
        m.Ip = Ip
        m.Port = Port

        exitChan := make(chan int)
        signalChan := make(chan os.Signal, 1)
        go func() {
                <-signalChan
                StopFlag = 1
                mgr.RegisterRemove(m)
                exitChan <- 1
        }()
        signal.Notify(signalChan, os.Interrupt, os.Kill, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM)

        go func() {
                st := time.Now().Unix()
                et := time.Now().Unix()
                for {
                        if StopFlag == 1 {
                                return
                        }
                        et = time.Now().Unix()
                        if et-st > 30 {
                                ret := mgr.Register(m)
                                if !ret {
                                        glog.Glog(LogF, "register worker fail.")
                                }
                                st = time.Now().Unix()
                        }
                        rand.Seed(time.Now().UnixNano())
                        ri := rand.Intn(2)
                        time.Sleep(time.Duration(ri) * time.Second)
                }
        }()

	lis, err := net.Listen("tcp", ":"+Port) //监听所有网卡8028端口的TCP连接
	if err != nil {
		glog.Glog(LogF, fmt.Sprintf("监听失败: %v", err))
		return false
	}
	ss := grpc.NewServer() //创建gRPC服务

	/**注册接口服务
	 * 以定义proto时的service为单位注册，服务中可以有多个方法
	 * (proto编译时会为每个service生成Register***Server方法)
	 * 包.注册服务方法(gRpc服务实例，包含接口方法的结构体[指针])
	 */
	gproto.RegisterWorkerServer(ss, &WorkerServer{})
	/**如果有可以注册多个接口服务,结构体要实现对应的接口方法
	 */
	// 在gRPC服务器上注册反射服务
	reflection.Register(ss)
	// 将监听交给gRPC服务处理
	wg.Wrap(func() {
		err = ss.Serve(lis)
		if err != nil {
			glog.Glog(LogF, fmt.Sprintf("failed to serve: %v", err))
			return
		}
	})

        <-exitChan
        ss.Stop()
	wg.Wait()
	return true
}
