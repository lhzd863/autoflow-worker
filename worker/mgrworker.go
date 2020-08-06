package worker

import (
        "encoding/json"
        "fmt"
        "sync"
        "time"

        "github.com/lhzd863/autoflow/glog"
        "github.com/lhzd863/autoflow/util"

        "github.com/lhzd863/autoflow-worker/module"
)

var (
        AccessToken   string
        ApiServerIp   string
        ApiServerPort string
)
type MgrWorkerServer struct {
        sync.RWMutex
}

func NewMgrWorkerServer(paraMap map[string]interface{}) *MgrWorkerServer {
        AccessToken = paraMap["accesstoken"].(string)
        ApiServerIp = paraMap["apiserverip"].(string)
        ApiServerPort = paraMap["apiserverport"].(string)

        m := &MgrWorkerServer{}
        return m
}

func (ws *MgrWorkerServer) WorkerJobRunningRegister(p *module.MetaWorkerJobBean) bool {
        
        m:=new(module.MetaSystemWorkerRoutineJobRunningHeartBean)
        m.Id = p.Id
        m.WorkerId = p.WorkerId
        m.Sys = p.Sys
        m.Job = p.Job
        m.Ip = Ip
        m.Port = Port
        m.StartTime = p.StartTime
        m.UpdateTime = time.Now().Format("2006-01-02 15:04:05")

        glog.Glog(LogF, fmt.Sprintf("Register node %v, %v", Ip, Port))
        url := fmt.Sprintf("http://%v:%v/api/v1/worker/routine/job/running/heart/add?accesstoken=%v", ApiServerIp, ApiServerPort,AccessToken)
        loc, _ := time.LoadLocation("Local")
        timeLayout := "2006-01-02 15:04:05"
        stheTime, _ := time.ParseInLocation(timeLayout, m.StartTime, loc)
        sst := stheTime.Unix()
        timeStr := time.Now().Format("2006-01-02 15:04:05")
        etheTime, _ := time.ParseInLocation(timeLayout, timeStr, loc)
        est := etheTime.Unix()

        if est-sst <= 60 {
                m.Duration = fmt.Sprintf("%vs", est-sst)
        } else if est-sst <= 3600 {
                m.Duration = fmt.Sprintf("%vmin", (est-sst)/60)
        } else {
                m.Duration = fmt.Sprintf("%vh", (est-sst)/3600)
        }

        jsonstr0, err := json.Marshal(m)
        if err != nil {
                glog.Glog(LogF, fmt.Sprint(err))
                return false
        }

        jsonstr, err := util.Api_RequestPost(url, string(jsonstr0))
        if err != nil {
                glog.Glog(LogF, fmt.Sprint(err))
                return false
        }
        retbn1 := new(module.RetBean)
        err = json.Unmarshal([]byte(jsonstr), &retbn1)
        if err != nil {
                glog.Glog(LogF, fmt.Sprint(err))
                return false
        }
        if retbn1.Status_Code != 200 {
                glog.Glog(LogF, fmt.Sprintf("post url return status code:%v", retbn1.Status_Code))
                return false
        }
        return true
}

func (ws *MgrWorkerServer) WorkerJobRunningRegisterRemove(p *module.MetaWorkerJobBean) bool {

        m := new(module.MetaSystemWorkerRoutineJobRunningHeartBean)
        m.Id = p.Id
        m.WorkerId = p.WorkerId
        m.Sys = p.Sys
        m.Job = p.Job
        m.Ip = Ip
        m.Job = Port

        glog.Glog(LogF, fmt.Sprintf("Register node %v, %v", Ip, Port))
        url := fmt.Sprintf("http://%v:%v/api/v1/worker/routine/job/running/heart/rm?accesstoken=%v", ApiServerIp, ApiServerPort, AccessToken)
        jsonstr0, err := json.Marshal(m)
        if err != nil {
                glog.Glog(LogF, fmt.Sprint(err))
                return false
        }
        jsonstr, err := util.Api_RequestPost(url, string(jsonstr0))
        if err != nil {
                glog.Glog(LogF, fmt.Sprint(err))
                return false
        }
        retbn1 := new(module.RetBean)
        err = json.Unmarshal([]byte(jsonstr), &retbn1)
        if err != nil {
                glog.Glog(LogF, fmt.Sprint(err))
                return false
        }
        if retbn1.Status_Code != 200 {
                glog.Glog(LogF, fmt.Sprintf("post url return status code:%v", retbn1.Status_Code))
                return false
        }
        return true
}

func (ws *MgrWorkerServer) Register(m *module.MetaWorkerHeartBean) bool {
        glog.Glog(LogF, fmt.Sprintf("Register node %v, %v", Ip, Port))
        url := fmt.Sprintf("http://%v:%v/api/v1/worker/heart/add?accesstoken=%v", ApiServerIp, ApiServerPort, AccessToken)
        m.MaxCnt = fmt.Sprint(ProcessNum)
        m.RunningCnt = fmt.Sprint(len(jobpool.MemMap))
        m.CurrentExecCnt = "0"
        loc, _ := time.LoadLocation("Local")
        timeLayout := "2006-01-02 15:04:05"
        stheTime, _ := time.ParseInLocation(timeLayout, m.StartTime, loc)
        sst := stheTime.Unix()
        timeStr := time.Now().Format("2006-01-02 15:04:05")
        etheTime, _ := time.ParseInLocation(timeLayout, timeStr, loc)
        est := etheTime.Unix()
        if est-sst <= 60 {
                m.Duration = fmt.Sprintf("%vs", est-sst)
        } else if est-sst <= 3600 {
                m.Duration = fmt.Sprintf("%vmin", (est-sst)/60)
        } else {
                m.Duration = fmt.Sprintf("%vh", (est-sst)/3600)
        }

        jsonstr0, err := json.Marshal(m)
        if err != nil {
                glog.Glog(LogF, fmt.Sprintf("json marshal %v", err))
                return false
        }
        jsonstr, err := util.Api_RequestPost(url, string(jsonstr0))
        if err != nil {
                glog.Glog(LogF, fmt.Sprint(err))
                return false
        }
        retbn1 := new(module.RetBean)
        err = json.Unmarshal([]byte(jsonstr), &retbn1)
        if err != nil {
                glog.Glog(LogF, fmt.Sprint(err))
                return false
        }
        if retbn1.Status_Code != 200 {
                glog.Glog(LogF, fmt.Sprintf("post url return status code:%v", retbn1.Status_Code))
                return false
        }
        return true
}

func (ws *MgrWorkerServer) RegisterRemove(m *module.MetaWorkerHeartBean) bool {
        glog.Glog(LogF, fmt.Sprintf("Register node %v, %v", Ip, Port))
        url := fmt.Sprintf("http://%v:%v/api/v1/worker/heart/rm?accesstoken=%v", ApiServerIp, ApiServerPort, AccessToken)
        jsonstr0, err := json.Marshal(m)
        if err != nil {
                glog.Glog(LogF, fmt.Sprintf("json marshal %v", err))
                return false
        }
        jsonstr, err := util.Api_RequestPost(url, string(jsonstr0))
        if err != nil {
                glog.Glog(LogF, fmt.Sprint(err))
                return false
        }
        retbn1 := new(module.RetBean)
        err = json.Unmarshal([]byte(jsonstr), &retbn1)
        if err != nil {
                glog.Glog(LogF, fmt.Sprint(err))
                return false
        }
        if retbn1.Status_Code != 200 {
                glog.Glog(LogF, fmt.Sprintf("post url return status code:%v", retbn1.Status_Code))
                return false
        }
        return true
}


