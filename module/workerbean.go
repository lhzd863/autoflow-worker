package module

import (
	"os/exec"
)

type MetaParaWorkerJobBean struct {
	Id        string        `json:"id"`
	FlowId    string        `json:"flowid"`
	WorkerId  string        `json:"workerid"`
	Sys       string        `josn:"sys"`
	Job       string        `json:"job"`
	Context   string        `json:"context"`
	Cmd       []interface{} `json:"cmd"`
	Parameter []interface{} `json:"parameter"`
	Timeout   int64         `json:"timeout"`
	Retry     int8          `json:"retry"`
}

type MetaWorkerJobBean struct {
	Id         string        `json:"id"`
	FlowId     string        `json:"flowid"`
	WorkerId   string        `json:"workerid"`
	Ip         string        `json:"ip"`
	Port       string        `json:"port"`
	Sys        string        `josn:"sys"`
	Job        string        `json:"job"`
	Context    string        `json:"context"`
	Cmd        []interface{} `json:"cmd"`
	Parameter  []interface{} `json:"parameter"`
	Timeout    int64         `json:"timeout"`
	Retry      int8          `json:"retry"`
	StartTime  string        `json:"starttime"`
	EndTime    string        `json:"endtime"`
	Status     string        `json:"status"`
	Command    *exec.Cmd     `json:"command"`
	CmdRunning string        `json:"cmdrunning"`
	RetCode    string        `json:"retcode"`
}

type KVBean struct {
	K string `json:"k"`
	V string `json:"v"`
}

type MetaWorkerConf struct {
	Version       string `yaml:"version"`
	Name          string `yaml:"name"`
	Ip            string `yaml:"ip"`
	Port          string `yaml:"port"`
	HomeDir       string `yaml:"homedir"`
	MaxProcess    int64  `yaml:"maxprocess"`
	AccessToken   string `yaml:"accesstoken"`
	ApiserverIp   string `yaml:"apiserverip"`
	ApiserverPort string `yaml:"apiserverport"`
}

type MetaWorkerStopJobBean struct {
	Id string `json:"id"`
}

type MetaWorkerHeartBean struct {
	Id             string `json:"id"`
	WorkerId       string `json:"workerid"`
	Ip             string `json:"ip"`
	Port           string `json:"port"`
	MaxCnt         string `json:"maxcnt"`
	RunningCnt     string `json:"runningcnt"`
	CurrentExecCnt string `json:"currentexeccnt"`
	StartTime      string `json:"starttime"`
	UpdateTime     string `json:"updatetime"`
	Duration       string `json:"duration"`
}

type MetaSystemWorkerRoutineJobRunningHeartBean struct {
	Id         string `json:"id"`
	WorkerId   string `json:"workerid"`
	Sys        string `json:"sys"`
	Job        string `json:"job"`
	Ip         string `json:"ip"`
	Port       string `json:"port"`
	StartTime  string `json:"starttime"`
	UpdateTime string `json:"updatetime"`
	Duration   string `json:"duration"`
}

type RetBean struct {
	Status_Txt  string      `json:"status_txt"`
	Status_Code int         `json:"status_code"`
	Data        interface{} `json:"data"`
}
