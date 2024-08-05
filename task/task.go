package task

import (
	"time"

	"github.com/docker/go-connections/nat"
	"github.com/google/uuid"
)

type State int

const (
	Pending = iota
	Scheduled
	Running
	Completed
	Failed
)

type Task struct {
	ID uuid.UUID
	ContainerID string
	Name string
	State State
	Image string
	CPU float64
	Memory int64
	Disk int64
	ExposedPorts nat.PortSet
	PortBindings map[string]string
	RestartPolicy string
	StartTime time.Time
	FinishTime time.Time
}

type TaskEvent struct {
	ID uuid.UUID
	State State // Represent the transition of the state
	Timestamp time.Time
	Task Task
}
