package manager

import (
	"cube/task"
	"fmt"

	"github.com/golang-collections/collections/queue"
	"github.com/google/uuid"
)

// 1. Accept requests from users to start and stop tasks
// 2. Schedule tasks onto worker machines
// 3. Keep track of tasks, their states, and the machine on which they run

type Manager struct {
	Pending       queue.Queue
	TaskDb        map[string][]*task.Task
	EventDb       map[string][]*task.TaskEvent
	Workers       []string
	WorkerTaskMap map[string][]uuid.UUID
	TaskWorkerMap map[uuid.UUID]string
}

func (m *Manager) SelectWorker() {
	fmt.Println("I will select an appropriate worker")
}

func (m *Manager) UpdateTasks() {
	fmt.Println("I will update tasks")
}

func (m *Manager) SendWork() {
	fmt.Println("I will send work to workers")
}
