package store

import (
	"cube/task"
	"fmt"
)

type InMemoryTaskStore struct {
	Db map[string]*task.Task
}

func NewInMemoryTaskStore() *InMemoryTaskStore {
	return &InMemoryTaskStore{
		Db: make(map[string]*task.Task),
	}
}

func (i *InMemoryTaskStore) Put(key string, value any) error {
	t, ok := value.(*task.Task)
	if !ok {
		return fmt.Errorf("Value %v is not a task.Task type", value)
	}

	i.Db[key] = t

	return nil
}

func (i *InMemoryTaskStore) Get(key string) (any, error) {
	v, ok := i.Db[key]
	if !ok {
		return nil, fmt.Errorf("Task with key %s does not exist", key)
	}

	return v, nil
}

func (i *InMemoryTaskStore) List() (any, error) {
	var tasks []*task.Task
	for _, t := range i.Db {
		tasks = append(tasks, t)
	}

	return tasks, nil
}

func (i *InMemoryTaskStore) Count() (int, error) {
	return len(i.Db), nil
}

type InMemoryTaskEventStore struct {
	Db map[string]*task.TaskEvent
}

func NewInMemoryTaskEventStore() *InMemoryTaskEventStore {
	return &InMemoryTaskEventStore{
		Db: make(map[string]*task.TaskEvent),
	}
}

func (i *InMemoryTaskEventStore) Put(key string, value any) error {
	t, ok := value.(*task.TaskEvent)
	if !ok {
		return fmt.Errorf("Value %v is not a task.TaskEvent type", value)
	}

	i.Db[key] = t

	return nil
}

func (i *InMemoryTaskEventStore) Get(key string) (any, error) {
	v, ok := i.Db[key]
	if !ok {
		return nil, fmt.Errorf("Task with key %s does not exist", key)
	}

	return v, nil
}

func (i *InMemoryTaskEventStore) List() (any, error) {
	var tasks []*task.TaskEvent
	for _, t := range i.Db {
		tasks = append(tasks, t)
	}

	return tasks, nil
}

func (i *InMemoryTaskEventStore) Count() (int, error) {
	return len(i.Db), nil
}
