package task

import (
	"context"
	"io"
	"log"
	"math"
	"os"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/client"
	"github.com/docker/docker/pkg/stdcopy"
)

type Docker struct {
	Client      *client.Client
	Config      Config
	ContainerId string
}

type DockerResult struct {
	Error       error
	Action      string
	ContainerId string
	Result      string
}

type DockerInspectResponse struct {
	Error     error
	Container *types.ContainerJSON
}

func NewDocker(conf *Config) *Docker {
	dc, _ := client.NewClientWithOpts(client.FromEnv)
	return &Docker{
		Client: dc,
		Config: *conf,
	}
}

func (d *Docker) Run() DockerResult {
	ctx := context.Background()

	// Pull image
	reader, err := d.Client.ImagePull(ctx, d.Config.Image, types.ImagePullOptions{})
	if err != nil {
		log.Printf("Error pulling image %s: %v\n", d.Config.Image, err)
		return DockerResult{Error: err}
	}
	io.Copy(os.Stdout, reader)

	rp := container.RestartPolicy{
		Name: d.Config.RestartPolicy,
	}

	r := container.Resources{
		Memory:   d.Config.Memory,
		NanoCPUs: int64(d.Config.Cpu * math.Pow10(9)),
	}

	cc := container.Config{
		Image:        d.Config.Image,
		Tty:          false,
		Env:          d.Config.Env,
		ExposedPorts: d.Config.ExposedPorts,
	}

	hc := container.HostConfig{
		RestartPolicy:   rp,
		Resources:       r,
		PublishAllPorts: true,
	}

	// Create container
	resp, err := d.Client.ContainerCreate(ctx, &cc, &hc, nil, nil, d.Config.Name)
	if err != nil {
		log.Printf("Error creating container using image %s: %v\n", d.Config.Image, err)
		return DockerResult{Error: err}
	}

	// Start container
	if err := d.Client.ContainerStart(ctx, resp.ID, types.ContainerStartOptions{}); err != nil {
		log.Printf("Error starting container %s: %v\n", d.Config.Image, err)
		return DockerResult{Error: err}
	}

	d.ContainerId = resp.ID

	out, err := d.Client.ContainerLogs(ctx, resp.ID, types.ContainerLogsOptions{ShowStdout: true, ShowStderr: true})
	if err != nil {
		log.Printf("Error getting logs for container %s: %v\n", resp.ID, err)
		return DockerResult{Error: err}
	}

	stdcopy.StdCopy(os.Stdout, os.Stderr, out)

	return DockerResult{ContainerId: resp.ID, Action: "start", Result: "success"}
}

func (d *Docker) Stop(id string) DockerResult {
	log.Printf("Attempting to stop container: %v\n", id)
	ctx := context.Background()
	if err := d.Client.ContainerStop(ctx, id, container.StopOptions{}); err != nil {
		log.Printf("Error stopping container %s: %v\n", id, err)
		return DockerResult{Error: err}
	}

	if err := d.Client.ContainerRemove(ctx, id, types.ContainerRemoveOptions{RemoveVolumes: true, RemoveLinks: false, Force: false}); err != nil {
		log.Printf("Error removing container %s: %v\n", id, err)
		return DockerResult{Error: err}
	}

	return DockerResult{Action: "stop", Result: "success", Error: nil}
}

func (d *Docker) Inspect(containerID string) DockerInspectResponse {
	ctx := context.Background()

	resp, err := d.Client.ContainerInspect(ctx, containerID)
	if err != nil {
		log.Printf("Error inspecting container: %v\n", err)
		return DockerInspectResponse{Error: err}
	}

	return DockerInspectResponse{Container: &resp}
}
