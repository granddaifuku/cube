package scheduler

// 1. Determine a set of candidate workers on which a task could run
// 2. Score the candidate workers
// 3. Pick the best worker

type Scheduler interface {
	SelectCandidateNodes()
	Score()
	Pick()
}
