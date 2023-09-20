package mapreduce

import (
	"context"
	"sync"
)

type workerpoolStatus string

const wpNew = workerpoolStatus("New")
const wpStarted = workerpoolStatus("Started")
const wpStopped = workerpoolStatus("Stopped")

type workerpool[I, O any] struct {
	inputChannel  chan I
	outputChannel chan O
	errChannel    chan error
	size          int
	do            func(I) (O, error)
	status        workerpoolStatus
	mu            *sync.RWMutex // guards the status of the worker pool
	doneWithInput *sync.WaitGroup
}

func NewWorkerPool[I, O any](size int, do func(I) (O, error)) *workerpool[I, O] {
	inputChannel := make(chan I)
	outputChannel := make(chan O)
	errChannel := make(chan error)

	var doneWithInput sync.WaitGroup
	doneWithInput.Add(size)

	var mu sync.RWMutex

	pool := workerpool[I, O]{
		inputChannel:  inputChannel,
		outputChannel: outputChannel,
		errChannel:    errChannel,
		do:            do,
		size:          size,
		status:        wpNew,
		doneWithInput: &doneWithInput,
		mu:            &mu,
	}

	return &pool
}

func (wp *workerpool[I, O]) getStatus() workerpoolStatus {
	wp.mu.RLock()
	status := wp.status
	wp.mu.RUnlock()
	return status
}

func (wp *workerpool[I, O]) start(ctx context.Context) {
	// to do
}

func (wp *workerpool[I, O]) stop(ctx context.Context) {
	// to do
}

func (wp *workerpool[I, O]) process(input I) {
	wp.inputChannel <- input
}
