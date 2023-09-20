package mapreduce

import "context"

func MapReduce[I, O, R any](ctx context.Context, size int, inputs []I, mapper func(I) (O, error), reducer func(R, O) (R, error), initalValue R) (R, error) {
	wp := NewWorkerPool[I, O](size, mapper)
	wp.start(ctx)
	go feed(ctx, inputs, wp)
	acc, err := reduce(ctx, wp, reducer, initalValue)
	return acc, err
}


func feed[I, O any](ctx context.Context, inputs []I, wp *workerpool[I, O]) {
	defer wp.stop(ctx)
	for _, input := range inputs {
		select {
		case <-ctx.Done():
			return
		case wp.inputChannel <- input:
			continue
		}
	}
}

func reduce[I, O, R any](ctx context.Context, wp *workerpool[I, O], reducer func(R, O) (R, error), acc R) (R, error) {
	errors := []error{}
	var erx error

	for {
		select {
		case res, more := <-wp.outputChannel:
			if !more {
				goto end
			}
			_, err := reducer(acc, res)
			if err != nil {
				errors = append(errors, err)
			}
		case err, more := <-wp.errChannel:
			if more {
				errors = append(errors, err)
			}
		case <-ctx.Done():
			return acc, ctx.Err()
		}
	}

end:
	if len(errors) > 0 {
		erx = MapReduceError{errors}
	}

	return acc, erx
}
