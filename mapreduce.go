package mapreduce

import "context"

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
