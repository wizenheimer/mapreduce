package mapreduce

import "fmt"

type MapReduceError struct {
	Errors []error
}

func (err MapReduceError) Error() string {
	return fmt.Sprintf("Error instances : %v", len(err.Errors))
}

type MapError struct {
	Err error
}

func (e MapError) Error() string {
	return "Map error: " + e.Err.Error()
}
func (e MapError) Unwrap() error {
	return e.Err
}
