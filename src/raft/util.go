package raft

import "log"

// Debugging
const Debug = 2

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug <= 2 {
		log.Printf(format, a...)
	}
	return
}

func L1BPrint(format string, a ...interface{}) (n int, err error) {
	if Debug <= 2 {
		log.Printf(format, a...)
	}
	return
}

func L2BPrint(format string, a ...interface{}) (n int, err error) {
	if Debug <= 2 {
		log.Printf(format, a...)
	}
	return
}
