package raft

import (
	"fmt"
	"log"
)

// Debugging
const Debug = false

const Debug2B = false

const Debug_test = false

const Debug_lock = false

func LPrint(rf *Raft, format string, a ...interface{}) {
	if Debug_lock {
		format = "[peer %v (%v) at Term %v] " + format + "\n"
		a = append([]interface{}{rf.me, rf.role, rf.currentTerm}, a...)
		fmt.Printf(format, a...)
	}
}

func TPrintf(format string, a ...interface{}) {
	if Debug_test {
		log.Printf(format, a...)
	}
	return
}
func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func MyPrint(rf *Raft, format string, a ...interface{}) {
	if Debug {
		format = "[peer %v (%v) at Term %v] " + format + "\n"
		a = append([]interface{}{rf.me, rf.role, rf.currentTerm}, a...)
		fmt.Printf(format, a...)
	}
}

func MyPrint2B(rf *Raft, format string, a ...interface{}) {
	if Debug2B {
		format = "[peer %v (%v) at Term %v] " + format + "\n"
		a = append([]interface{}{rf.me, rf.role, rf.currentTerm}, a...)
		fmt.Printf(format, a...)
	}
}

func Min(x, y int) int {
	if x < y {
		return x
	}
	return y
}

func Max(x, y int) int {
	if x > y {
		return x
	}
	return y
}
