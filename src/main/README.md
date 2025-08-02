# MapReduce Implementation in Go (MIT 6.824)

This project is a simplified implementation of the MapReduce framework, based on MIT's [6.824: Distributed Systems](https://pdos.csail.mit.edu/6.824/) course. It includes a coordinator and workers that execute map and reduce tasks over a set of input files. The system supports crash recovery and is designed to handle parallel task execution using Go’s concurrency primitives.

## Overview

MapReduce is a programming model for processing large data sets with a parallel, distributed algorithm. This implementation includes:

- A **Coordinator** that assigns map and reduce tasks to workers.
- **Workers** that process tasks and report back to the coordinator.
- Support for **worker crashes** and **task re-assignment**.
- Tests for **correctness**, **crash tolerance**, and **early exit** scenarios.

## Features

- Basic map and reduce functionality.
- Worker registration and task assignment.
- Task timeout and re-assignment.
- Coordinator detects job completion (`Done()` method).
- Passes correctness, crash, and early-exit tests.
