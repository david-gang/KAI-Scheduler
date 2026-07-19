// Copyright 2025 NVIDIA CORPORATION
// SPDX-License-Identifier: Apache-2.0

package reclaim_test

import (
	"testing"

	. "go.uber.org/mock/gomock"
	"gopkg.in/h2non/gock.v1"

	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/actions/reclaim"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/pod_status"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/constants"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/test_utils"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/test_utils/jobs_fake"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/test_utils/nodes_fake"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/test_utils/tasks_fake"
)

// A GPU reclaimer (1 GPU + 3 CPU) targets the only fully-usable GPU node, which is
// full:
//
//	gpu-node (1 GPU, 3 CPU) = victim-gpu (1 GPU + 2 CPU) + victim-cpu (1 CPU)
//
// To free the GPU the reclaim must evict victim-gpu; that yields only 2 CPU, so to
// reach the reclaimer's 3 CPU it must also evict the CPU-only victim-cpu. victim-cpu
// requires no GPU and fits on the CPU-only rehome-node, so a correct reclaim should
// re-home (pipeline) it there rather than kill it.
//
// Bug: common.FeasibleNodesForJob is computed from the *reclaimer* (all its tasks
// require GPU), so it returns only nodes with idle/releasing GPUs. That same node
// set is reused inside TryToVirtuallyAllocatePreemptorAndGetVictims to re-home the
// evicted victims, so a CPU-only node is invisible and victim-cpu is needlessly
// reclaimed (Releasing) even though a CPU node had room.
//
// This test asserts the CORRECT behavior (victim-cpu pipelined to rehome-node) and
// currently FAILS on that bug (victim-cpu ends up Releasing on gpu-node). The
// control below is identical except rehome-node has a GPU (so it enters the feasible
// set), and there the CPU victim is correctly salvaged - isolating the defect to the
// GPU-only feasible-node filter being reused for victim re-homing.
//
// headroom-node adds cluster CPU capacity so the reclaimer queue's fair share is not
// degraded below its request; it has no GPU, so it never enters the feasible set.
func TestReclaimCpuVictimRehomingOnCpuNode(t *testing.T) {
	test_utils.InitTestingInfrastructure()
	controller := NewController(t)
	defer controller.Finish()
	defer gock.Off()

	topology := test_utils.TestTopologyBasic{
		Name: "cpu-only victim dragged into a gpu reclaim must re-home on a cpu-only node",
		Jobs: cpuVictimRehomingJobs(),
		Nodes: map[string]nodes_fake.TestNodeBasic{
			"gpu-node":      {GPUs: 1, CPUMillis: 3000},
			"headroom-node": {GPUs: 0, CPUMillis: 4000},
			// Only place victim-cpu can go; excluded from the reclaimer's feasible
			// set because it has no GPUs.
			"rehome-node": {GPUs: 0, CPUMillis: 1000},
			// A spare idle GPU keeps the reclaimer's initial feasible set non-empty
			// so reclaim can start; it has too little CPU (0.1) to host the
			// reclaimer or either victim, so it never affects placement. Without it,
			// gpu-node has 0 idle GPUs and the feasible set is empty, and reclaim
			// never fires at all - a second facet of the same FeasibleNodesForJob
			// issue that otherwise masks the re-homing bug under test here.
			"idle-gpu-node": {GPUs: 1, CPUMillis: 100},
		},
		Queues: cpuVictimRehomingQueues(),
		JobExpectedResults: map[string]test_utils.TestExpectedResultBasic{
			"gpu-reclaimer": {
				NodeName:             "gpu-node",
				GPUsRequired:         1,
				Status:               pod_status.Pipelined,
				DontValidateGPUGroup: true,
			},
			"victim-cpu": {
				// Correct behavior: relocated to the CPU node, not killed.
				NodeName: "rehome-node",
				Status:   pod_status.Pipelined,
			},
		},
		Mocks: cpuVictimRehomingMocks(),
	}

	ssn := test_utils.BuildSession(topology, controller)
	reclaim.New().Execute(ssn)
	test_utils.MatchExpectedAndRealTasks(t, 0, topology, ssn)
}

// Control for TestReclaimCpuVictimRehomingOnCpuNode: identical, except rehome-node
// has a single GPU, so it enters the reclaimer's feasible set. It still has only
// 1 CPU - too little for the 3-CPU reclaimer and for victim-gpu (2 CPU) - so only
// victim-cpu can land there and there is no contention for it. The reclaimer still
// must evict both victims on gpu-node. Because rehome-node is now in the feasible
// set, victim-cpu is correctly pipelined onto it. The only difference from the
// failing test is rehome-node's GPUs: 1 vs 0.
func TestReclaimCpuVictimRehomingOnGpuNode_Control(t *testing.T) {
	test_utils.InitTestingInfrastructure()
	controller := NewController(t)
	defer controller.Finish()
	defer gock.Off()

	topology := test_utils.TestTopologyBasic{
		Name: "control: cpu victim re-homes on a spare gpu node that is in the feasible set",
		Jobs: cpuVictimRehomingJobs(),
		Nodes: map[string]nodes_fake.TestNodeBasic{
			"gpu-node":      {GPUs: 1, CPUMillis: 3000},
			"headroom-node": {GPUs: 0, CPUMillis: 4000},
			// Same as rehome-node in the failing test but with a GPU: enters the
			// feasible set, so victim-cpu can be re-homed here.
			"rehome-node": {GPUs: 1, CPUMillis: 1000},
			// Keeps the reclaimer's initial feasible set non-empty so reclaim can
			// start (see the failing test for details); too little CPU to host
			// anything, so it never affects placement.
			"idle-gpu-node": {GPUs: 1, CPUMillis: 100},
		},
		Queues: cpuVictimRehomingQueues(),
		JobExpectedResults: map[string]test_utils.TestExpectedResultBasic{
			"gpu-reclaimer": {
				NodeName:             "gpu-node",
				GPUsRequired:         1,
				Status:               pod_status.Pipelined,
				DontValidateGPUGroup: true,
			},
			"victim-cpu": {
				NodeName: "rehome-node",
				Status:   pod_status.Pipelined,
			},
		},
		Mocks: cpuVictimRehomingMocks(),
	}

	ssn := test_utils.BuildSession(topology, controller)
	reclaim.New().Execute(ssn)
	test_utils.MatchExpectedAndRealTasks(t, 0, topology, ssn)
}

func cpuVictimRehomingJobs() []*jobs_fake.TestJobBasic {
	return []*jobs_fake.TestJobBasic{
		{
			Name:                "victim-gpu",
			Priority:            constants.PriorityTrainNumber,
			RequiredGPUsPerTask: 1,
			RequiredCPUsPerTask: 2000,
			QueueName:           "victim-queue",
			Tasks:               []*tasks_fake.TestTaskBasic{{NodeName: "gpu-node", State: pod_status.Running}},
		},
		{
			Name:                "victim-cpu",
			Priority:            constants.PriorityTrainNumber,
			RequiredCPUsPerTask: 1000,
			QueueName:           "victim-queue",
			Tasks:               []*tasks_fake.TestTaskBasic{{NodeName: "gpu-node", State: pod_status.Running}},
		},
		{
			Name:                "gpu-reclaimer",
			Priority:            constants.PriorityTrainNumber,
			RequiredGPUsPerTask: 1,
			RequiredCPUsPerTask: 3000,
			QueueName:           "reclaimer-queue",
			Tasks:               []*tasks_fake.TestTaskBasic{{State: pod_status.Pending}},
		},
	}
}

func cpuVictimRehomingQueues() []test_utils.TestQueueBasic {
	return []test_utils.TestQueueBasic{
		{
			Name:         "reclaimer-queue",
			DeservedGPUs: 1,
			DeservedCPUs: test_utils.CreateFloat64Pointer(3000),
		},
		{
			Name:         "victim-queue",
			DeservedGPUs: 0,
			DeservedCPUs: test_utils.CreateFloat64Pointer(0),
		},
	}
}

func cpuVictimRehomingMocks() *test_utils.TestMock {
	return &test_utils.TestMock{
		CacheRequirements: &test_utils.CacheMocking{
			NumberOfCacheBinds:      10,
			NumberOfCacheEvictions:  5,
			NumberOfPipelineActions: 5,
		},
	}
}
