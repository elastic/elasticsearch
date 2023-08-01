/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.autoscaling;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.LatchedActionListener;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.monitor.os.OsStats;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.xpack.core.ml.autoscaling.MlAutoscalingStats;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.process.MlMemoryTracker;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static org.elasticsearch.xpack.ml.autoscaling.MlAutoscalingResourceTracker.MlJobRequirements;
import static org.mockito.Mockito.mock;

public class MlAutoscalingResourceTrackerTests extends ESTestCase {

    public void testGetMlNodeStatsForNoMlNode() throws InterruptedException {
        AtomicBoolean clientGotCalled = new AtomicBoolean();

        try (Client client = new NoOpClient(getTestName()) {

            @SuppressWarnings("unchecked")
            @Override
            protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                clientGotCalled.set(true);
                listener.onResponse(
                    (Response) new NodesStatsResponse(new ClusterName("_name"), Collections.emptyList(), Collections.emptyList())
                );
            }
        }) {
            this.<Map<String, OsStats>>assertAsync(
                listener -> MlAutoscalingResourceTracker.getMlNodeStats(Strings.EMPTY_ARRAY, client, TimeValue.MAX_VALUE, listener),
                response -> {
                    assertFalse(clientGotCalled.get());
                }
            );

            this.<Map<String, OsStats>>assertAsync(
                listener -> MlAutoscalingResourceTracker.getMlNodeStats(new String[] { "ml-1" }, client, TimeValue.MAX_VALUE, listener),
                response -> {
                    assertTrue(clientGotCalled.get());
                }
            );
        }
    }

    public void testGetMemoryAndProcessors() throws InterruptedException {
        MlAutoscalingContext mlAutoscalingContext = new MlAutoscalingContext();
        MlMemoryTracker mockTracker = mock(MlMemoryTracker.class);

        long memory = randomLongBetween(100, 1_000_000);
        this.<MlAutoscalingStats>assertAsync(
            listener -> MlAutoscalingResourceTracker.getMemoryAndProcessors(
                mlAutoscalingContext,
                mockTracker,
                Map.of(
                    "ml-1",
                    new OsStats(
                        randomNonNegativeLong(),
                        new OsStats.Cpu(randomShort(), null),
                        new OsStats.Mem(memory, memory, randomLongBetween(0, memory)),
                        new OsStats.Swap(randomNonNegativeLong(), randomNonNegativeLong()),
                        null
                    ),
                    "ml-2",
                    new OsStats(
                        randomNonNegativeLong(),
                        new OsStats.Cpu(randomShort(), null),
                        new OsStats.Mem(memory, memory, randomLongBetween(0, memory)),
                        new OsStats.Swap(randomNonNegativeLong(), randomNonNegativeLong()),
                        null
                    )
                ),
                memory / 2,
                10,
                MachineLearning.DEFAULT_MAX_OPEN_JOBS_PER_NODE,
                listener
            ),
            stats -> {
                assertEquals(memory, stats.perNodeMemoryInBytes());
                assertEquals(2, stats.nodes());
                assertEquals(0, stats.minNodes());
                assertEquals(0, stats.extraSingleNodeProcessors());
                assertEquals(MachineLearning.NATIVE_EXECUTABLE_CODE_OVERHEAD.getBytes(), stats.perNodeMemoryOverheadInBytes());
            }
        );

        // simulate 1 small, 1 bigger node
        this.<MlAutoscalingStats>assertAsync(
            listener -> MlAutoscalingResourceTracker.getMemoryAndProcessors(
                mlAutoscalingContext,
                mockTracker,
                Map.of(
                    "ml-1",
                    new OsStats(
                        randomNonNegativeLong(),
                        new OsStats.Cpu(randomShort(), null),
                        new OsStats.Mem(memory, memory, randomLongBetween(0, memory)),
                        new OsStats.Swap(randomNonNegativeLong(), randomNonNegativeLong()),
                        null
                    ),
                    "ml-2",
                    new OsStats(
                        randomNonNegativeLong(),
                        new OsStats.Cpu(randomShort(), null),
                        new OsStats.Mem(2 * memory, 2 * memory, randomLongBetween(0, 2 * memory)),
                        new OsStats.Swap(randomNonNegativeLong(), randomNonNegativeLong()),
                        null
                    )
                ),
                memory / 2,
                10,
                MachineLearning.DEFAULT_MAX_OPEN_JOBS_PER_NODE,
                listener
            ),
            stats -> {
                assertEquals(0, stats.perNodeMemoryInBytes());
                assertEquals(2, stats.nodes());
                assertEquals(0, stats.minNodes());
                assertEquals(0, stats.extraSingleNodeProcessors());
                assertEquals(MachineLearning.NATIVE_EXECUTABLE_CODE_OVERHEAD.getBytes(), stats.perNodeMemoryOverheadInBytes());
            }
        );
    }

    public void testCheckIfJobsCanBeMovedInLeastEfficientWayMemoryOnly() {
        assertEquals(
            0L,
            MlAutoscalingResourceTracker.checkIfJobsCanBeMovedInLeastEfficientWay(
                List.of(MlJobRequirements.of(10L, 0)),
                Map.of("node_a", MlJobRequirements.of(100L, 0)),
                1000L,
                10,
                MachineLearning.DEFAULT_MAX_OPEN_JOBS_PER_NODE
            )
        );
        assertEquals(
            10L,
            MlAutoscalingResourceTracker.checkIfJobsCanBeMovedInLeastEfficientWay(
                List.of(MlJobRequirements.of(10L, 0)),
                Map.of("node_a", MlJobRequirements.of(995L, 0)),
                1000L,
                10,
                MachineLearning.DEFAULT_MAX_OPEN_JOBS_PER_NODE
            )
        );

        // equal sizes fit on all nodes
        assertEquals(
            0L,
            MlAutoscalingResourceTracker.checkIfJobsCanBeMovedInLeastEfficientWay(
                List.of(
                    MlJobRequirements.of(10L, 0),
                    MlJobRequirements.of(10L, 0),
                    MlJobRequirements.of(10L, 0),
                    MlJobRequirements.of(10L, 0),
                    MlJobRequirements.of(10L, 0)
                ),
                Map.of(
                    "node_a",
                    MlJobRequirements.of(976L, 0),
                    "node_b",
                    MlJobRequirements.of(986L, 0),
                    "node_c",
                    MlJobRequirements.of(967L, 0)
                ),
                1000L,
                10,
                MachineLearning.DEFAULT_MAX_OPEN_JOBS_PER_NODE
            )
        );

        // run into max open job limit
        assertEquals(
            10L,
            MlAutoscalingResourceTracker.checkIfJobsCanBeMovedInLeastEfficientWay(
                List.of(
                    MlJobRequirements.of(10L, 0),
                    MlJobRequirements.of(10L, 0),
                    MlJobRequirements.of(10L, 0),
                    MlJobRequirements.of(10L, 0),
                    MlJobRequirements.of(10L, 0)
                ),
                Map.of(
                    "node_a",
                    MlJobRequirements.of(976L, 0, 3),
                    "node_b",
                    MlJobRequirements.of(986L, 0, 3),
                    "node_c",
                    MlJobRequirements.of(967L, 0, 2)
                ),
                1000L,
                10,
                4
            )
        );

        assertEquals(
            0L,
            MlAutoscalingResourceTracker.checkIfJobsCanBeMovedInLeastEfficientWay(
                List.of(
                    MlJobRequirements.of(10L, 0),
                    MlJobRequirements.of(10L, 0),
                    MlJobRequirements.of(10L, 0),
                    MlJobRequirements.of(10L, 0),
                    MlJobRequirements.of(10L, 0)
                ),
                Map.of(
                    "node_a",
                    MlJobRequirements.of(980L, 0),
                    "node_b",
                    MlJobRequirements.of(990L, 0),
                    "node_c",
                    MlJobRequirements.of(970L, 0)
                ),
                1000L,
                10,
                MachineLearning.DEFAULT_MAX_OPEN_JOBS_PER_NODE
            )
        );

        // doesn't fit
        assertEquals(
            10L,
            MlAutoscalingResourceTracker.checkIfJobsCanBeMovedInLeastEfficientWay(
                List.of(
                    MlJobRequirements.of(10L, 0),
                    MlJobRequirements.of(10L, 0),
                    MlJobRequirements.of(10L, 0),
                    MlJobRequirements.of(10L, 0),
                    MlJobRequirements.of(10L, 0),
                    MlJobRequirements.of(10L, 0),
                    MlJobRequirements.of(10L, 0)
                ),
                Map.of(
                    "node_a",
                    MlJobRequirements.of(976L, 0),
                    "node_b",
                    MlJobRequirements.of(986L, 0),
                    "node_c",
                    MlJobRequirements.of(967L, 0)
                ),
                1000L,
                10,
                MachineLearning.DEFAULT_MAX_OPEN_JOBS_PER_NODE
            )
        );
        assertEquals(
            40L,
            MlAutoscalingResourceTracker.checkIfJobsCanBeMovedInLeastEfficientWay(
                List.of(
                    MlJobRequirements.of(10L, 0),
                    MlJobRequirements.of(10L, 0),
                    MlJobRequirements.of(10L, 0),
                    MlJobRequirements.of(10L, 0),
                    MlJobRequirements.of(10L, 0),
                    MlJobRequirements.of(10L, 0),
                    MlJobRequirements.of(40L, 0)
                ),
                Map.of(
                    "node_a",
                    MlJobRequirements.of(976L, 0),
                    "node_b",
                    MlJobRequirements.of(946L, 0),
                    "node_c",
                    MlJobRequirements.of(967L, 0)
                ),
                1000L,
                10,
                MachineLearning.DEFAULT_MAX_OPEN_JOBS_PER_NODE
            )
        );

        assertEquals(
            130L,
            MlAutoscalingResourceTracker.checkIfJobsCanBeMovedInLeastEfficientWay(
                List.of(
                    MlJobRequirements.of(10L, 0),
                    MlJobRequirements.of(20L, 0),
                    MlJobRequirements.of(30L, 0),
                    MlJobRequirements.of(40L, 0),
                    MlJobRequirements.of(50L, 0),
                    MlJobRequirements.of(60L, 0),
                    MlJobRequirements.of(70L, 0)
                ), // 280, with better packing this could return 20 + 50
                Map.of(
                    "node_a",
                    MlJobRequirements.of(886L, 0),
                    "node_b",
                    MlJobRequirements.of(926L, 0),
                    "node_c",
                    MlJobRequirements.of(967L, 0)
                ),
                1000L,
                10,
                MachineLearning.DEFAULT_MAX_OPEN_JOBS_PER_NODE
            )
        );

        assertEquals(
            70L,
            MlAutoscalingResourceTracker.checkIfJobsCanBeMovedInLeastEfficientWay(
                List.of(
                    MlJobRequirements.of(10L, 0),
                    MlJobRequirements.of(20L, 0),
                    MlJobRequirements.of(30L, 0),
                    MlJobRequirements.of(40L, 0),
                    MlJobRequirements.of(50L, 0),
                    MlJobRequirements.of(60L, 0),
                    MlJobRequirements.of(70L, 0)
                ), // 280, solvable with optimal packing
                Map.of(
                    "node_a",
                    MlJobRequirements.of(886L, 0),
                    "node_b",
                    MlJobRequirements.of(906L, 0),
                    "node_c",
                    MlJobRequirements.of(917L, 0)
                ),
                1000L,
                10,
                MachineLearning.DEFAULT_MAX_OPEN_JOBS_PER_NODE
            )
        );

        assertEquals(
            70L,
            MlAutoscalingResourceTracker.checkIfJobsCanBeMovedInLeastEfficientWay(
                List.of(
                    MlJobRequirements.of(10L, 0),
                    MlJobRequirements.of(20L, 0),
                    MlJobRequirements.of(30L, 0),
                    MlJobRequirements.of(40L, 0),
                    MlJobRequirements.of(50L, 0),
                    MlJobRequirements.of(60L, 0),
                    MlJobRequirements.of(70L, 0)
                ), // 280, solvable with optimal packing
                Map.of(
                    "node_a",
                    MlJobRequirements.of(866L, 0),
                    "node_b",
                    MlJobRequirements.of(886L, 0),
                    "node_c",
                    MlJobRequirements.of(917L, 0)
                ),
                1000L,
                10,
                MachineLearning.DEFAULT_MAX_OPEN_JOBS_PER_NODE
            )
        );

        assertEquals(
            500L,
            MlAutoscalingResourceTracker.checkIfJobsCanBeMovedInLeastEfficientWay(
                List.of(MlJobRequirements.of(500L, 0), MlJobRequirements.of(200L, 0)),
                Map.of("node_a", MlJobRequirements.of(1400L, 0), "node_b", MlJobRequirements.of(1700L, 0)),
                2000L,
                10,
                MachineLearning.DEFAULT_MAX_OPEN_JOBS_PER_NODE
            )
        );

        assertEquals(
            700L,
            MlAutoscalingResourceTracker.checkIfJobsCanBeMovedInLeastEfficientWay(
                List.of(MlJobRequirements.of(500L, 0), MlJobRequirements.of(200L, 0)),
                Collections.emptyMap(),
                2000L,
                10,
                MachineLearning.DEFAULT_MAX_OPEN_JOBS_PER_NODE
            )
        );

        assertEquals(
            0L,
            MlAutoscalingResourceTracker.checkIfJobsCanBeMovedInLeastEfficientWay(
                Collections.emptyList(),
                Collections.emptyMap(),
                2000L,
                10,
                MachineLearning.DEFAULT_MAX_OPEN_JOBS_PER_NODE
            )
        );

        assertEquals(
            0L,
            MlAutoscalingResourceTracker.checkIfJobsCanBeMovedInLeastEfficientWay(
                Collections.emptyList(),
                Map.of("node_a", MlJobRequirements.of(1400L, 0), "node_b", MlJobRequirements.of(1700L, 0)),
                2000L,
                10,
                MachineLearning.DEFAULT_MAX_OPEN_JOBS_PER_NODE
            )
        );
    }

    public void testCheckIfJobsCanBeMovedInLeastEfficientWayProcessorsAndMemory() {
        assertEquals(
            0L,
            MlAutoscalingResourceTracker.checkIfJobsCanBeMovedInLeastEfficientWay(
                List.of(MlJobRequirements.of(10L, 2)),
                Map.of("node_a", MlJobRequirements.of(100L, 2)),
                1000L,
                4,
                MachineLearning.DEFAULT_MAX_OPEN_JOBS_PER_NODE
            )
        );

        // fits memory-wise, but not processors
        assertEquals(
            10L,
            MlAutoscalingResourceTracker.checkIfJobsCanBeMovedInLeastEfficientWay(
                List.of(MlJobRequirements.of(10L, 2)),
                Map.of("node_a", MlJobRequirements.of(100L, 2)),
                1000L,
                3,
                MachineLearning.DEFAULT_MAX_OPEN_JOBS_PER_NODE
            )
        );
        // fits processors, but not memory
        assertEquals(
            10L,
            MlAutoscalingResourceTracker.checkIfJobsCanBeMovedInLeastEfficientWay(
                List.of(MlJobRequirements.of(10L, 1)),
                Map.of("node_a", MlJobRequirements.of(995L, 2)),
                1000L,
                4,
                MachineLearning.DEFAULT_MAX_OPEN_JOBS_PER_NODE
            )
        );

        // fit, but requires some shuffling
        assertEquals(
            0L,
            MlAutoscalingResourceTracker.checkIfJobsCanBeMovedInLeastEfficientWay(
                List.of(
                    MlJobRequirements.of(10L, 1),
                    MlJobRequirements.of(10L, 1),
                    MlJobRequirements.of(10L, 1),
                    MlJobRequirements.of(10L, 1),
                    MlJobRequirements.of(10L, 1)
                ),
                Map.of(
                    "node_a",
                    MlJobRequirements.of(980L, 3),
                    "node_b",
                    MlJobRequirements.of(980L, 1),
                    "node_c",
                    MlJobRequirements.of(970L, 0)
                ),
                1000L,
                4,
                MachineLearning.DEFAULT_MAX_OPEN_JOBS_PER_NODE
            )
        );

        // special processor placement
        assertEquals(
            0L,
            MlAutoscalingResourceTracker.checkIfJobsCanBeMovedInLeastEfficientWay(
                List.of(
                    MlJobRequirements.of(10L, 4),
                    MlJobRequirements.of(10L, 3),
                    MlJobRequirements.of(10L, 2),
                    MlJobRequirements.of(10L, 1),
                    MlJobRequirements.of(10L, 0)
                ),
                Map.of(
                    "node_a",
                    MlJobRequirements.of(900L, 3),
                    "node_b",
                    MlJobRequirements.of(920L, 1),
                    "node_c",
                    MlJobRequirements.of(940L, 0),
                    "node_d",
                    MlJobRequirements.of(960L, 0)
                ),
                1000L,
                4,
                MachineLearning.DEFAULT_MAX_OPEN_JOBS_PER_NODE
            )
        );

        // special processor placement, but doesn't fit due to open job limit
        assertEquals(
            30L,
            MlAutoscalingResourceTracker.checkIfJobsCanBeMovedInLeastEfficientWay(
                List.of(
                    MlJobRequirements.of(10L, 4),
                    MlJobRequirements.of(10L, 3),
                    MlJobRequirements.of(10L, 2),
                    MlJobRequirements.of(10L, 1),
                    MlJobRequirements.of(10L, 0)
                ),
                Map.of(
                    "node_a",
                    MlJobRequirements.of(900L, 3),
                    "node_b",
                    MlJobRequirements.of(920L, 1),
                    "node_c",
                    MlJobRequirements.of(940L, 0, 5),
                    "node_d",
                    MlJobRequirements.of(960L, 0, 4)
                ),
                1000L,
                4,
                5
            )
        );

        // plenty of space, but no processor
        assertEquals(
            40L,
            MlAutoscalingResourceTracker.checkIfJobsCanBeMovedInLeastEfficientWay(
                List.of(
                    MlJobRequirements.of(10L, 1),
                    MlJobRequirements.of(10L, 1),
                    MlJobRequirements.of(10L, 1),
                    MlJobRequirements.of(10L, 1),
                    MlJobRequirements.of(10L, 1)
                ),
                Map.of(
                    "node_a",
                    MlJobRequirements.of(980L, 3),
                    "node_b",
                    MlJobRequirements.of(980L, 4),
                    "node_c",
                    MlJobRequirements.of(970L, 4)
                ),
                1000L,
                4,
                MachineLearning.DEFAULT_MAX_OPEN_JOBS_PER_NODE
            )
        );

        // processor available, but not in combination with memory
        assertEquals(
            30L,
            MlAutoscalingResourceTracker.checkIfJobsCanBeMovedInLeastEfficientWay(
                List.of(
                    MlJobRequirements.of(10L, 1),
                    MlJobRequirements.of(10L, 1),
                    MlJobRequirements.of(10L, 1),
                    MlJobRequirements.of(10L, 1),
                    MlJobRequirements.of(10L, 1)
                ),
                Map.of(
                    "node_a",
                    MlJobRequirements.of(980L, 1),
                    "node_b",
                    MlJobRequirements.of(980L, 4),
                    "node_c",
                    MlJobRequirements.of(970L, 4)
                ),
                1000L,
                4,
                MachineLearning.DEFAULT_MAX_OPEN_JOBS_PER_NODE
            )
        );
    }

    public void testCheckIfOneNodeCouldBeRemovedMemoryOnly() {
        assertEquals(
            true,
            MlAutoscalingResourceTracker.checkIfOneNodeCouldBeRemoved(
                Map.of(
                    "node_a",
                    List.of(MlJobRequirements.of(100L, 0), MlJobRequirements.of(200L, 0), MlJobRequirements.of(300L, 0)),
                    "node_b",
                    List.of(MlJobRequirements.of(200L, 0), MlJobRequirements.of(300L, 0)),
                    "node_c",
                    List.of(
                        MlJobRequirements.of(10L, 0),
                        MlJobRequirements.of(10L, 0),
                        MlJobRequirements.of(10L, 0),
                        MlJobRequirements.of(10L, 0),
                        MlJobRequirements.of(10L, 0)
                    )
                ),
                600L,
                10,
                MachineLearning.DEFAULT_MAX_OPEN_JOBS_PER_NODE
            )
        );

        assertEquals(
            false,
            MlAutoscalingResourceTracker.checkIfOneNodeCouldBeRemoved(
                Map.of(
                    "node_a",
                    List.of(MlJobRequirements.of(100L, 0), MlJobRequirements.of(200L, 0), MlJobRequirements.of(300L, 0)),
                    "node_b",
                    List.of(MlJobRequirements.of(280L, 0), MlJobRequirements.of(300L, 0)),
                    "node_c",
                    List.of(
                        MlJobRequirements.of(10L, 0),
                        MlJobRequirements.of(10L, 0),
                        MlJobRequirements.of(10L, 0),
                        MlJobRequirements.of(10L, 0),
                        MlJobRequirements.of(10L, 0)
                    )
                ),
                600L,
                10,
                MachineLearning.DEFAULT_MAX_OPEN_JOBS_PER_NODE
            )
        );

        assertEquals(
            false,
            MlAutoscalingResourceTracker.checkIfOneNodeCouldBeRemoved(
                Map.of(
                    "node_a",
                    List.of(
                        MlJobRequirements.of(10L, 0),
                        MlJobRequirements.of(10L, 0),
                        MlJobRequirements.of(10L, 0),
                        MlJobRequirements.of(10L, 0),
                        MlJobRequirements.of(10L, 0)
                    )
                ),
                600L,
                10,
                MachineLearning.DEFAULT_MAX_OPEN_JOBS_PER_NODE
            )
        );

        assertEquals(
            false,
            MlAutoscalingResourceTracker.checkIfOneNodeCouldBeRemoved(
                Collections.emptyMap(),
                999L,
                10,
                MachineLearning.DEFAULT_MAX_OPEN_JOBS_PER_NODE
            )
        );

        // solvable case with optimal packing, but not possible if badly packed
        assertEquals(
            false,
            MlAutoscalingResourceTracker.checkIfOneNodeCouldBeRemoved(
                Map.of(
                    "node_a",
                    List.of(MlJobRequirements.of(100L, 0), MlJobRequirements.of(200L, 0), MlJobRequirements.of(300L, 0)),
                    "node_b",
                    List.of(MlJobRequirements.of(280L, 0), MlJobRequirements.of(300L, 0)),
                    "node_c",
                    List.of(
                        MlJobRequirements.of(500L, 0),
                        MlJobRequirements.of(10L, 0),
                        MlJobRequirements.of(10L, 0),
                        MlJobRequirements.of(10L, 0),
                        MlJobRequirements.of(10L, 0),
                        MlJobRequirements.of(10L, 0)
                    )
                ),
                1000L,
                10,
                MachineLearning.DEFAULT_MAX_OPEN_JOBS_PER_NODE
            )
        );

        // same with smaller jobs, that can be re-arranged
        assertEquals(
            true,
            MlAutoscalingResourceTracker.checkIfOneNodeCouldBeRemoved(
                Map.of(
                    "node_a",
                    List.of(MlJobRequirements.of(100L, 0), MlJobRequirements.of(200L, 0), MlJobRequirements.of(300L, 0)),
                    "node_b",
                    List.of(MlJobRequirements.of(280L, 0), MlJobRequirements.of(300L, 0)),
                    "node_c",
                    List.of(
                        MlJobRequirements.of(100L, 0),
                        MlJobRequirements.of(100L, 0),
                        MlJobRequirements.of(100L, 0),
                        MlJobRequirements.of(100L, 0),
                        MlJobRequirements.of(100L, 0),
                        MlJobRequirements.of(10L, 0),
                        MlJobRequirements.of(10L, 0),
                        MlJobRequirements.of(10L, 0),
                        MlJobRequirements.of(10L, 0),
                        MlJobRequirements.of(10L, 0)
                    )
                ),
                1000L,
                10,
                MachineLearning.DEFAULT_MAX_OPEN_JOBS_PER_NODE
            )
        );

        assertEquals(
            true,
            MlAutoscalingResourceTracker.checkIfOneNodeCouldBeRemoved(
                Map.of(
                    "node_a",
                    List.of(MlJobRequirements.of(100L, 0), MlJobRequirements.of(200L, 0), MlJobRequirements.of(300L, 0)),
                    "node_b",
                    List.of(MlJobRequirements.of(280L, 0), MlJobRequirements.of(300L, 0)),
                    "node_c",
                    List.of(
                        MlJobRequirements.of(100L, 0),
                        MlJobRequirements.of(100L, 0),
                        MlJobRequirements.of(100L, 0),
                        MlJobRequirements.of(100L, 0),
                        MlJobRequirements.of(100L, 0),
                        MlJobRequirements.of(50L, 0),
                        MlJobRequirements.of(10L, 0),
                        MlJobRequirements.of(10L, 0),
                        MlJobRequirements.of(10L, 0),
                        MlJobRequirements.of(10L, 0)
                    )
                ),
                1000L,
                10,
                MachineLearning.DEFAULT_MAX_OPEN_JOBS_PER_NODE
            )
        );

        assertEquals(
            true,
            MlAutoscalingResourceTracker.checkIfOneNodeCouldBeRemoved(
                Map.of(
                    "node_a",
                    List.of(MlJobRequirements.of(100L, 0), MlJobRequirements.of(200L, 0), MlJobRequirements.of(300L, 0)),
                    "node_b",
                    List.of(MlJobRequirements.of(280L, 0), MlJobRequirements.of(325L, 0)),
                    "node_c",
                    List.of(
                        MlJobRequirements.of(100L, 0),
                        MlJobRequirements.of(100L, 0),
                        MlJobRequirements.of(100L, 0),
                        MlJobRequirements.of(100L, 0),
                        MlJobRequirements.of(100L, 0),
                        MlJobRequirements.of(50L, 0),
                        MlJobRequirements.of(10L, 0),
                        MlJobRequirements.of(10L, 0),
                        MlJobRequirements.of(10L, 0),
                        MlJobRequirements.of(10L, 0)
                    )
                ),
                1000L,
                10,
                MachineLearning.DEFAULT_MAX_OPEN_JOBS_PER_NODE
            )
        );
    }

    public void testCheckIfOneNodeCouldBeRemovedProcessorAndMemory() {
        // plenty of processors and memory
        assertEquals(
            true,
            MlAutoscalingResourceTracker.checkIfOneNodeCouldBeRemoved(
                Map.of(
                    "node_a",
                    List.of(MlJobRequirements.of(100L, 1), MlJobRequirements.of(200L, 1), MlJobRequirements.of(300L, 1)),
                    "node_b",
                    List.of(MlJobRequirements.of(200L, 1), MlJobRequirements.of(300L, 1)),
                    "node_c",
                    List.of(
                        MlJobRequirements.of(10L, 1),
                        MlJobRequirements.of(10L, 1),
                        MlJobRequirements.of(10L, 1),
                        MlJobRequirements.of(10L, 1),
                        MlJobRequirements.of(10L, 1)
                    )
                ),
                600L,
                10,
                MachineLearning.DEFAULT_MAX_OPEN_JOBS_PER_NODE
            )
        );

        // processors limit
        assertEquals(
            false,
            MlAutoscalingResourceTracker.checkIfOneNodeCouldBeRemoved(
                Map.of(
                    "node_a",
                    List.of(MlJobRequirements.of(100L, 1), MlJobRequirements.of(200L, 1), MlJobRequirements.of(300L, 1)),
                    "node_b",
                    List.of(MlJobRequirements.of(200L, 1), MlJobRequirements.of(300L, 1)),
                    "node_c",
                    List.of(
                        MlJobRequirements.of(10L, 1),
                        MlJobRequirements.of(10L, 1),
                        MlJobRequirements.of(10L, 1),
                        MlJobRequirements.of(10L, 1),
                        MlJobRequirements.of(10L, 1)
                    )
                ),
                600L,
                2,
                MachineLearning.DEFAULT_MAX_OPEN_JOBS_PER_NODE
            )
        );

        // job limit
        assertEquals(
            false,
            MlAutoscalingResourceTracker.checkIfOneNodeCouldBeRemoved(
                Map.of(
                    "node_a",
                    List.of(MlJobRequirements.of(100L, 1), MlJobRequirements.of(200L, 1), MlJobRequirements.of(300L, 1)),
                    "node_b",
                    List.of(MlJobRequirements.of(200L, 1), MlJobRequirements.of(300L, 1), MlJobRequirements.of(10L, 1)),
                    "node_c",
                    List.of(
                        MlJobRequirements.of(10L, 1),
                        MlJobRequirements.of(10L, 1),
                        MlJobRequirements.of(10L, 1),
                        MlJobRequirements.of(10L, 1),
                        MlJobRequirements.of(10L, 1)
                    )
                ),
                600L,
                10,
                5
            )
        );

        // 1 node with some jobs that require processors
        assertEquals(
            false,
            MlAutoscalingResourceTracker.checkIfOneNodeCouldBeRemoved(
                Map.of(
                    "node_a",
                    List.of(
                        MlJobRequirements.of(10L, 1),
                        MlJobRequirements.of(10L, 1),
                        MlJobRequirements.of(10L, 1),
                        MlJobRequirements.of(10L, 1),
                        MlJobRequirements.of(10L, 3)
                    )
                ),
                600L,
                10,
                MachineLearning.DEFAULT_MAX_OPEN_JOBS_PER_NODE
            )
        );
    }

    private <T> void assertAsync(Consumer<ActionListener<T>> function, Consumer<T> furtherTests) throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicBoolean listenerCalled = new AtomicBoolean(false);

        LatchedActionListener<T> listener = new LatchedActionListener<>(ActionListener.wrap(r -> {
            assertTrue("listener called more than once", listenerCalled.compareAndSet(false, true));
            furtherTests.accept(r);
        }, e -> {
            assertTrue("listener called more than once", listenerCalled.compareAndSet(false, true));
            fail("got unexpected exception: " + e);
        }), latch);

        function.accept(listener);
        assertTrue("timed out after 5s", latch.await(5, TimeUnit.SECONDS));
    }
}
