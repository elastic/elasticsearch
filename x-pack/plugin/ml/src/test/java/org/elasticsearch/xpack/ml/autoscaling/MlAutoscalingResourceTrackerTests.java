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

    public void testGetMemoryAndCpuPerNodeMemoryInBytes() throws InterruptedException {
        MlAutoscalingContext mlAutoscalingContext = new MlAutoscalingContext();
        MlMemoryTracker mockTracker = mock(MlMemoryTracker.class);

        long memory = randomLongBetween(100, 1_000_000);
        this.<MlAutoscalingStats>assertAsync(
            listener -> MlAutoscalingResourceTracker.getMemoryAndCpu(
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
            listener -> MlAutoscalingResourceTracker.getMemoryAndCpu(
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

    public void testTryMoveJobsByMemory() {
        assertEquals(0L, MlAutoscalingResourceTracker.tryMoveJobsByMemoryInLeastEfficientWay(List.of(10L), Map.of("node_a", 100L), 1000L));
        assertEquals(10L, MlAutoscalingResourceTracker.tryMoveJobsByMemoryInLeastEfficientWay(List.of(10L), Map.of("node_a", 995L), 1000L));

        // equal sizes fit on all nodes
        assertEquals(
            0L,
            MlAutoscalingResourceTracker.tryMoveJobsByMemoryInLeastEfficientWay(
                List.of(10L, 10L, 10L, 10L, 10L),
                Map.of("node_a", 976L, "node_b", 986L, "node_c", 967L),
                1000L
            )
        );

        assertEquals(
            0L,
            MlAutoscalingResourceTracker.tryMoveJobsByMemoryInLeastEfficientWay(
                List.of(10L, 10L, 10L, 10L, 10L),
                Map.of("node_a", 980L, "node_b", 990L, "node_c", 970L),
                1000L
            )
        );

        // doesn't fit
        assertEquals(
            10L,
            MlAutoscalingResourceTracker.tryMoveJobsByMemoryInLeastEfficientWay(
                List.of(10L, 10L, 10L, 10L, 10L, 10L, 10L),
                Map.of("node_a", 976L, "node_b", 986L, "node_c", 967L),
                1000L
            )
        );
        assertEquals(
            40L,
            MlAutoscalingResourceTracker.tryMoveJobsByMemoryInLeastEfficientWay(
                List.of(10L, 10L, 10L, 10L, 10L, 10L, 40L),
                Map.of("node_a", 976L, "node_b", 946L, "node_c", 967L),
                1000L
            )
        );

        assertEquals(
            130L,
            MlAutoscalingResourceTracker.tryMoveJobsByMemoryInLeastEfficientWay(
                List.of(10L, 20L, 30L, 40L, 50L, 60L, 70L), // 280, with better packing this could return 20 + 50
                Map.of("node_a", 886L, "node_b", 926L, "node_c", 967L),
                1000L
            )
        );

        assertEquals(
            70L,
            MlAutoscalingResourceTracker.tryMoveJobsByMemoryInLeastEfficientWay(
                List.of(10L, 20L, 30L, 40L, 50L, 60L, 70L), // 280, solvable with optimal packing
                Map.of("node_a", 886L, "node_b", 906L, "node_c", 917L),
                1000L
            )
        );

        assertEquals(
            70L,
            MlAutoscalingResourceTracker.tryMoveJobsByMemoryInLeastEfficientWay(
                List.of(10L, 20L, 30L, 40L, 50L, 60L, 70L), // 280, solvable with optimal packing
                Map.of("node_a", 866L, "node_b", 886L, "node_c", 917L),
                1000L
            )
        );

        assertEquals(
            500L,
            MlAutoscalingResourceTracker.tryMoveJobsByMemoryInLeastEfficientWay(
                List.of(500L, 200L),
                Map.of("node_a", 1400L, "node_b", 1700L),
                2000L
            )
        );

        assertEquals(
            700L,
            MlAutoscalingResourceTracker.tryMoveJobsByMemoryInLeastEfficientWay(List.of(500L, 200L), Collections.emptyMap(), 2000L)
        );

        assertEquals(
            0L,
            MlAutoscalingResourceTracker.tryMoveJobsByMemoryInLeastEfficientWay(Collections.emptyList(), Collections.emptyMap(), 2000L)
        );

        assertEquals(
            0L,
            MlAutoscalingResourceTracker.tryMoveJobsByMemoryInLeastEfficientWay(
                Collections.emptyList(),
                Map.of("node_a", 1400L, "node_b", 1700L),
                2000L
            )
        );
    }

    public void testTryRemoveOneNode() {
        assertEquals(
            true,
            MlAutoscalingResourceTracker.tryRemoveOneNode(
                Map.of("node_a", List.of(100L, 200L, 300L), "node_b", List.of(200L, 300L), "node_c", List.of(10L, 10L, 10L, 10L, 10L)),
                600L
            )
        );

        assertEquals(
            false,
            MlAutoscalingResourceTracker.tryRemoveOneNode(
                Map.of("node_a", List.of(100L, 200L, 300L), "node_b", List.of(280L, 300L), "node_c", List.of(10L, 10L, 10L, 10L, 10L)),
                600L
            )
        );

        assertEquals(false, MlAutoscalingResourceTracker.tryRemoveOneNode(Map.of("node_a", List.of(10L, 10L, 10L, 10L, 10L)), 600L));

        assertEquals(false, MlAutoscalingResourceTracker.tryRemoveOneNode(Collections.emptyMap(), 999L));

        // solvable case with optimal packing, but not possible if badly packed
        assertEquals(
            false,
            MlAutoscalingResourceTracker.tryRemoveOneNode(
                Map.of(
                    "node_a",
                    List.of(100L, 200L, 300L),
                    "node_b",
                    List.of(280L, 300L),
                    "node_c",
                    List.of(500L, 10L, 10L, 10L, 10L, 10L)
                ),
                1000L
            )
        );

        // same with smaller jobs, that can be re-arranged
        assertEquals(
            true,
            MlAutoscalingResourceTracker.tryRemoveOneNode(
                Map.of(
                    "node_a",
                    List.of(100L, 200L, 300L),
                    "node_b",
                    List.of(280L, 300L),
                    "node_c",
                    List.of(100L, 100L, 100L, 100L, 100L, 10L, 10L, 10L, 10L, 10L)
                ),
                1000L
            )
        );

        assertEquals(
            true,
            MlAutoscalingResourceTracker.tryRemoveOneNode(
                Map.of(
                    "node_a",
                    List.of(100L, 200L, 300L),
                    "node_b",
                    List.of(280L, 300L),
                    "node_c",
                    List.of(100L, 100L, 100L, 100L, 100L, 50L, 10L, 10L, 10L, 10L)
                ),
                1000L
            )
        );

        assertEquals(
            true,
            MlAutoscalingResourceTracker.tryRemoveOneNode(
                Map.of(
                    "node_a",
                    List.of(100L, 200L, 300L),
                    "node_b",
                    List.of(280L, 325L),
                    "node_c",
                    List.of(100L, 100L, 100L, 100L, 100L, 50L, 10L, 10L, 10L, 10L)
                ),
                1000L
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
