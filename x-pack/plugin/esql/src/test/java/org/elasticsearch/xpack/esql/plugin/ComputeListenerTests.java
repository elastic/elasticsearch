/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.node.VersionInformation;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.compute.operator.DriverProfile;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.TaskCancellationService;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.junit.After;
import org.junit.Before;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.elasticsearch.test.tasks.MockTaskManager.SPY_TASK_MANAGER_SETTING;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThan;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;

public class ComputeListenerTests extends ESTestCase {
    private ThreadPool threadPool;
    private TransportService transportService;

    @Before
    public void setUpTransportService() {
        threadPool = new TestThreadPool(getTestName());
        transportService = MockTransportService.createNewService(
            Settings.builder().put(SPY_TASK_MANAGER_SETTING.getKey(), true).build(),
            VersionInformation.CURRENT,
            TransportVersionUtils.randomVersion(),
            threadPool
        );
        transportService.start();
        TaskCancellationService cancellationService = new TaskCancellationService(transportService);
        transportService.getTaskManager().setTaskCancellationService(cancellationService);
        Mockito.clearInvocations(transportService.getTaskManager());
    }

    @After
    public void shutdownTransportService() {
        transportService.close();
        terminate(threadPool);
    }

    private CancellableTask newTask() {
        return new CancellableTask(
            randomIntBetween(1, 100),
            "test-type",
            "test-action",
            "test-description",
            TaskId.EMPTY_TASK_ID,
            Map.of()
        );
    }

    private ComputeResponse randomResponse() {
        int numProfiles = randomIntBetween(0, 2);
        List<DriverProfile> profiles = new ArrayList<>(numProfiles);
        for (int i = 0; i < numProfiles; i++) {
            profiles.add(new DriverProfile(randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong(), List.of()));
        }
        return new ComputeResponse(profiles);
    }

    public void testEmpty() {
        PlainActionFuture<ComputeResponse> results = new PlainActionFuture<>();
        try (ComputeListener ignored = new ComputeListener(transportService, newTask(), results)) {
            assertFalse(results.isDone());
        }
        assertTrue(results.isDone());
        assertThat(results.actionGet(10, TimeUnit.SECONDS).getProfiles(), empty());
    }

    public void testCollectComputeResults() {
        PlainActionFuture<ComputeResponse> future = new PlainActionFuture<>();
        List<DriverProfile> allProfiles = new ArrayList<>();
        try (ComputeListener computeListener = new ComputeListener(transportService, newTask(), future)) {
            int tasks = randomIntBetween(1, 100);
            for (int t = 0; t < tasks; t++) {
                if (randomBoolean()) {
                    ActionListener<Void> subListener = computeListener.acquireAvoid();
                    threadPool.schedule(
                        ActionRunnable.wrap(subListener, l -> l.onResponse(null)),
                        TimeValue.timeValueNanos(between(0, 100)),
                        threadPool.generic()
                    );
                } else {
                    ComputeResponse resp = randomResponse();
                    allProfiles.addAll(resp.getProfiles());
                    ActionListener<ComputeResponse> subListener = computeListener.acquireCompute();
                    threadPool.schedule(
                        ActionRunnable.wrap(subListener, l -> l.onResponse(resp)),
                        TimeValue.timeValueNanos(between(0, 100)),
                        threadPool.generic()
                    );
                }
            }
        }
        ComputeResponse result = future.actionGet(10, TimeUnit.SECONDS);
        assertThat(
            result.getProfiles().stream().collect(Collectors.toMap(p -> p, p -> 1, Integer::sum)),
            equalTo(allProfiles.stream().collect(Collectors.toMap(p -> p, p -> 1, Integer::sum)))
        );
        Mockito.verifyNoInteractions(transportService.getTaskManager());
    }

    public void testCancelOnFailure() throws Exception {
        Queue<Exception> rootCauseExceptions = ConcurrentCollections.newQueue();
        IntStream.range(0, between(1, 100))
            .forEach(
                n -> rootCauseExceptions.add(new CircuitBreakingException("breaking exception " + n, CircuitBreaker.Durability.TRANSIENT))
            );
        int successTasks = between(1, 50);
        int failedTasks = between(1, 100);
        PlainActionFuture<ComputeResponse> rootListener = new PlainActionFuture<>();
        CancellableTask rootTask = newTask();
        try (ComputeListener computeListener = new ComputeListener(transportService, rootTask, rootListener)) {
            for (int i = 0; i < successTasks; i++) {
                ActionListener<ComputeResponse> subListener = computeListener.acquireCompute();
                threadPool.schedule(
                    ActionRunnable.wrap(subListener, l -> l.onResponse(randomResponse())),
                    TimeValue.timeValueNanos(between(0, 100)),
                    threadPool.generic()
                );
            }
            for (int i = 0; i < failedTasks; i++) {
                ActionListener<?> subListener = randomBoolean() ? computeListener.acquireAvoid() : computeListener.acquireCompute();
                threadPool.schedule(ActionRunnable.wrap(subListener, l -> {
                    Exception ex = rootCauseExceptions.poll();
                    if (ex == null) {
                        ex = new TaskCancelledException("task was cancelled");
                    }
                    l.onFailure(ex);
                }), TimeValue.timeValueNanos(between(0, 100)), threadPool.generic());
            }
        }
        assertBusy(rootListener::isDone);
        ExecutionException failure = expectThrows(ExecutionException.class, () -> rootListener.get(1, TimeUnit.SECONDS));
        Throwable cause = failure.getCause();
        assertNotNull(failure);
        assertThat(cause, instanceOf(CircuitBreakingException.class));
        assertThat(failure.getSuppressed().length, lessThan(10));
        Mockito.verify(transportService.getTaskManager(), Mockito.times(1))
            .cancelTaskAndDescendants(eq(rootTask), eq("cancelled on failure"), eq(false), any());
    }

    public void testCollectWarnings() throws Exception {
        List<DriverProfile> allProfiles = new ArrayList<>();
        Map<String, Set<String>> allWarnings = new HashMap<>();
        ActionListener<ComputeResponse> rootListener = new ActionListener<>() {
            @Override
            public void onResponse(ComputeResponse result) {
                assertThat(
                    result.getProfiles().stream().collect(Collectors.toMap(p -> p, p -> 1, Integer::sum)),
                    equalTo(allProfiles.stream().collect(Collectors.toMap(p -> p, p -> 1, Integer::sum)))
                );
                Map<String, Set<String>> responseHeaders = threadPool.getThreadContext()
                    .getResponseHeaders()
                    .entrySet()
                    .stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, e -> new HashSet<>(e.getValue())));
                assertThat(responseHeaders, equalTo(allWarnings));
            }

            @Override
            public void onFailure(Exception e) {
                throw new AssertionError(e);
            }
        };
        CountDownLatch latch = new CountDownLatch(1);
        try (
            ComputeListener computeListener = new ComputeListener(
                transportService,
                newTask(),
                ActionListener.runAfter(rootListener, latch::countDown)
            )
        ) {
            int tasks = randomIntBetween(1, 100);
            for (int t = 0; t < tasks; t++) {
                if (randomBoolean()) {
                    ActionListener<Void> subListener = computeListener.acquireAvoid();
                    threadPool.schedule(
                        ActionRunnable.wrap(subListener, l -> l.onResponse(null)),
                        TimeValue.timeValueNanos(between(0, 100)),
                        threadPool.generic()
                    );
                } else {
                    ComputeResponse resp = randomResponse();
                    allProfiles.addAll(resp.getProfiles());
                    int numWarnings = randomIntBetween(1, 5);
                    Map<String, String> warnings = new HashMap<>();
                    for (int i = 0; i < numWarnings; i++) {
                        warnings.put("key" + between(1, 10), "value" + between(1, 10));
                    }
                    for (Map.Entry<String, String> e : warnings.entrySet()) {
                        allWarnings.computeIfAbsent(e.getKey(), v -> new HashSet<>()).add(e.getValue());
                    }
                    ActionListener<ComputeResponse> subListener = computeListener.acquireCompute();
                    threadPool.schedule(ActionRunnable.wrap(subListener, l -> {
                        for (Map.Entry<String, String> e : warnings.entrySet()) {
                            threadPool.getThreadContext().addResponseHeader(e.getKey(), e.getValue());
                        }
                        l.onResponse(resp);
                    }), TimeValue.timeValueNanos(between(0, 100)), threadPool.generic());
                }
            }
        }
        assertTrue(latch.await(10, TimeUnit.SECONDS));
        Mockito.verifyNoInteractions(transportService.getTaskManager());
    }
}
