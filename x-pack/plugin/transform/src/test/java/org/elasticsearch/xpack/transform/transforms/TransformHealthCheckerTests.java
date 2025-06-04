/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.transforms;

import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.health.HealthStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.indexing.IndexerState;
import org.elasticsearch.xpack.core.transform.transforms.TransformHealth;
import org.elasticsearch.xpack.core.transform.transforms.TransformState;
import org.elasticsearch.xpack.core.transform.transforms.TransformTaskState;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransformHealthCheckerTests extends ESTestCase {

    public void testGreen() {
        TransformTask task = mock(TransformTask.class);
        TransformContext context = createTestContext();

        withIdStateAndContext(task, randomAlphaOfLength(10), context);
        assertThat(TransformHealthChecker.checkTransform(task), equalTo(TransformHealth.GREEN));
    }

    public void testPersistenceFailure() {
        TransformTask task = mock(TransformTask.class);
        TransformContext context = createTestContext();
        Instant now = getNow();

        withIdStateAndContext(task, randomAlphaOfLength(10), context);
        assertThat(TransformHealthChecker.checkTransform(task), equalTo(TransformHealth.GREEN));

        context.incrementAndGetStatePersistenceFailureCount(new ElasticsearchException("failed to persist"));
        TransformHealth health = TransformHealthChecker.checkTransform(task);
        assertThat(health.getStatus(), equalTo(HealthStatus.YELLOW));
        assertEquals(1, health.getIssues().size());
        assertThat(health.getIssues().get(0).getIssue(), equalTo("Task encountered failures updating internal state"));
        assertThat(health.getIssues().get(0).getFirstOccurrence(), greaterThanOrEqualTo(now));

        assertThat(health.getIssues().get(0).getFirstOccurrence(), lessThan(Instant.MAX));

        context.resetStatePersistenceFailureCount();
        assertThat(TransformHealthChecker.checkTransform(task), equalTo(TransformHealth.GREEN));
    }

    public void testStatusSwitchingAndMultipleFailures() {
        TransformTask task = mock(TransformTask.class);
        TransformContext context = createTestContext();
        Instant now = getNow();

        withIdStateAndContext(task, randomAlphaOfLength(10), context);
        assertThat(TransformHealthChecker.checkTransform(task), equalTo(TransformHealth.GREEN));

        context.incrementAndGetFailureCount(new ElasticsearchException("internal error"));
        TransformHealth health = TransformHealthChecker.checkTransform(task);
        assertThat(health.getStatus(), equalTo(HealthStatus.YELLOW));

        Instant firstOccurrence = health.getIssues().get(0).getFirstOccurrence();
        assertThat(firstOccurrence, greaterThanOrEqualTo(now));
        assertThat(firstOccurrence, lessThan(Instant.MAX));

        for (int i = 1; i < TransformHealthChecker.RED_STATUS_FAILURE_COUNT_BOUNDARY; ++i) {
            context.incrementAndGetFailureCount(new ElasticsearchException("internal error"));
            assertThat(TransformHealthChecker.checkTransform(task).getStatus(), equalTo(HealthStatus.YELLOW));
            assertEquals(1, TransformHealthChecker.checkTransform(task).getIssues().size());
            assertThat(health.getIssues().get(0).getFirstOccurrence(), equalTo(firstOccurrence));
        }

        // turn RED
        context.incrementAndGetFailureCount(new ElasticsearchException("internal error"));
        assertThat(TransformHealthChecker.checkTransform(task).getStatus(), equalTo(HealthStatus.RED));
        assertEquals(1, TransformHealthChecker.checkTransform(task).getIssues().size());
        assertThat(health.getIssues().get(0).getFirstOccurrence(), equalTo(firstOccurrence));

        // add a persistence error
        context.incrementAndGetStatePersistenceFailureCount(new ElasticsearchException("failed to persist"));
        assertThat(TransformHealthChecker.checkTransform(task).getStatus(), equalTo(HealthStatus.RED));
        assertEquals(2, TransformHealthChecker.checkTransform(task).getIssues().size());

        // reset the indexer error
        context.resetReasonAndFailureCounter();
        assertThat(TransformHealthChecker.checkTransform(task).getStatus(), equalTo(HealthStatus.YELLOW));
        assertEquals(1, TransformHealthChecker.checkTransform(task).getIssues().size());
        assertThat(
            TransformHealthChecker.checkTransform(task).getIssues().get(0).getIssue(),
            equalTo("Task encountered failures updating internal state")
        );
        context.resetStatePersistenceFailureCount();
        assertThat(TransformHealthChecker.checkTransform(task), equalTo(TransformHealth.GREEN));
    }

    public void testStartUpFailures() {
        var task = mock(TransformTask.class);
        var context = createTestContext();
        var now = getNow();

        withIdStateAndContext(task, randomAlphaOfLength(10), context);
        assertThat(TransformHealthChecker.checkTransform(task), equalTo(TransformHealth.GREEN));

        context.incrementAndGetStartUpFailureCount(new ElasticsearchException("failed to persist"));

        var health = TransformHealthChecker.checkTransform(task);
        assertThat(health.getStatus(), equalTo(HealthStatus.YELLOW));
        assertEquals(1, health.getIssues().size());
        assertThat(health.getIssues().get(0).getIssue(), equalTo("Transform task is automatically retrying its startup process"));
        assertThat(health.getIssues().get(0).getFirstOccurrence(), greaterThanOrEqualTo(now));
        assertThat(health.getIssues().get(0).getFirstOccurrence(), lessThan(Instant.MAX));

        IntStream.range(0, 10).forEach(i -> context.incrementAndGetStartUpFailureCount(new ElasticsearchException("failed to persist")));
        assertThat("Start up failures should always be yellow regardless of count", health.getStatus(), equalTo(HealthStatus.YELLOW));

        context.resetStartUpFailureCount();
        assertThat(TransformHealthChecker.checkTransform(task), equalTo(TransformHealth.GREEN));
    }

    private TransformContext createTestContext() {
        return new TransformContext(TransformTaskState.STARTED, "", 0, mock(TransformContext.Listener.class));
    }

    private static Instant getNow() {
        return Instant.now().truncatedTo(ChronoUnit.MILLIS);
    }

    private static void withIdStateAndContext(TransformTask task, String transformId, TransformContext context) {
        when(task.getTransformId()).thenReturn(transformId);
        when(task.getState()).thenReturn(
            new TransformState(TransformTaskState.STARTED, IndexerState.INDEXING, null, 0, "", null, null, false, null)
        );
        when(task.getContext()).thenReturn(context);
    }
}
