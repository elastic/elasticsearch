/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ml.action.DeleteExpiredDataAction;
import org.elasticsearch.xpack.ml.job.persistence.JobConfigProvider;
import org.elasticsearch.xpack.ml.job.persistence.JobResultsProvider;
import org.elasticsearch.xpack.ml.job.retention.MlDataRemover;
import org.elasticsearch.xpack.ml.notifications.AnomalyDetectionAuditor;
import org.junit.After;
import org.junit.Before;

import java.time.Clock;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class TransportDeleteExpiredDataActionTests extends ESTestCase {

    private ThreadPool threadPool;
    private TransportDeleteExpiredDataAction transportDeleteExpiredDataAction;
    private AnomalyDetectionAuditor auditor;

    /**
     * A data remover that only checks for timeouts.
     */
    private static class DummyDataRemover implements MlDataRemover {

        public void remove(float requestsPerSec, ActionListener<Boolean> listener, BooleanSupplier isTimedOutSupplier) {
            listener.onResponse(isTimedOutSupplier.getAsBoolean() == false);
        }
    }

    @Before
    public void setup() {
        threadPool = new TestThreadPool("TransportDeleteExpiredDataActionTests thread pool");
        TransportService transportService = mock(TransportService.class);
        Client client = mock(Client.class);
        ClusterService clusterService = mock(ClusterService.class);
        auditor = mock(AnomalyDetectionAuditor.class);
        transportDeleteExpiredDataAction = new TransportDeleteExpiredDataAction(
            threadPool,
            ThreadPool.Names.SAME,
            transportService,
            new ActionFilters(Collections.emptySet()),
            client,
            clusterService,
            mock(JobConfigProvider.class),
            mock(JobResultsProvider.class),
            auditor,
            Clock.systemUTC()
        );
    }

    @After
    public void teardown() {
        threadPool.shutdown();
    }

    public void testDeleteExpiredDataIterationNoTimeout() {

        final int numRemovers = randomIntBetween(2, 5);

        List<MlDataRemover> removers = Stream.generate(DummyDataRemover::new).limit(numRemovers).collect(Collectors.toList());

        AtomicBoolean succeeded = new AtomicBoolean();
        ActionListener<DeleteExpiredDataAction.Response> finalListener = ActionListener.wrap(
            response -> succeeded.set(response.isDeleted()),
            e -> fail(e.getMessage())
        );

        BooleanSupplier isTimedOutSupplier = () -> false;

        DeleteExpiredDataAction.Request request = new DeleteExpiredDataAction.Request(null, null);
        transportDeleteExpiredDataAction.deleteExpiredData(request, removers.iterator(), 1.0f, finalListener, isTimedOutSupplier, true);

        assertTrue(succeeded.get());
    }

    public void testDeleteExpiredDataIterationWithTimeout() {

        final int numRemovers = randomIntBetween(2, 5);
        AtomicInteger removersRemaining = new AtomicInteger(randomIntBetween(0, numRemovers - 1));

        List<MlDataRemover> removers = Stream.generate(DummyDataRemover::new).limit(numRemovers).collect(Collectors.toList());

        AtomicBoolean succeeded = new AtomicBoolean();
        ActionListener<DeleteExpiredDataAction.Response> finalListener = ActionListener.wrap(
            response -> succeeded.set(response.isDeleted()),
            e -> fail(e.getMessage())
        );

        BooleanSupplier isTimedOutSupplier = () -> (removersRemaining.getAndDecrement() <= 0);

        DeleteExpiredDataAction.Request request = new DeleteExpiredDataAction.Request(null, null);
        request.setJobId("_all");
        transportDeleteExpiredDataAction.deleteExpiredData(request, removers.iterator(), 1.0f, finalListener, isTimedOutSupplier, true);
        assertFalse(succeeded.get());

        verify(auditor, times(1)).warning(
            "",
            "Deleting expired ML data was cancelled after the timeout period of [8h] was exceeded. "
                + "The setting [xpack.ml.nightly_maintenance_requests_per_second] "
                + "controls the deletion rate, consider increasing the value to assist in pruning old data"
        );
        verifyNoMoreInteractions(auditor);
    }

    public void testDeleteExpiredDataIterationWithTimeout_GivenJobIds() {

        final int numRemovers = randomIntBetween(2, 5);
        AtomicInteger removersRemaining = new AtomicInteger(randomIntBetween(0, numRemovers - 1));

        List<MlDataRemover> removers = Stream.generate(DummyDataRemover::new).limit(numRemovers).collect(Collectors.toList());

        AtomicBoolean succeeded = new AtomicBoolean();
        ActionListener<DeleteExpiredDataAction.Response> finalListener = ActionListener.wrap(
            response -> succeeded.set(response.isDeleted()),
            e -> fail(e.getMessage())
        );

        BooleanSupplier isTimedOutSupplier = () -> (removersRemaining.getAndDecrement() <= 0);

        DeleteExpiredDataAction.Request request = new DeleteExpiredDataAction.Request(null, null);
        request.setJobId("foo*");
        request.setExpandedJobIds(new String[] { "foo1", "foo2" });
        transportDeleteExpiredDataAction.deleteExpiredData(request, removers.iterator(), 1.0f, finalListener, isTimedOutSupplier, true);
        assertFalse(succeeded.get());

        verify(auditor, times(1)).warning(eq("foo1"), anyString());
        verify(auditor, times(1)).warning(eq("foo2"), anyString());
        verifyNoMoreInteractions(auditor);
    }
}
