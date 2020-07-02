/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ml.action.DeleteExpiredDataAction;
import org.elasticsearch.xpack.ml.job.persistence.JobConfigProvider;
import org.elasticsearch.xpack.ml.job.retention.MlDataRemover;
import org.junit.After;
import org.junit.Before;

import java.time.Clock;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.mockito.Mockito.mock;

public class TransportDeleteExpiredDataActionTests extends ESTestCase {

    private ThreadPool threadPool;
    private TransportDeleteExpiredDataAction transportDeleteExpiredDataAction;

    /**
     * A data remover that only checks for timeouts.
     */
    private static class DummyDataRemover implements MlDataRemover {

        public void remove(
            float requestsPerSec,
            ActionListener<Boolean> listener,
            Supplier<Boolean> isTimedOutSupplier
        ) {
            listener.onResponse(isTimedOutSupplier.get() == false);
        }
    }

    @Before
    public void setup() {
        threadPool = new TestThreadPool("TransportDeleteExpiredDataActionTests thread pool");
        TransportService transportService = mock(TransportService.class);
        Client client = mock(Client.class);
        ClusterService clusterService = mock(ClusterService.class);
        transportDeleteExpiredDataAction = new TransportDeleteExpiredDataAction(threadPool, ThreadPool.Names.SAME, transportService,
            new ActionFilters(Collections.emptySet()), client, clusterService, mock(JobConfigProvider.class), Clock.systemUTC());
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

        Supplier<Boolean> isTimedOutSupplier = () -> false;

        transportDeleteExpiredDataAction.deleteExpiredData(removers.iterator(), 1.0f, finalListener, isTimedOutSupplier, true);

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

        Supplier<Boolean> isTimedOutSupplier = () -> (removersRemaining.getAndDecrement() <= 0);

        transportDeleteExpiredDataAction.deleteExpiredData(removers.iterator(), 1.0f, finalListener, isTimedOutSupplier, true);

        assertFalse(succeeded.get());
    }
}
