/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.rollup.action;

import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockUtils;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureResponse;
import org.elasticsearch.xpack.core.rollup.RollupFeatureSetUsage;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.mock;

public class RollupInfoTransportActionTests extends ESTestCase {

    public void testAvailable() {
        TransportService transportService = MockUtils.setupTransportServiceWithThreadpoolExecutor();
        RollupInfoTransportAction featureSet = new RollupInfoTransportAction(transportService, mock(ActionFilters.class));
        assertThat(featureSet.available(), is(true));
    }

    public void testEnabledDefault() {
        TransportService transportService = MockUtils.setupTransportServiceWithThreadpoolExecutor();
        RollupInfoTransportAction featureSet = new RollupInfoTransportAction(transportService, mock(ActionFilters.class));
        assertThat(featureSet.enabled(), is(true));
    }

    public void testUsage() throws ExecutionException, InterruptedException, IOException {
        ThreadPool threadPool = mock(ThreadPool.class);
        TransportService transportService = MockUtils.setupTransportServiceWithThreadpoolExecutor(threadPool);
        var usageAction = new RollupUsageTransportAction(transportService, null, threadPool, mock(ActionFilters.class));
        PlainActionFuture<XPackUsageFeatureResponse> future = new PlainActionFuture<>();
        usageAction.localClusterStateOperation(null, null, ClusterState.EMPTY_STATE, future);
        RollupFeatureSetUsage rollupUsage = (RollupFeatureSetUsage) future.get().getUsage();
        BytesStreamOutput out = new BytesStreamOutput();
        rollupUsage.writeTo(out);
        var serializedUsage = new RollupFeatureSetUsage(out.bytes().streamInput());
        assertThat(rollupUsage.name(), is(serializedUsage.name()));
        assertThat(rollupUsage.enabled(), is(serializedUsage.enabled()));
        assertThat(rollupUsage.enabled(), is(serializedUsage.enabled()));
        assertThat(rollupUsage.getNumberOfRollupJobs(), equalTo(serializedUsage.getNumberOfRollupJobs()));
    }

}
