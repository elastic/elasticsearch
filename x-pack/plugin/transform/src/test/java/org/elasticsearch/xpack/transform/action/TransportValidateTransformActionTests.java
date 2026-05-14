/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.action;

import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.XPackFeatures;
import org.elasticsearch.xpack.core.transform.action.ValidateTransformAction;
import org.elasticsearch.xpack.core.transform.transforms.DestConfig;
import org.elasticsearch.xpack.core.transform.transforms.SourceConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;
import org.elasticsearch.xpack.transform.TransformServices;
import org.elasticsearch.xpack.transform.transforms.scheduling.TransformScheduler;
import org.junit.After;
import org.junit.Before;

import java.time.Clock;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.containsString;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransportValidateTransformActionTests extends ESTestCase {

    private ThreadPool threadPool;

    @Before
    public void setUpThreadPool() {
        threadPool = new TestThreadPool(getTestName());
    }

    @After
    public void tearDownThreadPool() {
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
    }

    public void testOpTypeCreate_withFeatureUnsupported_failsValidation() {
        TransportValidateTransformAction action = newActionWithClusterFeature(false);

        ValidateTransformAction.Request request = new ValidateTransformAction.Request(
            configWithOpType(DocWriteRequest.OpType.CREATE),
            randomBoolean(),
            TimeValue.THIRTY_SECONDS
        );
        PlainActionFuture<ValidateTransformAction.Response> future = new PlainActionFuture<>();
        action.doExecute(mock(Task.class), request, future);

        ValidationException e = expectThrows(ValidationException.class, () -> future.actionGet(TimeValue.THIRTY_SECONDS));
        assertThat(e.getMessage(), containsString("[dest.op_type] is [create]"));
        assertThat(e.getMessage(), containsString("not yet supported by all nodes in the cluster"));
    }

    /**
     * Constructs a {@link TransportValidateTransformAction} with all dependencies mocked, configured so that
     * {@link FeatureService#clusterHasFeature(ClusterState, org.elasticsearch.features.NodeFeature)} returns the given
     * value for the {@code transform.dest.op_type} feature. {@link TransformScheduler} is a final class and cannot be
     * mocked, so it is constructed with a real {@link TestThreadPool}. All other dependencies use Mockito defaults — the
     * test relies on the gate firing before they are touched.
     */
    private TransportValidateTransformAction newActionWithClusterFeature(boolean featureSupported) {
        FeatureService featureService = mock(FeatureService.class);
        when(featureService.clusterHasFeature(any(), eq(XPackFeatures.TRANSFORM_DEST_OP_TYPE))).thenReturn(featureSupported);

        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(ClusterState.EMPTY_STATE);

        // Client.threadPool() is dereferenced by RemoteClusterLicenseChecker during action construction when the node has
        // the remote_cluster_client role (the default with Settings.EMPTY).
        Client client = mock(Client.class);
        when(client.threadPool()).thenReturn(threadPool);

        TransformServices transformServices = new TransformServices(
            mock(),
            mock(),
            mock(),
            new TransformScheduler(Clock.systemUTC(), threadPool, Settings.EMPTY, TimeValue.ZERO),
            mock(),
            mock(),
            pid -> false,
            mock()
        );

        return new TransportValidateTransformAction(
            mock(),
            mock(ActionFilters.class),
            client,
            mock(),
            clusterService,
            Settings.EMPTY,
            mock(),
            transformServices,
            featureService,
            mock()
        );
    }

    private static TransformConfig configWithOpType(DocWriteRequest.OpType opType) {
        return TransformConfig.builder()
            .setId("test-transform")
            .setSource(new SourceConfig("source-index"))
            .setDest(new DestConfig("dest-index", null, null, opType))
            .build();
    }
}
