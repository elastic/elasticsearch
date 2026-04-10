/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.checkpoint;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.client.internal.ParentTaskAssigningClient;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.indices.TestIndexNameExpressionResolver;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.test.transport.StubLinkedProjectConfigService;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.transform.action.GetCheckpointAction;
import org.elasticsearch.xpack.core.transform.transforms.TransformCheckpoint;
import org.elasticsearch.xpack.core.transform.transforms.TransformCheckpointStats;
import org.elasticsearch.xpack.core.transform.transforms.TransformCheckpointingInfo;
import org.elasticsearch.xpack.core.transform.transforms.TransformCheckpointingInfo.TransformCheckpointingInfoBuilder;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfigTests;
import org.elasticsearch.xpack.core.transform.transforms.TransformIndexerPosition;
import org.elasticsearch.xpack.core.transform.transforms.TransformIndexerPositionTests;
import org.elasticsearch.xpack.core.transform.transforms.TransformParsingContext;
import org.elasticsearch.xpack.core.transform.transforms.TransformProgress;
import org.elasticsearch.xpack.core.transform.transforms.TransformProgressTests;
import org.elasticsearch.xpack.transform.TransformSingleNodeTestCase;
import org.elasticsearch.xpack.transform.notifications.TransformAuditor;
import org.elasticsearch.xpack.transform.persistence.IndexBasedTransformConfigManager;
import org.junit.AfterClass;
import org.junit.Before;

import java.time.Clock;
import java.time.Instant;
import java.util.Collections;
import java.util.Map;

import static org.mockito.Mockito.mock;

public class TransformCheckpointServiceNodeTests extends TransformSingleNodeTestCase {

    // re-use the mock client for the whole test suite as the underlying thread pool and the
    // corresponding context if recreated cause unreliable test execution
    // see https://github.com/elastic/elasticsearch/issues/45238 and https://github.com/elastic/elasticsearch/issues/42577
    private static TestThreadPool threadPool;
    private static MockClientForCheckpointing mockClientForCheckpointing = null;

    private IndexBasedTransformConfigManager transformsConfigManager;
    private TransformCheckpointService transformCheckpointService;

    private class MockClientForCheckpointing extends NoOpClient {

        private volatile Map<String, long[]> checkpoints;

        MockClientForCheckpointing(ThreadPool threadPool) {
            super(threadPool);
        }

        void setCheckpoints(Map<String, long[]> checkpoints) {
            this.checkpoints = checkpoints;
        }

        @SuppressWarnings("unchecked")
        @Override
        protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
            ActionType<Response> action,
            Request request,
            ActionListener<Response> listener
        ) {
            if (request instanceof GetCheckpointAction.Request) {
                final GetCheckpointAction.Response getCheckpointResponse = new GetCheckpointAction.Response(checkpoints);
                listener.onResponse((Response) getCheckpointResponse);
                return;
            }

            super.doExecute(action, request, listener);
        }
    }

    @Before
    public void createComponents() {
        // it's not possible to run it as @BeforeClass as clients aren't initialized
        if (threadPool == null) {
            threadPool = new TestThreadPool("TransformCheckpointServiceNodeTests");
        }
        if (mockClientForCheckpointing == null) {
            mockClientForCheckpointing = new MockClientForCheckpointing(threadPool);
        }
        ClusterService clusterService = mock(ClusterService.class);
        transformsConfigManager = new IndexBasedTransformConfigManager(
            clusterService,
            TestIndexNameExpressionResolver.newInstance(),
            client(),
            xContentRegistry(),
            new TransformParsingContext(false)
        );

        // use a mock for the checkpoint service
        TransformAuditor mockAuditor = mock(TransformAuditor.class);
        transformCheckpointService = new TransformCheckpointService(
            Clock.systemUTC(),
            Settings.EMPTY,
            StubLinkedProjectConfigService.INSTANCE,
            transformsConfigManager,
            mockAuditor
        );
    }

    @AfterClass
    public static void tearDownClient() {
        mockClientForCheckpointing = null;
        threadPool.close();
        threadPool = null;
    }

    public void testCreateReadDeleteCheckpoint() throws InterruptedException {
        String transformId = randomAlphaOfLengthBetween(3, 10);
        long timestamp = 1000;

        TransformCheckpoint checkpoint = new TransformCheckpoint(
            transformId,
            timestamp,
            1L,
            createCheckPointMap(transformId, 10, 10, 10),
            null
        );

        // create transform
        assertAsync(
            listener -> transformsConfigManager.putTransformConfiguration(
                TransformConfigTests.randomTransformConfig(transformId),
                listener
            ),
            true,
            null,
            null
        );

        // by design no exception is thrown but an empty checkpoint is returned
        assertAsync(
            listener -> transformsConfigManager.getTransformCheckpoint(transformId, 1L, listener),
            TransformCheckpoint.EMPTY,
            null,
            null
        );

        assertAsync(listener -> transformsConfigManager.putTransformCheckpoint(checkpoint, listener), true, null, null);

        assertAsync(listener -> transformsConfigManager.getTransformCheckpoint(transformId, 1L, listener), checkpoint, null, null);

        // add a 2nd checkpoint
        TransformCheckpoint checkpoint2 = new TransformCheckpoint(
            transformId,
            timestamp + 100L,
            2L,
            createCheckPointMap(transformId, 20, 20, 20),
            null
        );

        assertAsync(listener -> transformsConfigManager.putTransformCheckpoint(checkpoint2, listener), true, null, null);

        // both checkpoints should be there
        assertAsync(listener -> transformsConfigManager.getTransformCheckpoint(transformId, 1L, listener), checkpoint, null, null);
        assertAsync(listener -> transformsConfigManager.getTransformCheckpoint(transformId, 2L, listener), checkpoint2, null, null);

        // delete transform
        assertAsync(listener -> transformsConfigManager.deleteTransform(transformId, listener), true, null, null);

        // checkpoints should be empty again
        assertAsync(
            listener -> transformsConfigManager.getTransformCheckpoint(transformId, 1L, listener),
            TransformCheckpoint.EMPTY,
            null,
            null
        );

        assertAsync(
            listener -> transformsConfigManager.getTransformCheckpoint(transformId, 2L, listener),
            TransformCheckpoint.EMPTY,
            null,
            null
        );
    }

    public void testGetCheckpointStats() throws InterruptedException {
        String transformId = randomAlphaOfLengthBetween(3, 10);
        long timestamp = 1000;
        TransformIndexerPosition position = TransformIndexerPositionTests.randomTransformIndexerPosition();
        TransformProgress progress = TransformProgressTests.randomTransformProgress();

        // create transform
        assertAsync(
            listener -> transformsConfigManager.putTransformConfiguration(
                TransformConfigTests.randomTransformConfig(transformId),
                listener
            ),
            true,
            null,
            null
        );

        TransformCheckpoint checkpoint = new TransformCheckpoint(
            transformId,
            timestamp,
            1L,
            createCheckPointMap(transformId, 10, 10, 10),
            null
        );

        assertAsync(listener -> transformsConfigManager.putTransformCheckpoint(checkpoint, listener), true, null, null);

        TransformCheckpoint checkpoint2 = new TransformCheckpoint(
            transformId,
            timestamp + 100L,
            2L,
            createCheckPointMap(transformId, 20, 20, 20),
            null
        );

        assertAsync(listener -> transformsConfigManager.putTransformCheckpoint(checkpoint2, listener), true, null, null);

        mockClientForCheckpointing.setCheckpoints(createCheckPointMap(transformId, 20, 20, 20));
        TransformCheckpointingInfo checkpointInfo = new TransformCheckpointingInfo(
            new TransformCheckpointStats(1, null, null, timestamp, 0L),
            new TransformCheckpointStats(2, position, progress, timestamp + 100L, 0L),
            30L,
            Instant.ofEpochMilli(timestamp),
            null
        );

        assertAsync(
            listener -> getCheckpoint(transformCheckpointService, transformId, 1, position, progress, listener),
            checkpointInfo,
            null,
            null
        );

        mockClientForCheckpointing.setCheckpoints(createCheckPointMap(transformId, 10, 50, 33));
        checkpointInfo = new TransformCheckpointingInfo(
            new TransformCheckpointStats(1, null, null, timestamp, 0L),
            new TransformCheckpointStats(2, position, progress, timestamp + 100L, 0L),
            63L,
            Instant.ofEpochMilli(timestamp),
            null
        );
        assertAsync(
            listener -> getCheckpoint(transformCheckpointService, transformId, 1, position, progress, listener),
            checkpointInfo,
            null,
            null
        );

        // same as current
        mockClientForCheckpointing.setCheckpoints(createCheckPointMap(transformId, 10, 10, 10));
        checkpointInfo = new TransformCheckpointingInfo(
            new TransformCheckpointStats(1, null, null, timestamp, 0L),
            new TransformCheckpointStats(2, position, progress, timestamp + 100L, 0L),
            0L,
            Instant.ofEpochMilli(timestamp),
            null
        );
        assertAsync(
            listener -> getCheckpoint(transformCheckpointService, transformId, 1, position, progress, listener),
            checkpointInfo,
            null,
            null
        );
    }

    private static Map<String, long[]> createCheckPointMap(
        String index,
        long checkpointShard1,
        long checkpointShard2,
        long checkpointShard3
    ) {
        return Collections.singletonMap(index, new long[] { checkpointShard1, checkpointShard2, checkpointShard3 });
    }

    private static void getCheckpoint(
        TransformCheckpointService transformCheckpointService,
        String transformId,
        long lastCheckpointNumber,
        TransformIndexerPosition nextCheckpointPosition,
        TransformProgress nextCheckpointProgress,
        ActionListener<TransformCheckpointingInfo> listener
    ) {
        ActionListener<TransformCheckpointingInfoBuilder> checkPointInfoListener = listener.delegateFailureAndWrap(
            (l, infoBuilder) -> l.onResponse(infoBuilder.build())
        );
        transformCheckpointService.getCheckpointingInfo(
            new ParentTaskAssigningClient(mockClientForCheckpointing, new TaskId("dummy-node:123456")),
            TimeValue.timeValueSeconds(5),
            transformId,
            lastCheckpointNumber,
            nextCheckpointPosition,
            nextCheckpointProgress,
            checkPointInfoListener
        );
    }

}
