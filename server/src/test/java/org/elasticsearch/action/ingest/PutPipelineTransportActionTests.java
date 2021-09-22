/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.ingest;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.ingest.IngestMetadata;
import org.elasticsearch.ingest.IngestService;
import org.elasticsearch.ingest.PipelineConfiguration;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpNodeClient;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PutPipelineTransportActionTests extends ESTestCase {

    public void testUpdatingPipelineWithoutChangesIsNoOp() throws Exception {
        var threadPool = mock(ThreadPool.class);
        when(threadPool.generic()).thenReturn(EsExecutors.DIRECT_EXECUTOR_SERVICE);
        when(threadPool.executor(anyString())).thenReturn(EsExecutors.DIRECT_EXECUTOR_SERVICE);
        var client = new NoOpNodeClient(threadPool);
        var action = new PutPipelineTransportAction(
            threadPool,
            mock(TransportService.class),
            mock(ActionFilters.class),
            null,
            mock(IngestService.class),
            client
        );

        var pipelineId = randomAlphaOfLength(5);
        var value = randomAlphaOfLength(5);
        var pipelineString = "{\"processors\": [{\"set\" : {\"field\": \"_field\", \"value\": \"" + value + "\"}}]}";
        var existingPipeline = new PipelineConfiguration(pipelineId, new BytesArray(pipelineString), XContentType.JSON);
        var clusterState = ClusterState.builder(new ClusterName("test"))
            .metadata(Metadata.builder().putCustom(
                    IngestMetadata.TYPE,
                    new IngestMetadata(Map.of(pipelineId, existingPipeline))
                ).build()
            ).build();

        CountDownLatch latch = new CountDownLatch(1);
        var listener = new ActionListener<AcknowledgedResponse>() {
            final AtomicLong successCount = new AtomicLong(0);
            final AtomicLong failureCount = new AtomicLong(0);

            @Override
            public void onResponse(AcknowledgedResponse acknowledgedResponse) {
                successCount.incrementAndGet();
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                failureCount.incrementAndGet();
                latch.countDown();
            }

            public long getSuccessCount() {
                return successCount.get();
            }

            public long getFailureCount() {
                return failureCount.get();
            }
        };

        var request = new PutPipelineRequest(pipelineId, new BytesArray(pipelineString), XContentType.JSON);
        action.masterOperation(null, request, clusterState, listener);
        latch.await();

        assertThat(client.getExecutionCount(), equalTo(0L));
        assertThat(listener.getSuccessCount(), equalTo(1L));
        assertThat(listener.getFailureCount(), equalTo(0L));
    }
}
