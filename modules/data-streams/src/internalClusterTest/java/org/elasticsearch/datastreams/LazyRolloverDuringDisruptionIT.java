/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.rollover.RolloverRequestBuilder;
import org.elasticsearch.action.admin.indices.template.put.TransportPutComposableIndexTemplateAction;
import org.elasticsearch.action.datastreams.CreateDataStreamAction;
import org.elasticsearch.action.datastreams.GetDataStreamAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentType;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, numClientNodes = 0)
public class LazyRolloverDuringDisruptionIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(DataStreamsPlugin.class);
    }

    public void testRolloverIsExecutedOnce() throws ExecutionException, InterruptedException {
        internalCluster().startMasterOnlyNode();
        internalCluster().startDataOnlyNodes(3);
        ensureStableCluster(4);

        String dataStreamName = "my-data-stream";
        createDataStream(dataStreamName);

        // Mark it to lazy rollover
        safeGet(new RolloverRequestBuilder(client()).setRolloverTarget(dataStreamName).lazy(true).execute());

        // Verify that the data stream is marked for rollover and that it has currently one index
        DataStream dataStream = getDataStream(dataStreamName);
        assertThat(dataStream.rolloverOnWrite(), equalTo(true));
        assertThat(dataStream.getDataComponent().getIndices().size(), equalTo(1));

        // Introduce a disruption to the master node that should delay the rollover execution
        final var barrier = new CyclicBarrier(2);
        internalCluster().getCurrentMasterNodeInstance(ClusterService.class)
            .submitUnbatchedStateUpdateTask("block", new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    safeAwait(barrier);
                    safeAwait(barrier);
                    return currentState;
                }

                @Override
                public void onFailure(Exception e) {
                    fail(e);
                }
            });
        safeAwait(barrier);

        // Start indexing operations
        int docs = randomIntBetween(5, 10);
        CountDownLatch countDownLatch = new CountDownLatch(docs);
        for (int i = 0; i < docs; i++) {
            var indexRequest = new IndexRequest(dataStreamName).opType(DocWriteRequest.OpType.CREATE);
            final String doc = "{ \"@timestamp\": \"2099-05-06T16:21:15.000Z\", \"message\": \"something cool happened\" }";
            indexRequest.source(doc, XContentType.JSON);
            client().index(indexRequest, new ActionListener<>() {
                @Override
                public void onResponse(DocWriteResponse docWriteResponse) {
                    countDownLatch.countDown();
                }

                @Override
                public void onFailure(Exception e) {
                    fail("Indexing request should have succeeded eventually, failed with " + e.getMessage());
                }
            });
        }

        // End the disruption so that all pending tasks will complete
        safeAwait(barrier);

        // Wait for all the indexing requests to be processed successfully
        safeAwait(countDownLatch);

        // Verify that the rollover has happened once
        dataStream = getDataStream(dataStreamName);
        assertThat(dataStream.rolloverOnWrite(), equalTo(false));
        assertThat(dataStream.getDataComponent().getIndices().size(), equalTo(2));
    }

    private DataStream getDataStream(String dataStreamName) {
        return safeGet(
            client().execute(
                GetDataStreamAction.INSTANCE,
                new GetDataStreamAction.Request(TEST_REQUEST_TIMEOUT, new String[] { dataStreamName })
            )
        ).getDataStreams().get(0).getDataStream();
    }

    private void createDataStream(String dataStreamName) throws InterruptedException, ExecutionException {
        final TransportPutComposableIndexTemplateAction.Request putComposableTemplateRequest =
            new TransportPutComposableIndexTemplateAction.Request("my-template");
        putComposableTemplateRequest.indexTemplate(
            ComposableIndexTemplate.builder()
                .indexPatterns(List.of(dataStreamName))
                .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate(false, false))
                .build()
        );
        final AcknowledgedResponse putComposableTemplateResponse = safeGet(
            client().execute(TransportPutComposableIndexTemplateAction.TYPE, putComposableTemplateRequest)
        );
        assertThat(putComposableTemplateResponse.isAcknowledged(), is(true));

        final CreateDataStreamAction.Request createDataStreamRequest = new CreateDataStreamAction.Request(
            TEST_REQUEST_TIMEOUT,
            TEST_REQUEST_TIMEOUT,
            dataStreamName
        );
        final AcknowledgedResponse createDataStreamResponse = safeGet(
            client().execute(CreateDataStreamAction.INSTANCE, createDataStreamRequest)
        );
        assertThat(createDataStreamResponse.isAcknowledged(), is(true));
    }
}
