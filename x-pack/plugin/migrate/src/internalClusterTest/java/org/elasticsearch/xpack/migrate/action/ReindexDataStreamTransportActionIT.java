/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.migrate.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.rollover.RolloverRequestBuilder;
import org.elasticsearch.action.admin.indices.template.put.TransportPutComposableIndexTemplateAction;
import org.elasticsearch.action.datastreams.CreateDataStreamAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.datastreams.DataStreamsPlugin;
import org.elasticsearch.exception.ResourceNotFoundException;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.migrate.MigratePlugin;
import org.elasticsearch.xpack.migrate.action.ReindexDataStreamAction.ReindexDataStreamRequest;
import org.elasticsearch.xpack.migrate.task.ReindexDataStreamEnrichedStatus;
import org.elasticsearch.xpack.migrate.task.ReindexDataStreamTask;

import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class ReindexDataStreamTransportActionIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(DataStreamsPlugin.class, MigratePlugin.class);
    }

    public void testNonExistentDataStream() {
        String nonExistentDataStreamName = randomAlphaOfLength(50);
        ReindexDataStreamRequest reindexDataStreamRequest = new ReindexDataStreamRequest(
            ReindexDataStreamAction.Mode.UPGRADE,
            nonExistentDataStreamName
        );
        assertThrows(
            ResourceNotFoundException.class,
            () -> client().execute(new ActionType<AcknowledgedResponse>(ReindexDataStreamAction.NAME), reindexDataStreamRequest).actionGet()
        );
    }

    public void testAlreadyUpToDateDataStream() throws Exception {
        String dataStreamName = randomAlphaOfLength(50).toLowerCase(Locale.ROOT);
        ReindexDataStreamRequest reindexDataStreamRequest = new ReindexDataStreamRequest(
            ReindexDataStreamAction.Mode.UPGRADE,
            dataStreamName
        );
        final int backingIndexCount = createDataStream(dataStreamName);
        client().execute(new ActionType<AcknowledgedResponse>(ReindexDataStreamAction.NAME), reindexDataStreamRequest).actionGet();
        String persistentTaskId = "reindex-data-stream-" + dataStreamName;
        AtomicReference<ReindexDataStreamTask> runningTask = new AtomicReference<>();
        for (TransportService transportService : internalCluster().getInstances(TransportService.class)) {
            TaskManager taskManager = transportService.getTaskManager();
            Optional<Map.Entry<Long, CancellableTask>> optionalTask = taskManager.getCancellableTasks()
                .entrySet()
                .stream()
                .filter(entry -> entry.getValue().getType().equals("persistent"))
                .filter(
                    entry -> entry.getValue() instanceof ReindexDataStreamTask
                        && persistentTaskId.equals((((ReindexDataStreamTask) entry.getValue()).getPersistentTaskId()))
                )
                .findAny();
            optionalTask.ifPresent(
                longCancellableTaskEntry -> runningTask.compareAndSet(null, (ReindexDataStreamTask) longCancellableTaskEntry.getValue())
            );
        }
        ReindexDataStreamTask task = runningTask.get();
        assertBusy(() -> {
            assertNotNull(task);
            assertThat(task.getStatus().complete(), equalTo(true));
            assertNull(task.getStatus().exception());
            assertThat(task.getStatus().pending(), equalTo(0));
            assertThat(task.getStatus().inProgress(), equalTo(Set.of()));
            assertThat(task.getStatus().errors().size(), equalTo(0));
        });

        assertBusy(() -> {
            GetMigrationReindexStatusAction.Response statusResponse = client().execute(
                new ActionType<GetMigrationReindexStatusAction.Response>(GetMigrationReindexStatusAction.NAME),
                new GetMigrationReindexStatusAction.Request(dataStreamName)
            ).actionGet();
            ReindexDataStreamEnrichedStatus status = statusResponse.getEnrichedStatus();
            assertThat(status.complete(), equalTo(true));
            assertThat(status.errors(), equalTo(List.of()));
            assertThat(status.exception(), equalTo(null));
            assertThat(status.pending(), equalTo(0));
            assertThat(status.inProgress().size(), equalTo(0));
            assertThat(status.totalIndices(), equalTo(backingIndexCount));
            assertThat(status.totalIndicesToBeUpgraded(), equalTo(0));
        });
        AcknowledgedResponse cancelResponse = client().execute(
            CancelReindexDataStreamAction.INSTANCE,
            new CancelReindexDataStreamAction.Request(dataStreamName)
        ).actionGet();
        assertNotNull(cancelResponse);
        assertThrows(
            ResourceNotFoundException.class,
            () -> client().execute(CancelReindexDataStreamAction.INSTANCE, new CancelReindexDataStreamAction.Request(dataStreamName))
                .actionGet()
        );
        assertThrows(
            ResourceNotFoundException.class,
            () -> client().execute(
                new ActionType<GetMigrationReindexStatusAction.Response>(GetMigrationReindexStatusAction.NAME),
                new GetMigrationReindexStatusAction.Request(dataStreamName)
            ).actionGet()
        );
    }

    private int createDataStream(String dataStreamName) {
        final TransportPutComposableIndexTemplateAction.Request putComposableTemplateRequest =
            new TransportPutComposableIndexTemplateAction.Request("my-template");
        putComposableTemplateRequest.indexTemplate(
            ComposableIndexTemplate.builder()
                .indexPatterns(List.of(dataStreamName))
                .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate(false, false))
                .template(Template.builder().build())
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
        int backingIndices = 1;
        for (int i = 0; i < randomIntBetween(2, 5); i++) {
            indexDocs(dataStreamName);
            safeGet(new RolloverRequestBuilder(client()).setRolloverTarget(dataStreamName).lazy(false).execute());
            backingIndices++;
        }
        return backingIndices;
    }

    private void indexDocs(String dataStreamName) {
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
        safeAwait(countDownLatch);
    }

}
