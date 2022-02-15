/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.action;

import com.fasterxml.jackson.core.JsonGenerator;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Cancellable;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseListener;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.transport.netty4.Netty4Plugin;
import org.elasticsearch.transport.nio.NioTransportPlugin;
import org.elasticsearch.xpack.sql.proto.CoreProtocol;
import org.elasticsearch.xpack.sql.proto.Mode;
import org.elasticsearch.xpack.sql.proto.Payloads;
import org.elasticsearch.xpack.sql.proto.RequestInfo;
import org.elasticsearch.xpack.sql.proto.SqlQueryRequest;
import org.elasticsearch.xpack.sql.proto.content.ContentFactory;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class RestSqlCancellationIT extends AbstractSqlBlockingIntegTestCase {

    private static String nodeHttpTypeKey;

    @BeforeClass
    public static void setUpTransport() {
        nodeHttpTypeKey = getHttpTypeKey(randomFrom(Netty4Plugin.class, NioTransportPlugin.class));
    }

    @Override
    protected boolean addMockHttpTransport() {
        return false; // enable http
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(NetworkModule.HTTP_TYPE_KEY, nodeHttpTypeKey)
            .build();
    }

    private static String getHttpTypeKey(Class<? extends Plugin> clazz) {
        if (clazz.equals(NioTransportPlugin.class)) {
            return NioTransportPlugin.NIO_HTTP_TRANSPORT_NAME;
        } else {
            assert clazz.equals(Netty4Plugin.class);
            return Netty4Plugin.NETTY_HTTP_TRANSPORT_NAME;
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(getTestTransportPlugin());
        plugins.add(NioTransportPlugin.class);
        return plugins;
    }

    @TestLogging(value = "org.elasticsearch.xpack.sql:TRACE", reason = "debug")
    public void testRestCancellation() throws Exception {
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate("test")
                .setMapping("val", "type=integer", "event_type", "type=keyword", "@timestamp", "type=date")
                .get()
        );
        createIndex("idx_unmapped");

        int numDocs = randomIntBetween(6, 20);

        List<IndexRequestBuilder> builders = new ArrayList<>();

        for (int i = 0; i < numDocs; i++) {
            int fieldValue = randomIntBetween(0, 10);
            builders.add(
                client().prepareIndex("test")
                    .setSource(
                        jsonBuilder().startObject()
                            .field("val", fieldValue)
                            .field("event_type", "my_event")
                            .field("@timestamp", "2020-04-09T12:35:48Z")
                            .endObject()
                    )
            );
        }

        indexRandom(true, builders);

        // We are cancelling during both mapping and searching but we cancel during mapping so we should never reach the second block
        List<SearchBlockPlugin> plugins = initBlockFactory(true, true);
        String id = randomAlphaOfLength(10);

        Request request = new Request("POST", Protocol.SQL_QUERY_REST_ENDPOINT);
        request.setJsonEntity(queryAsJson("SELECT event_type FROM test WHERE val=1"));
        request.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader(Task.X_OPAQUE_ID_HTTP_HEADER, id));
        logger.trace("Preparing search");

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Exception> error = new AtomicReference<>();
        Cancellable cancellable = getRestClient().performRequestAsync(request, new ResponseListener() {
            @Override
            public void onSuccess(Response response) {
                latch.countDown();
            }

            @Override
            public void onFailure(Exception exception) {
                error.set(exception);
                latch.countDown();
            }
        });

        logger.trace("Waiting for block to be established");
        awaitForBlockedFieldCaps(plugins);
        logger.trace("Block is established");
        TaskInfo blockedTaskInfo = getTaskInfoWithXOpaqueId(id, SqlQueryAction.NAME);
        assertThat(blockedTaskInfo, notNullValue());
        cancellable.cancel();
        logger.trace("Request is cancelled");

        assertBusy(() -> {
            for (TransportService transportService : internalCluster().getInstances(TransportService.class)) {
                if (transportService.getLocalNode().getId().equals(blockedTaskInfo.taskId().getNodeId())) {
                    Task task = transportService.getTaskManager().getTask(blockedTaskInfo.id());
                    if (task != null) {
                        assertThat(task, instanceOf(SqlQueryTask.class));
                        SqlQueryTask sqlSearchTask = (SqlQueryTask) task;
                        logger.trace("Waiting for cancellation to be propagated: {} ", sqlSearchTask.isCancelled());
                        assertThat(sqlSearchTask.isCancelled(), equalTo(true));
                    }
                    return;
                }
            }
            fail("Task not found");
        });

        logger.trace("Disabling field cap blocks");
        disableFieldCapBlocks(plugins);
        // The task should be cancelled before ever reaching search blocks
        assertBusy(() -> { assertThat(getTaskInfoWithXOpaqueId(id, SqlQueryAction.NAME), nullValue()); });
        // Make sure it didn't reach search blocks
        assertThat(getNumberOfContexts(plugins), equalTo(0));
        disableSearchBlocks(plugins);

        latch.await();
        assertThat(error.get(), instanceOf(CancellationException.class));
    }

    private static String queryAsJson(String query) throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        try (JsonGenerator generator = ContentFactory.generator(ContentFactory.ContentType.JSON, out)) {
            Payloads.generate(
                generator,
                new SqlQueryRequest(
                    query,
                    Collections.emptyList(),
                    CoreProtocol.TIME_ZONE,
                    null,
                    CoreProtocol.FETCH_SIZE,
                    CoreProtocol.REQUEST_TIMEOUT,
                    CoreProtocol.PAGE_TIMEOUT,
                    null,
                    null,
                    new RequestInfo(Mode.PLAIN),
                    CoreProtocol.FIELD_MULTI_VALUE_LENIENCY,
                    CoreProtocol.INDEX_INCLUDE_FROZEN,
                    CoreProtocol.BINARY_COMMUNICATION
                )
            );
        }
        return out.bytes().utf8ToString();
    }

    @Override
    protected boolean ignoreExternalCluster() {
        return true;
    }
}
