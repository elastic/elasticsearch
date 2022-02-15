/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.action;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.Cancellable;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.transport.netty4.Netty4Plugin;
import org.elasticsearch.transport.nio.NioTransportPlugin;
import org.junit.BeforeClass;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CancellationException;

import static org.elasticsearch.action.support.ActionTestUtils.wrapAsRestResponseListener;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class RestEqlCancellationIT extends AbstractEqlBlockingIntegTestCase {

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
        org.elasticsearch.client.eql.EqlSearchRequest eqlSearchRequest = new org.elasticsearch.client.eql.EqlSearchRequest(
            "test",
            "my_event where val==1"
        ).eventCategoryField("event_type");
        String id = randomAlphaOfLength(10);

        Request request = new Request("GET", "/test/_eql/search");
        request.setJsonEntity(Strings.toString(eqlSearchRequest));
        request.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader(Task.X_OPAQUE_ID_HTTP_HEADER, id));
        logger.trace("Preparing search");

        final PlainActionFuture<Response> future = PlainActionFuture.newFuture();
        Cancellable cancellable = getRestClient().performRequestAsync(request, wrapAsRestResponseListener(future));

        logger.trace("Waiting for block to be established");
        awaitForBlockedFieldCaps(plugins);
        logger.trace("Block is established");
        TaskInfo blockedTaskInfo = getTaskInfoWithXOpaqueId(id, EqlSearchAction.NAME);
        assertThat(blockedTaskInfo, notNullValue());
        cancellable.cancel();
        logger.trace("Request is cancelled");

        assertBusy(() -> {
            for (TransportService transportService : internalCluster().getInstances(TransportService.class)) {
                if (transportService.getLocalNode().getId().equals(blockedTaskInfo.taskId().getNodeId())) {
                    Task task = transportService.getTaskManager().getTask(blockedTaskInfo.id());
                    if (task != null) {
                        assertThat(task, instanceOf(EqlSearchTask.class));
                        EqlSearchTask eqlSearchTask = (EqlSearchTask) task;
                        logger.trace("Waiting for cancellation to be propagated {} ", eqlSearchTask.isCancelled());
                        assertThat(eqlSearchTask.isCancelled(), equalTo(true));
                    }
                    return;
                }
            }
            fail("Task not found");
        });

        logger.trace("Disabling field cap blocks");
        disableFieldCapBlocks(plugins);
        // The task should be cancelled before ever reaching search blocks
        assertBusy(() -> { assertThat(getTaskInfoWithXOpaqueId(id, EqlSearchAction.NAME), nullValue()); });
        // Make sure it didn't reach search blocks
        assertThat(getNumberOfContexts(plugins), equalTo(0));
        disableSearchBlocks(plugins);

        expectThrows(CancellationException.class, future::actionGet);
    }

    @Override
    protected boolean ignoreExternalCluster() {
        return true;
    }
}
