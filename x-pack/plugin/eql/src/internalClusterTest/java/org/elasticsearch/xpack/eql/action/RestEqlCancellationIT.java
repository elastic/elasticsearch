/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.action;

import org.apache.lucene.util.Constants;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Cancellable;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.junit.annotations.TestIssueLogging;
import org.elasticsearch.transport.Netty4Plugin;
import org.elasticsearch.transport.nio.NioTransportPlugin;
import org.junit.BeforeClass;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

@TestIssueLogging(value = "org.elasticsearch.xpack.eql.action:TRACE",
    issueUrl = "https://github.com/elastic/elasticsearch/issues/58270")
public class RestEqlCancellationIT extends AbstractEqlBlockingIntegTestCase {

    private static String nodeHttpTypeKey;

    @SuppressWarnings("unchecked")
    @BeforeClass
    public static void setUpTransport() {
        nodeHttpTypeKey = getHttpTypeKey(randomFrom(Netty4Plugin.class, NioTransportPlugin.class));
    }

    @Override
    protected boolean addMockHttpTransport() {
        return false; // enable http
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(NetworkModule.HTTP_TYPE_KEY, nodeHttpTypeKey).build();
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
        plugins.add(Netty4Plugin.class);
        plugins.add(NioTransportPlugin.class);
        return plugins;
    }

    public void testRestCancellation() throws Exception {
        assumeFalse("https://github.com/elastic/elasticsearch/issues/58270", Constants.WINDOWS);
        assertAcked(client().admin().indices().prepareCreate("test")
            .setMapping("val", "type=integer", "event_type", "type=keyword", "@timestamp", "type=date")
            .get());
        createIndex("idx_unmapped");

        int numDocs = randomIntBetween(6, 20);

        List<IndexRequestBuilder> builders = new ArrayList<>();

        for (int i = 0; i < numDocs; i++) {
            int fieldValue = randomIntBetween(0, 10);
            builders.add(client().prepareIndex("test").setSource(
                jsonBuilder().startObject()
                    .field("val", fieldValue).field("event_type", "my_event").field("@timestamp", "2020-04-09T12:35:48Z")
                    .endObject()));
        }

        indexRandom(true, builders);

        // We are cancelling during both mapping and searching but we cancel during mapping so we should never reach the second block
        List<SearchBlockPlugin> plugins = initBlockFactory(true, true);
        org.elasticsearch.client.eql.EqlSearchRequest eqlSearchRequest =
            new org.elasticsearch.client.eql.EqlSearchRequest("test", "my_event where val=1").eventCategoryField("event_type");
        String id = randomAlphaOfLength(10);

        Request request = new Request("GET", "/test/_eql/search");
        request.setJsonEntity(Strings.toString(eqlSearchRequest));
        request.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader(Task.X_OPAQUE_ID, id));
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
        assertThat(getTaskInfoWithXOpaqueId(id, EqlSearchAction.NAME), notNullValue());
        cancellable.cancel();
        logger.trace("Request is cancelled");
        disableFieldCapBlocks(plugins);
        // The task should be cancelled before ever reaching search blocks
        assertBusy(() -> {
            assertThat(getTaskInfoWithXOpaqueId(id, EqlSearchAction.NAME), nullValue());
        });
        // Make sure it didn't reach search blocks
        assertThat(getNumberOfContexts(plugins), equalTo(0));
        disableSearchBlocks(plugins);

        latch.await();
        assertThat(error.get(), instanceOf(CancellationException.class));
    }

    @Override
    protected boolean ignoreExternalCluster() {
        return true;
    }
}
