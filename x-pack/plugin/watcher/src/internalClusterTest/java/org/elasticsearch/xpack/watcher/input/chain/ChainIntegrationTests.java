/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.input.chain;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.transport.netty4.Netty4Plugin;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.watcher.transport.actions.put.PutWatchRequestBuilder;
import org.elasticsearch.xpack.watcher.common.http.HttpRequestTemplate;
import org.elasticsearch.xpack.watcher.input.http.HttpInput;
import org.elasticsearch.xpack.watcher.test.AbstractWatcherIntegrationTestCase;

import java.net.InetSocketAddress;
import java.util.Collection;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.watcher.actions.ActionBuilders.indexAction;
import static org.elasticsearch.xpack.watcher.client.WatchSourceBuilders.watchBuilder;
import static org.elasticsearch.xpack.watcher.input.InputBuilders.chainInput;
import static org.elasticsearch.xpack.watcher.input.InputBuilders.httpInput;
import static org.elasticsearch.xpack.watcher.input.InputBuilders.simpleInput;
import static org.elasticsearch.xpack.watcher.trigger.TriggerBuilders.schedule;
import static org.elasticsearch.xpack.watcher.trigger.schedule.IntervalSchedule.Interval.Unit.SECONDS;
import static org.elasticsearch.xpack.watcher.trigger.schedule.Schedules.interval;
import static org.hamcrest.Matchers.containsString;

public class ChainIntegrationTests extends AbstractWatcherIntegrationTestCase {

    @Override
    protected boolean addMockHttpTransport() {
        return false; // enable http
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder().put(super.nodeSettings(nodeOrdinal, otherSettings)).build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), Netty4Plugin.class); // for http
    }

    public void testChainedInputsAreWorking() throws Exception {
        String index = "the-most-awesome-index-ever";
        createIndex(index);
        prepareIndex(index).setId("id").setSource("{}", XContentType.JSON).setRefreshPolicy(IMMEDIATE).get();

        InetSocketAddress address = internalCluster().httpAddresses()[0];
        HttpInput.Builder httpInputBuilder = httpInput(
            HttpRequestTemplate.builder(address.getHostString(), address.getPort())
                .path("/" + index + "/_search")
                .body(Strings.toString(jsonBuilder().startObject().field("size", 1).endObject()))
        );

        ChainInput.Builder chainedInputBuilder = chainInput().add("first", simpleInput("url", "/" + index + "/_search"))
            .add("second", httpInputBuilder);

        new PutWatchRequestBuilder(client(), "_name").setSource(
            watchBuilder().trigger(schedule(interval(5, SECONDS)))
                .input(chainedInputBuilder)
                .addAction("indexAction", indexAction("my-index"))
        ).get();

        timeWarp().trigger("_name");
        refresh();

        assertWatchWithMinimumPerformedActionsCount("_name", 1, false);
    }

    public void assertWatchExecuted() {
        try {
            refresh();
            assertResponse(prepareSearch("my-index"), searchResponse -> {
                assertHitCount(searchResponse, 1);
                assertThat(searchResponse.getHits().getAt(0).getSourceAsString(), containsString("the-most-awesome-index-ever"));
            });
        } catch (IndexNotFoundException e) {
            fail("Index not found: [" + e.getIndex() + "]");
        }
    }
}
