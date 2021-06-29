/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.transport.action.get;

import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.protocol.xpack.watcher.PutWatchResponse;
import org.elasticsearch.xpack.core.watcher.support.xcontent.XContentSource;
import org.elasticsearch.xpack.core.watcher.transport.actions.get.GetWatchRequestBuilder;
import org.elasticsearch.xpack.core.watcher.transport.actions.get.GetWatchResponse;
import org.elasticsearch.xpack.core.watcher.transport.actions.put.PutWatchRequestBuilder;
import org.elasticsearch.xpack.core.watcher.watch.Watch;
import org.elasticsearch.xpack.watcher.condition.InternalAlwaysCondition;
import org.elasticsearch.xpack.watcher.test.AbstractWatcherIntegrationTestCase;

import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.watcher.actions.ActionBuilders.loggingAction;
import static org.elasticsearch.xpack.watcher.client.WatchSourceBuilders.watchBuilder;
import static org.elasticsearch.xpack.watcher.input.InputBuilders.simpleInput;
import static org.elasticsearch.xpack.watcher.trigger.TriggerBuilders.schedule;
import static org.elasticsearch.xpack.watcher.trigger.schedule.Schedules.interval;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class GetWatchTests extends AbstractWatcherIntegrationTestCase {

    public void testGet() throws Exception {
        PutWatchResponse putResponse = new PutWatchRequestBuilder(client(), "_name").setSource(watchBuilder()
                .trigger(schedule(interval("5m")))
                .input(simpleInput())
                .condition(InternalAlwaysCondition.INSTANCE)
                .addAction("_action1", loggingAction("{{ctx.watch_id}}")))
                .get();

        assertThat(putResponse, notNullValue());
        assertThat(putResponse.isCreated(), is(true));

        GetWatchResponse getResponse = new GetWatchRequestBuilder(client(), "_name").get();
        assertThat(getResponse, notNullValue());
        assertThat(getResponse.isFound(), is(true));
        assertThat(getResponse.getId(), is("_name"));
        Map<String, Object> source = getResponse.getSource().getAsMap();
        assertThat(source, notNullValue());
        assertThat(source, hasKey("trigger"));
        assertThat(source, hasKey("input"));
        assertThat(source, hasKey("condition"));
        assertThat(source, hasKey("actions"));
        assertThat(source, not(hasKey("status")));
    }

    public void testGetNotFound() throws Exception {
        // does not matter if the watch does not exist or the index does not exist, we expect the same response
        // if the watches index is an alias, remove the alias randomly, otherwise the index
        if (randomBoolean()) {
            try {
                GetIndexResponse indexResponse = client().admin().indices().prepareGetIndex().setIndices(Watch.INDEX).get();
                boolean isWatchIndexAlias = Watch.INDEX.equals(indexResponse.indices()[0]) == false;
                if (isWatchIndexAlias) {
                    assertAcked(client().admin().indices().prepareAliases().removeAlias(indexResponse.indices()[0], Watch.INDEX));
                } else {
                    assertAcked(client().admin().indices().prepareDelete(Watch.INDEX));
                }
            } catch (IndexNotFoundException e) {}
        }

        GetWatchResponse getResponse = new GetWatchRequestBuilder(client(), "_name").get();
        assertThat(getResponse, notNullValue());
        assertThat(getResponse.getId(), is("_name"));
        assertThat(getResponse.isFound(), is(false));
        assertThat(getResponse.getStatus(), nullValue());
        assertThat(getResponse.getSource(), nullValue());
        XContentSource source = getResponse.getSource();
        assertThat(source, nullValue());
    }
}
