/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.transport.action.get;

import org.elasticsearch.action.admin.indices.alias.get.GetAliasesResponse;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.xpack.watcher.condition.AlwaysCondition;
import org.elasticsearch.xpack.watcher.support.xcontent.XContentSource;
import org.elasticsearch.xpack.watcher.test.AbstractWatcherIntegrationTestCase;
import org.elasticsearch.xpack.watcher.transport.actions.get.GetWatchRequest;
import org.elasticsearch.xpack.watcher.transport.actions.get.GetWatchResponse;
import org.elasticsearch.xpack.watcher.transport.actions.put.PutWatchResponse;
import org.elasticsearch.xpack.watcher.watch.Watch;

import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.watcher.actions.ActionBuilders.loggingAction;
import static org.elasticsearch.xpack.watcher.client.WatchSourceBuilders.watchBuilder;
import static org.elasticsearch.xpack.watcher.input.InputBuilders.simpleInput;
import static org.elasticsearch.xpack.watcher.trigger.TriggerBuilders.schedule;
import static org.elasticsearch.xpack.watcher.trigger.schedule.Schedules.interval;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class GetWatchTests extends AbstractWatcherIntegrationTestCase {
    public void testGet() throws Exception {
        PutWatchResponse putResponse = watcherClient().preparePutWatch("_name").setSource(watchBuilder()
                .trigger(schedule(interval("5m")))
                .input(simpleInput())
                .condition(AlwaysCondition.INSTANCE)
                .addAction("_action1", loggingAction("{{ctx.watch_id}}")))
                .get();

        assertThat(putResponse, notNullValue());
        assertThat(putResponse.isCreated(), is(true));

        GetWatchResponse getResponse = watcherClient().getWatch(new GetWatchRequest("_name")).get();
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

    public void testGetNotFoundOnNonExistingIndex() throws Exception {
        // ensure index/alias is deleted, test infra might have created it automatically
        try {
            client().admin().indices().prepareDelete(Watch.INDEX).get();
        } catch (IndexNotFoundException e) {}
        Exception e = expectThrows(Exception.class, () -> watcherClient().getWatch(new GetWatchRequest("_name")).get());
        assertThat(e.getMessage(), containsString("no such index"));
    }

    public void testGetNotFound() throws Exception {
        GetAliasesResponse aliasesResponse = client().admin().indices().prepareGetAliases(Watch.INDEX).get();
        if (aliasesResponse.getAliases().isEmpty()) {
            assertAcked(client().admin().indices().prepareCreate(Watch.INDEX));
        }
        GetWatchResponse getResponse = watcherClient().getWatch(new GetWatchRequest("_name")).get();
        assertThat(getResponse, notNullValue());
        assertThat(getResponse.getId(), is("_name"));
        assertThat(getResponse.isFound(), is(false));
        assertThat(getResponse.getStatus(), nullValue());
        assertThat(getResponse.getSource(), nullValue());
        XContentSource source = getResponse.getSource();
        assertThat(source, nullValue());
    }
}
