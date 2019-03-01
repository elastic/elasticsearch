/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.watcher;

import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.protocol.xpack.watcher.PutWatchResponse;
import org.elasticsearch.xpack.core.watcher.watch.Watch;
import org.elasticsearch.xpack.watcher.condition.InternalAlwaysCondition;
import org.elasticsearch.xpack.watcher.test.AbstractWatcherIntegrationTestCase;

import java.util.Locale;

import static org.elasticsearch.xpack.watcher.actions.ActionBuilders.indexAction;
import static org.elasticsearch.xpack.watcher.client.WatchSourceBuilders.watchBuilder;
import static org.elasticsearch.xpack.watcher.input.InputBuilders.noneInput;
import static org.elasticsearch.xpack.watcher.trigger.TriggerBuilders.schedule;
import static org.elasticsearch.xpack.watcher.trigger.schedule.Schedules.interval;
import static org.hamcrest.Matchers.greaterThan;

public class WatcherConcreteIndexTests extends AbstractWatcherIntegrationTestCase {

    @Override
    protected boolean timeWarped() {
        return false;
    }

    public void testCanUseAnyConcreteIndexName() throws Exception {
        String newWatcherIndexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        String watchResultsIndex = randomAlphaOfLength(11).toLowerCase(Locale.ROOT);
        createIndex(watchResultsIndex);

        GetIndexResponse index = client().admin().indices().prepareGetIndex().setIndices(".watches").get();
        MappingMetaData mapping = index.getMappings().get(index.getIndices()[0]).get("doc");

        Settings settings = index.getSettings().get(index.getIndices()[0]);
        Settings.Builder newSettings = Settings.builder().put(settings);
        newSettings.put("index.number_of_shards", "1");
        newSettings.put("index.number_of_replicas", "0");
        newSettings.remove("index.provided_name");
        newSettings.remove("index.uuid");
        newSettings.remove("index.creation_date");
        newSettings.remove("index.version.created");

        CreateIndexResponse createIndexResponse = client().admin().indices().prepareCreate(newWatcherIndexName)
            .addMapping("doc", mapping.sourceAsMap())
            .setSettings(newSettings)
            .setWaitForActiveShards(1)
            .get();
        assertTrue(createIndexResponse.isAcknowledged());

        stopWatcher();
        client().admin().indices().prepareDelete(Watch.INDEX).get();
        client().admin().indices().prepareAliases().addAlias(newWatcherIndexName, Watch.INDEX).get();
        startWatcher();

        PutWatchResponse putWatchResponse = watcherClient().preparePutWatch("mywatch").setSource(watchBuilder()
            .trigger(schedule(interval("1s")))
            .input(noneInput())
            .condition(InternalAlwaysCondition.INSTANCE)
            .addAction("indexer", indexAction(watchResultsIndex, "_doc")))
            .get();

        assertTrue(putWatchResponse.isCreated());

        assertBusy(() -> {
            SearchResponse searchResult = client().prepareSearch(watchResultsIndex).setTrackTotalHits(true).get();
            assertThat((int) searchResult.getHits().getTotalHits().value, greaterThan(0));
        });

    }
}
