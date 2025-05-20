/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.TransportCreateIndexAction;
import org.elasticsearch.action.admin.indices.shrink.ResizeAction;
import org.elasticsearch.action.admin.indices.shrink.ResizeRequest;
import org.elasticsearch.action.admin.indices.shrink.ResizeType;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesIndexResponse;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class LookupIndexModeIT extends ESIntegTestCase {

    @Override
    protected int numberOfShards() {
        return 1;
    }

    public void testBasic() {
        internalCluster().ensureAtLeastNumDataNodes(1);
        Settings.Builder lookupSettings = Settings.builder().put("index.mode", "lookup");
        if (randomBoolean()) {
            lookupSettings.put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1);
        }
        CreateIndexRequest createRequest = new CreateIndexRequest("hosts");
        createRequest.settings(lookupSettings);
        createRequest.simpleMapping("ip", "type=ip", "os", "type=keyword");
        assertAcked(client().admin().indices().execute(TransportCreateIndexAction.TYPE, createRequest));
        Settings settings = client().admin()
            .indices()
            .prepareGetSettings(TEST_REQUEST_TIMEOUT, "hosts")
            .get()
            .getIndexToSettings()
            .get("hosts");
        assertThat(settings.get("index.mode"), equalTo("lookup"));
        assertNull(settings.get("index.auto_expand_replicas"));
        Map<String, String> allHosts = Map.of(
            "192.168.1.2",
            "Windows",
            "192.168.1.3",
            "MacOS",
            "192.168.1.4",
            "Linux",
            "192.168.1.5",
            "Android",
            "192.168.1.6",
            "iOS",
            "192.168.1.7",
            "Windows",
            "192.168.1.8",
            "MacOS",
            "192.168.1.9",
            "Linux",
            "192.168.1.10",
            "Linux",
            "192.168.1.11",
            "Windows"
        );
        for (Map.Entry<String, String> e : allHosts.entrySet()) {
            client().prepareIndex("hosts").setSource("ip", e.getKey(), "os", e.getValue()).get();
        }
        refresh("hosts");
        assertAcked(client().admin().indices().prepareCreate("events").setSettings(Settings.builder().put("index.mode", "logsdb")).get());
        int numDocs = between(1, 10);
        for (int i = 0; i < numDocs; i++) {
            String ip = randomFrom(allHosts.keySet());
            String message = randomFrom("login", "logout", "shutdown", "restart");
            client().prepareIndex("events").setSource("@timestamp", "2024-01-01", "ip", ip, "message", message).get();
        }
        refresh("events");
        // _search
        {
            SearchResponse resp = prepareSearch("events", "hosts").setQuery(new MatchQueryBuilder("_index_mode", "lookup"))
                .setSize(10000)
                .get();
            for (SearchHit hit : resp.getHits()) {
                assertThat(hit.getIndex(), equalTo("hosts"));
            }
            assertHitCount(resp, allHosts.size());
            resp.decRef();
        }
        // field_caps
        {
            FieldCapabilitiesRequest request = new FieldCapabilitiesRequest();
            request.indices("events", "hosts");
            request.fields("*");
            request.setMergeResults(false);
            request.indexFilter(new MatchQueryBuilder("_index_mode", "lookup"));
            var resp = client().fieldCaps(request).actionGet();
            assertThat(resp.getIndexResponses(), hasSize(1));
            FieldCapabilitiesIndexResponse indexResponse = resp.getIndexResponses().getFirst();
            assertThat(indexResponse.getIndexMode(), equalTo(IndexMode.LOOKUP));
            assertThat(indexResponse.getIndexName(), equalTo("hosts"));
        }
    }

    public void testRejectMoreThanOneShard() {
        int numberOfShards = between(2, 5);
        IllegalArgumentException error = expectThrows(IllegalArgumentException.class, () -> {
            client().admin()
                .indices()
                .prepareCreate("hosts")
                .setSettings(Settings.builder().put("index.mode", "lookup").put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numberOfShards))
                .setMapping("ip", "type=ip", "os", "type=keyword")
                .get();
        });
        assertThat(
            error.getMessage(),
            equalTo("index with [lookup] mode must have [index.number_of_shards] set to 1 or unset; provided " + numberOfShards)
        );
    }

    public void testResizeLookupIndex() {
        Settings.Builder createSettings = Settings.builder().put("index.mode", "lookup");
        if (randomBoolean()) {
            createSettings.put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1);
        }
        CreateIndexRequest createIndexRequest = new CreateIndexRequest("lookup-1").settings(createSettings);
        assertAcked(client().admin().indices().execute(TransportCreateIndexAction.TYPE, createIndexRequest));
        client().admin().indices().prepareAddBlock(IndexMetadata.APIBlock.WRITE, "lookup-1").get();

        ResizeRequest clone = new ResizeRequest("lookup-2", "lookup-1");
        clone.setResizeType(ResizeType.CLONE);
        assertAcked(client().admin().indices().execute(ResizeAction.INSTANCE, clone).actionGet());
        Settings settings = client().admin()
            .indices()
            .prepareGetSettings(TEST_REQUEST_TIMEOUT, "lookup-2")
            .get()
            .getIndexToSettings()
            .get("lookup-2");
        assertThat(settings.get("index.mode"), equalTo("lookup"));
        assertThat(settings.get("index.number_of_shards"), equalTo("1"));

        ResizeRequest split = new ResizeRequest("lookup-3", "lookup-1");
        split.setResizeType(ResizeType.SPLIT);
        split.getTargetIndexRequest().settings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 3));
        IllegalArgumentException error = expectThrows(
            IllegalArgumentException.class,
            () -> client().admin().indices().execute(ResizeAction.INSTANCE, split).actionGet()
        );
        assertThat(
            error.getMessage(),
            equalTo("index with [lookup] mode must have [index.number_of_shards] set to 1 or unset; provided 3")
        );
    }

    public void testResizeRegularIndexToLookup() {
        String dataNode = internalCluster().startDataOnlyNode();
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate("regular-1")
                .setSettings(
                    Settings.builder()
                        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 2)
                        .put("index.routing.allocation.require._name", dataNode)
                )
                .setMapping("ip", "type=ip", "os", "type=keyword")
                .get()
        );
        client().admin().indices().prepareAddBlock(IndexMetadata.APIBlock.WRITE, "regular-1").get();
        client().admin()
            .indices()
            .prepareUpdateSettings("regular-1")
            .setSettings(Settings.builder().put("index.number_of_replicas", 0))
            .get();

        ResizeRequest clone = new ResizeRequest("lookup-3", "regular-1");
        clone.setResizeType(ResizeType.CLONE);
        clone.getTargetIndexRequest().settings(Settings.builder().put("index.mode", "lookup"));
        IllegalArgumentException error = expectThrows(
            IllegalArgumentException.class,
            () -> client().admin().indices().execute(ResizeAction.INSTANCE, clone).actionGet()
        );
        assertThat(
            error.getMessage(),
            equalTo("index with [lookup] mode must have [index.number_of_shards] set to 1 or unset; provided 2")
        );

        ResizeRequest shrink = new ResizeRequest("lookup-4", "regular-1");
        shrink.setResizeType(ResizeType.SHRINK);
        shrink.getTargetIndexRequest()
            .settings(Settings.builder().put("index.mode", "lookup").put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1));

        error = expectThrows(
            IllegalArgumentException.class,
            () -> client().admin().indices().execute(ResizeAction.INSTANCE, shrink).actionGet()
        );
        assertThat(error.getMessage(), equalTo("can't change setting [index.mode] during resize"));
    }

    public void testDoNotOverrideAutoExpandReplicas() {
        internalCluster().ensureAtLeastNumDataNodes(1);
        Settings.Builder createSettings = Settings.builder().put("index.mode", "lookup");
        if (randomBoolean()) {
            createSettings.put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1);
        }
        createSettings.put("index.auto_expand_replicas", "3-5");
        CreateIndexRequest createRequest = new CreateIndexRequest("hosts");
        createRequest.settings(createSettings);
        createRequest.simpleMapping("ip", "type=ip", "os", "type=keyword");
        assertAcked(client().admin().indices().execute(TransportCreateIndexAction.TYPE, createRequest));
        Settings settings = client().admin()
            .indices()
            .prepareGetSettings(TEST_REQUEST_TIMEOUT, "hosts")
            .get()
            .getIndexToSettings()
            .get("hosts");
        assertThat(settings.get("index.mode"), equalTo("lookup"));
        assertThat(settings.get("index.auto_expand_replicas"), equalTo("3-5"));
    }
}
