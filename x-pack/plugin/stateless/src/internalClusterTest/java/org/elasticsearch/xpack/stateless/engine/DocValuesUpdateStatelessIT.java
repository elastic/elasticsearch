/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.engine;

import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.xpack.stateless.AbstractStatelessPluginIntegTestCase;

import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class DocValuesUpdateStatelessIT extends AbstractStatelessPluginIntegTestCase {

    private static final String MAPPING = """
        {
          "properties": {
            "status": { "type": "keyword", "index": false, "doc_values": { "updatable": true } },
            "count":  { "type": "long",    "index": false, "doc_values": { "updatable": true } },
            "name":   { "type": "keyword" }
          }
        }
        """;

    /**
     * Updatable doc-values fields work on a stateless index: an update touching only updatable fields takes the in-place path, and the
     * updated values are visible to search (served from the object store), with the untouched field preserved. Reads go through search
     * rather than get because stateless serves reads from search copies.
     *
     * <p>The in-place path is reached even though the search copies refresh from the object store with a lag: the gate that defers an
     * in-place update ({@code getMinTrackedReplicaLocalCheckpoint}) tracks only promotable copies, and a stateless index has a single
     * promotable {@code INDEX_ONLY} primary with unpromotable {@code SEARCH_ONLY} replicas, so no tracked copy holds the gate back.
     */
    public void testUpdatableDocValuesOnStateless() throws Exception {
        assumeTrue("doc_values updatable feature flag must be enabled", FieldMapper.DOC_VALUES_UPDATABLE_FEATURE_FLAG.isEnabled());
        startMasterOnlyNode();
        startSearchNode();
        startIndexNode();

        assertAcked(
            prepareCreate("idx").setSettings(
                Settings.builder()
                    .put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName())
                    .put("index.disable_sequence_numbers", false)
                    .put("index.number_of_shards", 1)
                    .put("index.number_of_replicas", 1)
            ).setMapping(MAPPING)
        );
        ensureGreen("idx");

        prepareIndex("idx").setId("1")
            .setSource("status", "new", "count", 1, "name", "widget")
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();

        UpdateResponse updated = client().update(
            new UpdateRequest("idx", "1").doc(Map.of("status", "active", "count", 42))
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
        ).actionGet();
        assertThat(updated.getResult(), equalTo(DocWriteResponse.Result.UPDATED));

        // The update must take the in-place path, not a reindex fallback: the in-place path increments this stat, a reindex leaves it
        // at zero. Guards against a regression where tracking unpromotable search copies in the gate would force a reindex on stateless.
        long inPlaceUpdateCount = indicesAdmin().prepareStats("idx")
            .clear()
            .setIndexing(true)
            .get()
            .getTotal()
            .getIndexing()
            .getTotal()
            .getInPlaceDocValuesUpdateCount();
        assertThat("expected an in-place doc-values update on stateless, not a reindex", inPlaceUpdateCount, greaterThan(0L));

        assertResponse(prepareSearch("idx").setQuery(QueryBuilders.matchAllQuery()), response -> {
            Map<String, Object> source = response.getHits().getAt(0).getSourceAsMap();
            assertThat(source.get("status"), equalTo("active"));
            assertThat(((Number) source.get("count")).longValue(), equalTo(42L));
            assertThat(source.get("name"), equalTo("widget"));
        });
    }
}
