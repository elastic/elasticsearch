/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ccr;

import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.xpack.CcrSingleNodeTestCase;
import org.elasticsearch.xpack.core.ccr.action.PutFollowAction;

import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;

public class DocValuesUpdateCcrIT extends CcrSingleNodeTestCase {

    private static final String MAPPING = """
        {
          "properties": {
            "status": { "type": "keyword", "index": false, "doc_values": { "updatable": true } },
            "count":  { "type": "long",    "index": false, "doc_values": { "updatable": true } }
          }
        }
        """;

    public void testFollowerReplicatesDocValuesUpdates() throws Exception {
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate("leader")
                .setSettings(
                    Settings.builder()
                        .put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName())
                        .put("index.disable_sequence_numbers", false)
                        .put("index.number_of_shards", 1)
                        .put("index.number_of_replicas", 0)
                )
                .setMapping(MAPPING)
        );
        ensureGreen("leader");

        int docs = 25;
        for (int i = 0; i < docs; i++) {
            prepareIndex("leader").setId(Integer.toString(i)).setSource("status", "new", "count", i).get();
        }

        // Update the updatable fields in place on the leader before the follower starts, so the follower must reconstruct the
        // doc-values-update operations from the leader's Lucene history (this is what the born-soft-deleted history document exists for).
        BulkRequest bulk = new BulkRequest().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (int i = 0; i < docs; i++) {
            bulk.add(new UpdateRequest("leader", Integer.toString(i)).doc(Map.of("status", "active", "count", i + 1000)));
        }
        assertFalse(client().bulk(bulk).actionGet().hasFailures());

        PutFollowAction.Request followRequest = getPutFollowRequest("leader", "follower");
        client().execute(PutFollowAction.INSTANCE, followRequest).get();

        // The follower must end up with the updated values, proving the doc-values updates replicated through CCR.
        assertBusy(() -> {
            assertHitCount(client().prepareSearch("follower").setQuery(QueryBuilders.termQuery("status", "active")).setSize(0), docs);
            assertHitCount(client().prepareSearch("follower").setQuery(QueryBuilders.rangeQuery("count").gte(1000)).setSize(0), docs);
        });

        // And a live update after following starts must also make it across.
        BulkRequest second = new BulkRequest().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        second.add(new UpdateRequest("leader", "0").doc(Map.of("status", "archived")));
        assertFalse(client().bulk(second).actionGet().hasFailures());
        assertBusy(
            () -> assertHitCount(client().prepareSearch("follower").setQuery(QueryBuilders.termQuery("status", "archived")).setSize(0), 1)
        );
        ensureEmptyWriteBuffers();
    }
}
