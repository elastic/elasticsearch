/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.update;

import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.snapshots.AbstractSnapshotIntegTestCase;
import org.elasticsearch.snapshots.SnapshotState;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.equalTo;

public class DocValuesUpdateSnapshotIT extends AbstractSnapshotIntegTestCase {

    private static final String MAPPING = """
        {
          "properties": {
            "status": { "type": "keyword", "index": false, "doc_values": { "updatable": true } },
            "count":  { "type": "long",    "index": false, "doc_values": { "updatable": true } }
          }
        }
        """;

    public void testDocValuesUpdatesSurviveSnapshotAndRestore() throws Exception {
        createRepository("repo", "fs");
        assertAcked(
            prepareCreate("idx").setSettings(
                Settings.builder()
                    .put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName())
                    .put("index.disable_sequence_numbers", false)
                    .put("index.number_of_shards", 1)
                    .put("index.number_of_replicas", 0)
            ).setMapping(MAPPING)
        );
        ensureGreen("idx");

        int docs = 30;
        for (int i = 0; i < docs; i++) {
            prepareIndex("idx").setId(Integer.toString(i)).setSource("status", "new", "count", i).get();
        }
        // Update in place so the snapshot captures the new doc-values generation files rather than the original ones.
        BulkRequest bulk = new BulkRequest().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (int i = 0; i < docs; i++) {
            bulk.add(new UpdateRequest("idx", Integer.toString(i)).doc(Map.of("status", "active", "count", i + 1000)));
        }
        assertFalse(client().bulk(bulk).actionGet().hasFailures());

        assertThat(createSnapshot("repo", "snap", List.of("idx")).state(), equalTo(SnapshotState.SUCCESS));
        assertAcked(indicesAdmin().prepareDelete("idx"));

        RestoreSnapshotResponse restore = clusterAdmin().prepareRestoreSnapshot(TEST_REQUEST_TIMEOUT, "repo", "snap")
            .setWaitForCompletion(true)
            .get();
        assertThat(restore.getRestoreInfo().successfulShards(), equalTo(1));
        ensureGreen("idx");

        // The restored index must carry the updated values.
        assertResponse(
            prepareSearch("idx").setSize(0).setQuery(QueryBuilders.termQuery("status", "active")),
            response -> assertHitCount(response, docs)
        );
        assertResponse(
            prepareSearch("idx").setSize(0).setQuery(QueryBuilders.rangeQuery("count").gte(1000)),
            response -> assertHitCount(response, docs)
        );
        assertThat(client().prepareGet("idx", "7").get().getSourceAsMap().get("status"), equalTo("active"));
    }
}
