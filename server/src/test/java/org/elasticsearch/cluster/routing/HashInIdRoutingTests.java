/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing;

import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.equalTo;

public class HashInIdRoutingTests extends ESSingleNodeTestCase {

    public void testBulkGeneratedIdsAreAddressableById() {
        createIndex(
            "test",
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 3)
                .put(IndexMetadata.INDEX_ROUTING_HASH_IN_ID.getKey(), true)
                .build()
        );
        int docs = 300;
        BulkRequestBuilder bulk = client().prepareBulk();
        for (int i = 0; i < docs; i++) {
            bulk.add(prepareIndex("test").setSource("field", i));
        }
        BulkResponse response = bulk.get();
        assertNoFailures(response);
        List<String> ids = Arrays.stream(response.getItems()).map(BulkItemResponse::getId).toList();

        for (String id : ids) {
            assertTrue(id, client().prepareGet("test", id).get().isExists());
        }
        client().prepareUpdate("test", ids.get(0)).setDoc("field", -1).get();
        client().prepareDelete("test", ids.get(1)).get();

        indicesAdmin().prepareRefresh("test").get();
        long count = indicesAdmin().prepareStats("test").setDocs(true).get().getTotal().getDocs().getCount();
        assertThat(count, equalTo((long) docs - 1));
    }
}
