/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.bulk;

import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Map;

public class PreResolvedUpdatesIT extends ESIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(PreResolvedUpdates.PRE_RESOLVE_BULK_UPDATES.getKey(), true)
            .build();
    }

    public void testBulkUpdates() {
        createIndex("test");
        int docs = randomIntBetween(10, 50);
        BulkRequestBuilder indexingBulk = client().prepareBulk();
        for (int i = 0; i < docs; i++) {
            indexingBulk.add(client().prepareIndex("test").setId(Integer.toString(i)).setSource("foo", "initial"));
        }
        assertFalse(indexingBulk.get().hasFailures());
        if (randomBoolean()) {
            refresh("test");
        }

        BulkRequestBuilder updatingBulk = client().prepareBulk();
        for (int i = 0; i < docs; i++) {
            updatingBulk.add(client().prepareUpdate("test", Integer.toString(i)).setDoc("foo", "updated"));
        }
        BulkResponse response = updatingBulk.get();
        assertFalse(response.buildFailureMessage(), response.hasFailures());
        for (BulkItemResponse item : response.getItems()) {
            assertEquals(DocWriteResponse.Result.UPDATED, item.getResponse().getResult());
        }

        String id = Integer.toString(randomIntBetween(0, docs - 1));
        assertEquals("updated", client().prepareGet("test", id).get().getSourceAsMap().get("foo"));
    }

    public void testRepeatedUpdatesOfSameIdInOneBulk() {
        createIndex("test");
        client().prepareIndex("test").setId("1").setSource("f0", "v0").get();
        if (randomBoolean()) {
            refresh("test");
        }

        int updates = randomIntBetween(2, 10);
        BulkRequestBuilder bulk = client().prepareBulk();
        for (int i = 1; i <= updates; i++) {
            bulk.add(client().prepareUpdate("test", "1").setDoc("f" + i, "v" + i));
        }
        BulkResponse response = bulk.get();
        assertFalse(response.buildFailureMessage(), response.hasFailures());

        Map<String, Object> source = client().prepareGet("test", "1").get().getSourceAsMap();
        for (int i = 0; i <= updates; i++) {
            assertEquals("v" + i, source.get("f" + i));
        }
    }
}
