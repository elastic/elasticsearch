/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.lucene;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.WarningsHandler;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.cluster.util.Version;
import org.elasticsearch.test.rest.ObjectPath;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.equalTo;

public class FullClusterRestartSystemIndexCompatibilityIT extends FullClusterRestartIndexCompatibilityTestCase {

    static {
        clusterConfig = config -> config.setting("xpack.license.self_generated.type", "trial");
    }

    public FullClusterRestartSystemIndexCompatibilityIT(Version version) {
        super(version);
    }

    // we need a place to store async_search ids across cluster restarts
    private static Map<String, String> async_search_ids = new HashMap<>(3);

    /**
     * 1. creates an index on N-2 and performs async_search on it that is kept in system index
     * 2. After update to N-1 (latest) perform a system index migration step, also write block the index
     * 3. on N, check that async search results are still retrievable and we can write to the system index
     */
    public void testAsyncSearchIndexMigration() throws Exception {
        final String index = suffix("index");
        final String asyncSearchIndex = ".async-search";
        final int numDocs = 2431;

        final Request asyncSearchRequest = new Request("POST", "/" + index + "/_async_search?size=100&keep_on_completion=true");

        if (isFullyUpgradedTo(VERSION_MINUS_2)) {
            createIndex(
                client(),
                index,
                Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, randomInt(2))
                    .build()
            );
            indexDocs(index, numDocs);
            ensureGreen(index);

            assertThat(indexVersion(index), equalTo(VERSION_MINUS_2));
            String asyncId = searchAsyncAndStoreId(asyncSearchRequest, "n-2_id");
            ensureGreen(asyncSearchIndex);

            assertAsyncSearchHitCount(asyncId, numDocs);
            assertBusy(() -> assertDocCountNoWarnings(client(), asyncSearchIndex, 1));
            assertThat(indexVersion(asyncSearchIndex, true), equalTo(VERSION_MINUS_2));
            return;
        }

        if (isFullyUpgradedTo(VERSION_MINUS_1)) {
            // check .async-search index is readable
            assertThat(indexVersion(asyncSearchIndex, true), equalTo(VERSION_MINUS_2));
            assertAsyncSearchHitCount(async_search_ids.get("n-2_id"), numDocs);

            // migrate system indices
            Request migrateRequest = new Request("POST", "/_migration/system_features");
            assertThat(
                ObjectPath.createFromResponse(client().performRequest(migrateRequest)).evaluate("features.0.feature_name"),
                equalTo("async_search")
            );
            assertBusy(() -> {
                Request checkMigrateProgress = new Request("GET", "/_migration/system_features");
                Response resp = null;
                try {
                    assertFalse(
                        ObjectPath.createFromResponse(client().performRequest(checkMigrateProgress))
                            .evaluate("migration_status")
                            .equals("IN_PROGRESS")
                    );
                } catch (IOException e) {
                    throw new AssertionError("System feature migration failed", e);
                }
            }, 30, TimeUnit.SECONDS);

            // check search results from n-2 search are still readable
            assertAsyncSearchHitCount(async_search_ids.get("n-2_id"), numDocs);

            // perform new async search and check its readable
            String asyncId = searchAsyncAndStoreId(asyncSearchRequest, "n-1_id");
            assertAsyncSearchHitCount(asyncId, numDocs);
            assertBusy(() -> assertDocCountNoWarnings(client(), asyncSearchIndex, 2));

            // in order to move to current version we need write block for n-2 index
            addIndexBlock(index, IndexMetadata.APIBlock.WRITE);
        }

        if (isFullyUpgradedTo(VERSION_CURRENT)) {
            assertThat(indexVersion(index, true), equalTo(VERSION_MINUS_2));
            assertAsyncSearchHitCount(async_search_ids.get("n-2_id"), numDocs);
            assertAsyncSearchHitCount(async_search_ids.get("n-1_id"), numDocs);

            // check system index is still writeable
            String asyncId = searchAsyncAndStoreId(asyncSearchRequest, "n_id");
            assertAsyncSearchHitCount(asyncId, numDocs);
            assertBusy(() -> assertDocCountNoWarnings(client(), asyncSearchIndex, 3));
        }

    }

    private static String searchAsyncAndStoreId(Request asyncSearchRequest, String asyncIdName) throws IOException {
        ObjectPath resp = ObjectPath.createFromResponse(client().performRequest(asyncSearchRequest));
        String asyncId = resp.evaluate("id");
        assertNotNull(asyncId);
        async_search_ids.put(asyncIdName, asyncId);
        return asyncId;
    }

    private static void assertAsyncSearchHitCount(String asyncId, int numDocs) throws IOException {
        var asyncGet = new Request("GET", "/_async_search/" + asyncId);
        ObjectPath resp = ObjectPath.createFromResponse(client().performRequest(asyncGet));
        assertEquals(Integer.valueOf(numDocs), resp.evaluate("response.hits.total.value"));
    }

    /**
     * Assert that the index in question has the given number of documents present
     */
    private static void assertDocCountNoWarnings(RestClient client, String indexName, long docCount) throws IOException {
        Request countReq = new Request("GET", "/" + indexName + "/_count");
        RequestOptions.Builder options = countReq.getOptions().toBuilder();
        options.setWarningsHandler(WarningsHandler.PERMISSIVE);
        countReq.setOptions(options);
        ObjectPath resp = ObjectPath.createFromResponse(client.performRequest(countReq));
        assertEquals(
            "expected " + docCount + " documents but it was a different number",
            docCount,
            Long.parseLong(resp.evaluate("count").toString())
        );
    }
}
