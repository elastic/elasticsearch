/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.integration;

import org.elasticsearch.Version;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.xpack.core.transform.transforms.DestAlias;
import org.elasticsearch.xpack.core.transform.transforms.SettingsConfig;
import org.junit.Before;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class TransformDestIndexIT extends TransformRestTestCase {

    private static boolean indicesCreated = false;

    // preserve indices in order to reuse source indices in several test cases
    @Override
    protected boolean preserveIndicesUponCompletion() {
        return true;
    }

    @Before
    public void createIndexes() throws IOException {

        // it's not possible to run it as @BeforeClass as clients aren't initialized then, so we need this little hack
        if (indicesCreated) {
            return;
        }

        createReviewsIndex();
        indicesCreated = true;
    }

    public void testTransformDestIndexMetadata() throws Exception {
        long testStarted = System.currentTimeMillis();
        String transformId = "test_meta";
        createPivotReviewsTransform(transformId, "pivot_reviews", null);
        startAndWaitForTransform(transformId, "pivot_reviews");

        Response mappingResponse = client().performRequest(new Request("GET", "pivot_reviews/_mapping"));

        Map<?, ?> mappingAsMap = entityAsMap(mappingResponse);
        assertEquals(
            Version.CURRENT.toString(),
            XContentMapValues.extractValue("pivot_reviews.mappings._meta._transform.version.created", mappingAsMap)
        );
        assertTrue(
            (Long) XContentMapValues.extractValue("pivot_reviews.mappings._meta._transform.creation_date_in_millis", mappingAsMap) < System
                .currentTimeMillis()
        );
        assertTrue(
            (Long) XContentMapValues.extractValue(
                "pivot_reviews.mappings._meta._transform.creation_date_in_millis",
                mappingAsMap
            ) > testStarted
        );
        assertEquals(transformId, XContentMapValues.extractValue("pivot_reviews.mappings._meta._transform.transform", mappingAsMap));
        assertEquals("transform", XContentMapValues.extractValue("pivot_reviews.mappings._meta.created_by", mappingAsMap));

        Response aliasesResponse = client().performRequest(new Request("GET", "pivot_reviews/_alias"));
        Map<?, ?> aliasesAsMap = entityAsMap(aliasesResponse);
        assertEquals(Map.of(), XContentMapValues.extractValue("pivot_reviews.aliases", aliasesAsMap));
    }

    public void testTransformDestIndexAliases() throws Exception {
        String transformId = "test_aliases";
        String destIndex1 = transformId + ".1";
        String destIndex2 = transformId + ".2";
        String destAliasAll = transformId + ".all";
        String destAliasLatest = transformId + ".latest";
        List<DestAlias> destAliases = List.of(new DestAlias(destAliasAll, false), new DestAlias(destAliasLatest, true));

        // Create the transform
        createPivotReviewsTransform(transformId, destIndex1, null, null, destAliases, null, null, null, REVIEWS_INDEX_NAME);
        startAndWaitForTransform(transformId, destIndex1);

        // Verify that both aliases are configured on the dest index
        assertAliases(destIndex1, destAliasAll, destAliasLatest);

        // Verify that the search results are the same, regardless whether we use index or alias
        assertHitsAreTheSame(destIndex1, destAliasAll, destAliasLatest);

        // Stop the transform so that the transform task is properly removed before calling DELETE.
        // TODO: Remove this step once the underlying issue (race condition is fixed).
        stopTransform(transformId, false);

        // Delete the transform
        deleteTransform(transformId);
        assertAliases(destIndex1, destAliasAll, destAliasLatest);

        // Create the transform again (this time with a different destination index)
        createPivotReviewsTransform(transformId, destIndex2, null, null, destAliases, null, null, null, REVIEWS_INDEX_NAME);
        startAndWaitForTransform(transformId, destIndex2);

        // Verify that destAliasLatest no longer points at destIndex1 but it now points at destIndex2
        assertAliases(destIndex1, destAliasAll);
        assertAliases(destIndex2, destAliasAll, destAliasLatest);
    }

    public void testTransformDestIndexCreatedDuringUpdate() throws Exception {
        String transformId = "test_dest_index_on_update";
        String destIndex = transformId + "-dest";

        assertFalse(indexExists(destIndex));

        // Create and start the unattended transform
        createPivotReviewsTransform(
            transformId,
            destIndex,
            null,
            null,
            null,
            new SettingsConfig.Builder().setUnattended(true).build(),
            null,
            null,
            REVIEWS_INDEX_NAME
        );
        startTransform(transformId);

        // Update the unattended transform. This will trigger destination index creation.
        // The update has to change something in the config (here, max_page_search_size). Otherwise it would have been optimized away.
        // Note that at this point the destination index could have already been created by the indexing process of the running transform
        // but the update code should cope with this situation.
        updateTransform(transformId, """
            { "settings": { "max_page_search_size": 123 } }""");

        // Verify that the destination index now exists
        assertTrue(indexExists(destIndex));
    }

    private static void assertAliases(String index, String... aliases) throws IOException {
        Map<String, Map<?, ?>> expectedAliases = Arrays.stream(aliases).collect(Collectors.toMap(a -> a, a -> Map.of()));
        Response aliasesResponse = client().performRequest(new Request("GET", index + "/_alias"));
        assertEquals(expectedAliases, XContentMapValues.extractValue(index + ".aliases", entityAsMap(aliasesResponse)));
    }

    private static void assertHitsAreTheSame(String index, String... aliases) throws IOException {
        Response indexSearchResponse = client().performRequest(new Request("GET", index + "/_search"));
        Object indexSearchHits = XContentMapValues.extractValue("hits", entityAsMap(indexSearchResponse));
        for (String alias : aliases) {
            Response aliasSearchResponse = client().performRequest(new Request("GET", alias + "/_search"));
            Object aliasSearchHits = XContentMapValues.extractValue("hits", entityAsMap(aliasSearchResponse));
            assertEquals("index = " + index + ", alias = " + alias, indexSearchHits, aliasSearchHits);
        }
    }
}
