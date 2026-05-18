/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.integration;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.rest.action.admin.indices.RestPutIndexTemplateAction;
import org.elasticsearch.xpack.core.transform.TransformConfigVersion;
import org.elasticsearch.xpack.core.transform.transforms.DestAlias;
import org.elasticsearch.xpack.core.transform.transforms.SettingsConfig;
import org.junit.Before;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.is;

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
            TransformConfigVersion.CURRENT.toString(),
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

    public void testUnattendedTransformDestIndexCreatedDuringUpdate_NoDeferValidation() throws Exception {
        testUnattendedTransformDestIndexCreatedDuringUpdate(false);
    }

    public void testUnattendedTransformDestIndexCreatedDuringUpdate_DeferValidation() throws Exception {
        testUnattendedTransformDestIndexCreatedDuringUpdate(true);
    }

    private void testUnattendedTransformDestIndexCreatedDuringUpdate(boolean deferValidation) throws Exception {
        String transformId = "test_dest_index_on_update" + (deferValidation ? "-defer" : "");
        String destIndex = transformId + "-dest";
        String destAliasAll = transformId + ".all";
        String destAliasLatest = transformId + ".latest";
        List<DestAlias> destAliases = List.of(new DestAlias(destAliasAll, false), new DestAlias(destAliasLatest, true));

        // Create and start the unattended transform
        SettingsConfig settingsConfig = new SettingsConfig.Builder().setUnattended(true).build();
        createPivotReviewsTransform(transformId, destIndex, null, null, destAliases, settingsConfig, null, null, REVIEWS_INDEX_NAME);
        assertFalse(indexExists(destIndex));

        startTransform(transformId);

        // Update the unattended transform. This will trigger destination index creation.
        // The update has to change something in the config (here, max_page_search_size). Otherwise it would have been optimized away.
        // Note that at this point the destination index could have already been created by the indexing process of the running transform
        // but the update code should cope with this situation.
        updateTransform(transformId, """
            { "settings": { "max_page_search_size": 123 } }""", deferValidation);

        // Verify that the destination index now exists
        assertTrue(indexExists(destIndex));
        // Verify that both aliases are configured on the dest index
        assertAliases(destIndex, destAliasAll, destAliasLatest);
    }

    public void testUnattendedTransformDestIndexCreatedDuringUpdate_EmptySourceIndex_NoDeferValidation() throws Exception {
        testUnattendedTransformDestIndexCreatedDuringUpdate_EmptySourceIndex(false);
    }

    public void testUnattendedTransformDestIndexCreatedDuringUpdate_EmptySourceIndex_DeferValidation() throws Exception {
        testUnattendedTransformDestIndexCreatedDuringUpdate_EmptySourceIndex(true);
    }

    private void testUnattendedTransformDestIndexCreatedDuringUpdate_EmptySourceIndex(boolean deferValidation) throws Exception {
        String transformId = "test_dest_index_on_update-empty" + (deferValidation ? "-defer" : "");
        String sourceIndexIndex = transformId + "-src";
        String destIndex = transformId + "-dest";
        String destAliasAll = transformId + ".all";
        String destAliasLatest = transformId + ".latest";
        List<DestAlias> destAliases = List.of(new DestAlias(destAliasAll, false), new DestAlias(destAliasLatest, true));

        // We want to use an empty source index to make sure transform will not write to the destination index
        putReviewsIndex(sourceIndexIndex, "date", false);
        assertFalse(indexExists(destIndex));

        // Create and start the unattended transform
        SettingsConfig settingsConfig = new SettingsConfig.Builder().setUnattended(true).build();
        createPivotReviewsTransform(transformId, destIndex, null, null, destAliases, settingsConfig, null, null, sourceIndexIndex);
        startTransform(transformId);

        // Verify that the destination index creation got skipped
        assertFalse(indexExists(destIndex));

        // Update the unattended transform. This will trigger destination index creation.
        // The update has to change something in the config (here, max_page_search_size). Otherwise it would have been optimized away.
        updateTransform(transformId, """
            { "settings": { "max_page_search_size": 123 } }""", deferValidation);

        // Verify that the destination index now exists
        assertTrue(indexExists(destIndex));
        // Verify that both aliases are configured on the dest index
        assertAliases(destIndex, destAliasAll, destAliasLatest);
    }

    public void testUnattendedTransformDestIndexCreatedByIndexer() throws Exception {
        String transformId = "test_dest_index_in_indexer";
        String destIndex = transformId + "-dest";
        String destAliasAll = transformId + ".all";
        String destAliasLatest = transformId + ".latest";
        List<DestAlias> destAliases = List.of(new DestAlias(destAliasAll, false), new DestAlias(destAliasLatest, true));

        // Create and start the unattended transform
        SettingsConfig settingsConfig = new SettingsConfig.Builder().setUnattended(true).build();
        createPivotReviewsTransform(transformId, destIndex, null, null, destAliases, settingsConfig, null, null, REVIEWS_INDEX_NAME);
        startTransform(transformId);
        waitForTransformCheckpoint(transformId, 1);

        // Verify that the destination index exists
        assertTrue(indexExists(destIndex));
        // Verify that both aliases are configured on the dest index
        assertAliases(destIndex, destAliasAll, destAliasLatest);
    }

    public void testTransformDestIndexMappings_DeduceMappings() throws Exception {
        testTransformDestIndexMappings("test_dest_index_mappings_deduce", true);
    }

    public void testTransformDestIndexMappings_NoDeduceMappings() throws Exception {
        testTransformDestIndexMappings("test_dest_index_mappings_no_deduce", false);
    }

    private void testTransformDestIndexMappings(String transformId, boolean deduceMappings) throws Exception {
        String destIndex = transformId + "-dest";

        {
            String destIndexTemplate = Strings.format("""
                {
                  "index_patterns": [ "%s*" ],
                  "mappings": {
                    "properties": {
                      "timestamp": {
                        "type": "date"
                      },
                      "reviewer": {
                        "type": "keyword"
                      },
                      "avg_rating": {
                        "type": "double"
                      }
                    }
                  }
                }""", destIndex);
            Request createIndexTemplateRequest = new Request("PUT", "_template/test_dest_index_mappings_template");
            createIndexTemplateRequest.setJsonEntity(destIndexTemplate);
            createIndexTemplateRequest.setOptions(expectWarnings(RestPutIndexTemplateAction.DEPRECATION_WARNING));
            Map<String, Object> createIndexTemplateResponse = entityAsMap(client().performRequest(createIndexTemplateRequest));
            assertThat(createIndexTemplateResponse.get("acknowledged"), equalTo(Boolean.TRUE));
        }

        // Verify that the destination index does not exist yet, even though the template already exists
        assertFalse(indexExists(destIndex));

        {
            String config = Strings.format("""
                {
                  "dest": {
                    "index": "%s"
                  },
                  "source": {
                    "index": "%s"
                  },
                  "sync": {
                    "time": {
                      "field": "timestamp",
                      "delay": "15m"
                    }
                  },
                  "frequency": "1s",
                  "pivot": {
                    "group_by": {
                      "timestamp": {
                        "date_histogram": {
                          "field": "timestamp",
                          "fixed_interval": "10s"
                        }
                      },
                      "reviewer": {
                        "terms": {
                          "field": "user_id"
                        }
                      }
                    },
                    "aggregations": {
                      "avg_rating": {
                        "avg": {
                          "field": "stars"
                        }
                      }
                    }
                  },
                  "settings": {
                    "unattended": true,
                    "deduce_mappings": %s
                  }
                }""", destIndex, REVIEWS_INDEX_NAME, deduceMappings);
            createReviewsTransform(transformId, null, null, config);

            startTransform(transformId);
            waitForTransformCheckpoint(transformId, 1);
        }

        // Verify that the destination index now exists and has correct mappings from the template
        assertTrue(indexExists(destIndex));
        assertThat(
            getIndexMappingAsMap(destIndex),
            hasEntry(
                "properties",
                Map.of("avg_rating", Map.of("type", "double"), "reviewer", Map.of("type", "keyword"), "timestamp", Map.of("type", "date"))
            )
        );
        Map<String, Object> searchResult = getAsMap(destIndex + "/_search?q=reviewer:user_0");
        String timestamp = (String) ((List<?>) XContentMapValues.extractValue("hits.hits._source.timestamp", searchResult)).get(0);
        assertThat(timestamp, is(equalTo("2017-01-10T10:10:10.000Z")));
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
