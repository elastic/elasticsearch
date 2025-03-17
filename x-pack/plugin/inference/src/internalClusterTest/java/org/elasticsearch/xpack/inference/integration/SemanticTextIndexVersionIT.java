/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.integration;

import org.elasticsearch.Version;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.test.index.IndexVersionUtils;
import org.junit.Before;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class SemanticTextIndexVersionIT extends ESIntegTestCase {
    private static final IndexVersion SEMANTIC_TEXT_INTRODUCED_VERSION = IndexVersions.SEMANTIC_TEXT_FIELD_TYPE;

    private Set<IndexVersion> availableVersions;
    private static final int MIN_NUMBER_OF_TESTS_TO_RUN = 10;

    @Before
    public void setupVersions() {
        availableVersions = IndexVersionUtils.allReleasedVersions().stream()
            .filter((version -> version.onOrAfter(SEMANTIC_TEXT_INTRODUCED_VERSION)))
            .collect(Collectors.toSet());

        logger.info("Available versions for testing: {}", availableVersions);
    }

    @Override
    protected boolean forbidPrivateIndexSettings() {
        return false;
    }

//    /**
//     * Creates an index with a random version from the filtered versions list.
//     * @param indexName The name of the index to create
//     * @return The selected version
//     */
//    protected Version createRandomVersionIndex(String indexName) throws IOException {
//        Version indexVersion = randomFrom(availableVersions);
//        logger.info("Creating index [{}] with version [{}]", indexName, indexVersion);
//
//        createIndex(indexName, getIndexSettingsWithVersion(indexVersion));
//        return indexVersion;
//    }

    /**
     * Generate settings for an index with a specific version.
     */
    private Settings getIndexSettingsWithVersion(IndexVersion version) {
        return Settings.builder()
            .put(indexSettings())
            .put("index.version.created", version)
            .build();
    }

    /**
     * Creates a subset of indices with different versions for testing.
     *
     * @return Map of created indices with their versions
     */
    protected Map<String, IndexVersion> createRandomVersionIndices() throws IOException {
        int versionsCount = Math.min(MIN_NUMBER_OF_TESTS_TO_RUN, availableVersions.size());
        List<IndexVersion> selectedVersions = randomSubsetOf(versionsCount, availableVersions);
        Map<String, IndexVersion> result = new HashMap<>();

        for (int i = 0; i < selectedVersions.size(); i++) {
            String indexName = "test_semantic" + "_" + i;
            IndexVersion version = selectedVersions.get(i);
            createIndex(indexName, getIndexSettingsWithVersion(version));
            result.put(indexName, version);
        }

        return result;
    }

    public void test() throws Exception {
        Map<String, IndexVersion> indices = createRandomVersionIndices();
        for (String indexName : indices.keySet()) {
            IndexVersion version = indices.get(indexName);
            logger.info("Testing index [{}] with version [{}]", indexName, version);
            assertTrue("Index " + indexName + " should exist", indexExists(indexName));
            assertEquals("Index version should match",
                version.id(),
                client().admin().indices().prepareGetSettings(TimeValue.THIRTY_SECONDS, indexName)
                    .get().getIndexToSettings().get(indexName)
                    .getAsVersionId("index.version.created", IndexVersion::fromId).id());
        }
    }

}

//[8.15.3, 8.17.3, 8.15.2, 8.16.6, 8.17.2, 9.0.0, 8.15.1, 8.16.5, 8.17.1, 8.15.0, 8.16.4, 8.17.0, 8.19.0, 8.16.3, 8.16.2, 9.1.0, 8.15.5, 8.16.1, 8.15.4, 8.16.0, 8.17.4, 8.18.0]
// Available versions for testing: [8512000, 9005000, 9013000, 8520000, 8509000, 8517000, 9000000, 9008000, 8525000, 8514000, 9007000, 9015000, 8522000, 8511000, 8519000, 9002000, 9010000, 8527000, 8508000, 8516000, 9001000, 9009000, 8524000, 8513000, 9004000, 9012000, 8521000, 8510000, 8518000, 9003000, 9011000, 8526000, 8507000, 8515000, 9006000, 9014000, 8523000]
