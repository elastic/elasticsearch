/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.extractor.features;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xcontent.XContentParserConfiguration.EMPTY;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;

public class ClusterFeaturesMetadataExtractorTests extends ESTestCase {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    public void testExtractHistoricalMetadata() throws IOException {
        ClusterFeaturesMetadataExtractor extractor = new ClusterFeaturesMetadataExtractor(this.getClass().getClassLoader());
        Set<String> featureNamesSet = new HashSet<>();
        extractor.extractClusterFeaturesMetadata(featureNamesSet::addAll);
        assertThat(featureNamesSet, not(empty()));
        assertThat(featureNamesSet, hasItem("test_features_enabled"));

        List<String> featureNamesList = generateAndReadFeatureNames(extractor);
        assertThat(featureNamesList, containsInAnyOrder(featureNamesSet.toArray()));
    }

    /**
     * The metadata file is an input of every REST/BWC test task that uses it. It must therefore be byte-for-byte reproducible:
     * collecting the names in a {@code HashSet} makes the order of hash-colliding names depend on insertion order, which
     * comes from {@code Set.of(...)} iteration and is randomized per JVM. The names must therefore be written sorted.
     * Otherwise concurrent builds push differently ordered files under the same build cache key, which changes the cache
     * keys of all downstream test tasks.
     */
    public void testMetadataFileIsSorted() throws IOException {
        ClusterFeaturesMetadataExtractor extractor = new ClusterFeaturesMetadataExtractor(this.getClass().getClassLoader());
        List<String> featureNamesList = generateAndReadFeatureNames(extractor);
        assertThat(featureNamesList.size(), greaterThan(1));
        assertThat(featureNamesList, equalTo(featureNamesList.stream().sorted().toList()));
    }

    private List<String> generateAndReadFeatureNames(ClusterFeaturesMetadataExtractor extractor) throws IOException {
        Path outputFile = temporaryFolder.newFile().toPath();
        extractor.generateMetadataFile(outputFile);
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(EMPTY, Files.newInputStream(outputFile))) {
            Map<String, Object> parsedMap = parser.map();
            assertThat(parsedMap, hasKey("feature_names"));

            @SuppressWarnings("unchecked")
            List<String> featureNames = (List<String>) parsedMap.get("feature_names");
            return featureNames;
        }
    }
}
