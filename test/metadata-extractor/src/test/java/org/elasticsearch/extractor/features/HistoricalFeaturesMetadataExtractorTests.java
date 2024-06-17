/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.extractor.features;

import org.elasticsearch.Version;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xcontent.XContentParserConfiguration.EMPTY;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;

public class HistoricalFeaturesMetadataExtractorTests extends ESTestCase {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    public void testExtractHistoricalMetadata() throws IOException {
        HistoricalFeaturesMetadataExtractor extractor = new HistoricalFeaturesMetadataExtractor(this.getClass().getClassLoader());
        Map<NodeFeature, Version> nodeFeatureVersionMap = new HashMap<>();
        Set<String> featureNamesSet = new HashSet<>();
        extractor.extractHistoricalFeatureMetadata((historical, names) -> {
            nodeFeatureVersionMap.putAll(historical);
            featureNamesSet.addAll(names);
        });
        assertThat(nodeFeatureVersionMap, not(anEmptyMap()));
        assertThat(featureNamesSet, not(empty()));

        Path outputFile = temporaryFolder.newFile().toPath();
        extractor.generateMetadataFile(outputFile);
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(EMPTY, Files.newInputStream(outputFile))) {
            Map<String, Object> parsedMap = parser.map();
            assertThat(parsedMap, hasKey("historical_features"));
            assertThat(parsedMap, hasKey("feature_names"));
            @SuppressWarnings("unchecked")
            Map<String, Object> historicalFeaturesMap = (Map<String, Object>) (parsedMap.get("historical_features"));
            nodeFeatureVersionMap.forEach((key, value) -> assertThat(historicalFeaturesMap, hasEntry(key.id(), value.toString())));

            @SuppressWarnings("unchecked")
            Collection<String> featureNamesList = (Collection<String>) (parsedMap.get("feature_names"));
            assertThat(featureNamesList, containsInAnyOrder(featureNamesSet.toArray()));
        }
    }
}
