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
import java.util.Map;

import static org.elasticsearch.xcontent.XContentParserConfiguration.EMPTY;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.not;

public class HistoricalFeaturesMetadataExtractorTests extends ESTestCase {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    public void testExtractHistoricalMetadata() throws IOException {
        HistoricalFeaturesMetadataExtractor extractor = new HistoricalFeaturesMetadataExtractor(this.getClass().getClassLoader());
        Map<NodeFeature, Version> nodeFeatureVersionMap = extractor.extractHistoricalFeatureMetadata();
        assertThat(nodeFeatureVersionMap, not(anEmptyMap()));

        Path outputFile = temporaryFolder.newFile().toPath();
        extractor.generateMetadataFile(outputFile);
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(EMPTY, Files.newInputStream(outputFile))) {
            Map<String, String> parsedMap = parser.mapStrings();
            for (Map.Entry<NodeFeature, Version> entry : nodeFeatureVersionMap.entrySet()) {
                assertThat(parsedMap, hasEntry(entry.getKey().id(), entry.getValue().toString()));
            }
        }
    }
}
