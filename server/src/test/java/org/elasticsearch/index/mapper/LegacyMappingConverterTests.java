/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.common.Strings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.test.StreamsUtils.copyToStringFromClasspath;

public class LegacyMappingConverterTests extends ESTestCase {

    public void testFilebeat5() throws IOException {
        LegacyMappingConverter converter = new LegacyMappingConverter();
        try (XContentParser parser = createParser(JsonXContent.jsonXContent,
            copyToStringFromClasspath("/org/elasticsearch/index/mapper/filebeat5.json"))) {
            Map<String, ?> mapping = parser.mapOrdered();
            Map<String, ?> newMapping = converter.extractNewMapping(mapping, true);
            logger.info("New mapping: {}", Strings.toString(JsonXContent.contentBuilder().prettyPrint().map(newMapping)));
            Map<String, ?> runtimeFieldMapping = converter.extractRuntimeFieldMapping(newMapping);
            logger.info("Runtime fields: {}", Strings.toString(JsonXContent.contentBuilder().prettyPrint().map(runtimeFieldMapping)));
            assertEquals(137, runtimeFieldMapping.keySet().size());
        }
    }

    public void testFilebeat6() throws IOException {
        LegacyMappingConverter converter = new LegacyMappingConverter();
        try (XContentParser parser = createParser(JsonXContent.jsonXContent,
            copyToStringFromClasspath("/org/elasticsearch/index/mapper/filebeat6.json"))) {
            Map<String, ?> mapping = parser.mapOrdered();
            Map<String, ?> newMapping = converter.extractNewMapping(mapping, true);
            logger.info("New mapping: {}", Strings.toString(JsonXContent.contentBuilder().prettyPrint().map(newMapping)));
            Map<String, ?> runtimeFieldMapping = converter.extractRuntimeFieldMapping(newMapping);
            logger.info("Runtime fields: {}", Strings.toString(JsonXContent.contentBuilder().prettyPrint().map(runtimeFieldMapping)));
            assertEquals(1160, runtimeFieldMapping.keySet().size());
        }
    }
}
