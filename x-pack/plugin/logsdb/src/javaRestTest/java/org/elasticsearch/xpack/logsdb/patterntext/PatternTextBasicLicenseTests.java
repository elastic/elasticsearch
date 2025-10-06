/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.patterntext;

import org.elasticsearch.xpack.logsdb.DataStreamLicenseChangeTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.Map;

import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;

public class PatternTextBasicLicenseTests extends DataStreamLicenseChangeTestCase {
    @Before
    public void checkClusterFeature() {
        assumeTrue("[patterned_text] must be available", clusterHasFeature("mapper.patterned_text"));
    }

    private static final String patternTextMapping = """
        {
          "index_patterns": ["%name%"],
          "priority": 500,
          "data_stream": {},
          "template": {
            "mappings": {
              "properties": {
                "pattern_field": {
                  "type": "pattern_text"
                }
              }
            }
          }
        }""";

    @SuppressWarnings("unchecked")
    public void testLicenseUpgrade() throws IOException {
        final String dataStreamName = "test-foo";

        final String doc = """
            {"index": {}}
            {"@timestamp": "2025-01-01T00:00:00.000Z", "pattern_field": "foo"}
            """;

        assertOK(putTemplate(client(), "logs@custom", patternTextMapping.replace("%name%", dataStreamName)));
        assertOK(bulkIndex(client(), dataStreamName, () -> doc));

        String backingIndex0 = getDataStreamBackingIndex(client(), dataStreamName, 0);
        {
            assertEquals("true", getSetting(client(), backingIndex0, "index.mapping.pattern_text.disable_templating"));
            Map<String, Object> mapping = getMapping(client(), backingIndex0);
            Map<String, Object> patternFieldMapping = (Map<String, Object>) ((Map<String, Object>) mapping.get("properties")).get(
                "pattern_field"
            );
            assertThat(patternFieldMapping, hasEntry("disable_templating", true));
        }

        assertOK(rolloverDataStream(client(), dataStreamName));

        String backingIndex1 = getDataStreamBackingIndex(client(), dataStreamName, 1);
        {
            assertEquals("true", getSetting(client(), backingIndex1, "index.mapping.pattern_text.disable_templating"));
            Map<String, Object> mapping = getMapping(client(), backingIndex1);
            Map<String, Object> patternFieldMapping = (Map<String, Object>) ((Map<String, Object>) mapping.get("properties")).get(
                "pattern_field"
            );
            assertThat(patternFieldMapping, hasKey("disable_templating"));
        }
    }
}
