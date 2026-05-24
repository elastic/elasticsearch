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
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.hasEntry;

public class PatternTextBasicLicenseIT extends DataStreamLicenseChangeTestCase {
    @Before
    public void checkClusterFeature() {
        assumeTrue("[patterned_text] must be available", clusterHasFeature("mapper.patterned_text"));
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> getFieldMapping(Map<String, Object> mapping, String fieldName) {
        return (Map<String, Object>) ((Map<String, Object>) mapping.get("properties")).get(fieldName);
    }

    public void testStandardIndex() throws IOException {
        final String dataStreamName = "test-foo";

        final String mappingStr = """
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

        final String doc = """
            {"index": {}}
            {"@timestamp": "2025-01-01T00:00:00.000Z", "pattern_field": "foo"}
            """;

        assertOK(putTemplate(client(), "logs@custom", mappingStr.replace("%name%", dataStreamName)));
        assertOK(bulkIndex(client(), dataStreamName, () -> doc));

        String backingIndex0 = getDataStreamBackingIndex(client(), dataStreamName, 0);
        {
            assertEquals("true", getSetting(client(), backingIndex0, "index.mapping.pattern_text.disable_templating"));
            Map<String, Object> mapping = getMapping(client(), backingIndex0);
            Map<String, Object> patternFieldMapping = getFieldMapping(mapping, "pattern_field");
            assertThat(patternFieldMapping, hasEntry("disable_templating", true));
        }

        assertOK(rolloverDataStream(client(), dataStreamName));

        String backingIndex1 = getDataStreamBackingIndex(client(), dataStreamName, 1);
        {
            assertEquals("true", getSetting(client(), backingIndex1, "index.mapping.pattern_text.disable_templating"));
            Map<String, Object> mapping = getMapping(client(), backingIndex1);
            Map<String, Object> patternFieldMapping = getFieldMapping(mapping, "pattern_field");
            assertThat(patternFieldMapping, hasEntry("disable_templating", true));
        }
    }

    public void testLogsdbIndex() throws IOException {
        final String dataStreamName = "test-bar";

        final String mappingStr = """
            {
              "index_patterns": ["%name%"],
              "priority": 500,
              "data_stream": {},
              "template": {
                "settings": {
                  "index.mode": "logsdb",
                  "index.logsdb.default_sort_on_message_template": true
                },
                "mappings": {
                  "properties": {
                    "@timestamp": {
                      "type": "date"
                    },
                    "message": {
                      "type": "pattern_text"
                    },
                    "host.name": {
                      "type": "keyword"
                    }
                  }
                }
              }
            }
            """;

        final String doc = """
            {"index": {}}
            {"@timestamp": "2025-01-01T00:00:00.000Z", "message": "foo 123", "host.name": "bar"}
            """;

        assertOK(putTemplate(client(), "logs@custom", mappingStr.replace("%name%", dataStreamName)));
        assertOK(bulkIndex(client(), dataStreamName, () -> doc));

        String backingIndex0 = getDataStreamBackingIndex(client(), dataStreamName, 0);
        {
            assertEquals("true", getSetting(client(), backingIndex0, "index.mapping.pattern_text.disable_templating"));
            assertEquals(
                List.of("host.name", "message.template_id", "@timestamp"),
                getSetting(client(), backingIndex0, "index.sort.field")
            );
            Map<String, Object> mapping = getMapping(client(), backingIndex0);
            Map<String, Object> patternFieldMapping = getFieldMapping(mapping, "message");
            assertThat(patternFieldMapping, hasEntry("disable_templating", true));
        }

        assertOK(rolloverDataStream(client(), dataStreamName));

        String backingIndex1 = getDataStreamBackingIndex(client(), dataStreamName, 1);
        {
            assertEquals("true", getSetting(client(), backingIndex1, "index.mapping.pattern_text.disable_templating"));
            assertEquals(
                List.of("host.name", "message.template_id", "@timestamp"),
                getSetting(client(), backingIndex1, "index.sort.field")
            );
            Map<String, Object> mapping = getMapping(client(), backingIndex1);
            Map<String, Object> patternFieldMapping = getFieldMapping(mapping, "message");
            assertThat(patternFieldMapping, hasEntry("disable_templating", true));
        }
    }
}
