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
import static org.hamcrest.Matchers.not;

public class PatternTextLicenseUpgradeIT extends DataStreamLicenseChangeTestCase {
    @Before
    public void checkClusterFeature() {
        assumeTrue("[patterned_text] must be available", clusterHasFeature("mapper.patterned_text"));
    }

    private static final String patternTextMapping = """
        {
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
        final String dataStreamName = "logs-test-pattern-text";

        assertOK(putComponentTemplate(client(), "logs@custom", patternTextMapping));
        assertOK(createDataStream(client(), dataStreamName));

        String backingIndex0 = getDataStreamBackingIndex(client(), dataStreamName, 0);
        {
            assertEquals("true", getSetting(client(), backingIndex0, "index.mapping.pattern_text.disable_templating"));
            Map<String, Object> mapping = getMapping(client(), backingIndex0);
            Map<String, Object> patternFieldMapping = (Map<String, Object>) ((Map<String, Object>) mapping.get("properties")).get(
                "pattern_field"
            );
            assertThat(patternFieldMapping, hasEntry("disable_templating", true));
        }

        startTrial();
        rolloverDataStream(client(), dataStreamName);

        {
            assertEquals("true", getSetting(client(), backingIndex0, "index.mapping.pattern_text.disable_templating"));
            Map<String, Object> mapping = getMapping(client(), backingIndex0);
            Map<String, Object> patternFieldMapping = (Map<String, Object>) ((Map<String, Object>) mapping.get("properties")).get(
                "pattern_field"
            );
            assertThat(patternFieldMapping, hasEntry("disable_templating", true));
        }

        String backingIndex1 = getDataStreamBackingIndex(client(), dataStreamName, 1);
        {
            assertEquals("false", getSetting(client(), backingIndex1, "index.mapping.pattern_text.disable_templating"));
            Map<String, Object> mapping = getMapping(client(), backingIndex1);
            Map<String, Object> patternFieldMapping = (Map<String, Object>) ((Map<String, Object>) mapping.get("properties")).get(
                "pattern_field"
            );
            assertThat(patternFieldMapping, not(hasKey("disable_templating")));
        }

    }
}
