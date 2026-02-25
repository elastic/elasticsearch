/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.patterntext;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.xpack.logsdb.DataStreamLicenseChangeTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

public class PatternTextLicenseDowngradeIT extends DataStreamLicenseChangeTestCase {
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
    public void testLicenseDowngrade() throws IOException {
        final String dataStreamName = "logs-test-pattern-text";

        startTrial();
        assertOK(putComponentTemplate(client(), "logs@custom", patternTextMapping));
        assertOK(createDataStream(client(), dataStreamName));

        String backingIndex0 = getDataStreamBackingIndex(client(), dataStreamName, 0);
        {
            assertEquals("false", getSetting(client(), backingIndex0, "index.mapping.pattern_text.disable_templating"));
            Map<String, Object> mapping = getMapping(client(), backingIndex0);
            Map<String, Object> patternFieldMapping = (Map<String, Object>) ((Map<String, Object>) mapping.get("properties")).get(
                "pattern_field"
            );
            assertThat(patternFieldMapping, not(hasKey("disable_templating")));
        }

        // Index docs while on trial license (templating enabled)
        List<String> preDowngradeDocs = List.of("foo 123 bar", "baz 456 qux", "hello 789 world");
        indexDocs(dataStreamName, preDowngradeDocs);

        // Verify search returns pre-downgrade docs
        assertSearchReturnsValues(preDowngradeDocs);

        startBasic();

        // Index docs after downgrade into the same backing index (still has disable_templating=false)
        List<String> postDowngradeDocs = List.of("post downgrade 111", "another post 222");
        indexDocs(dataStreamName, postDowngradeDocs);

        // Verify search returns all docs from before and after downgrade
        List<String> allDocsBeforeRollover = new ArrayList<>(preDowngradeDocs);
        allDocsBeforeRollover.addAll(postDowngradeDocs);
        assertSearchReturnsValues(allDocsBeforeRollover);

        // Original backing index settings remain unchanged after downgrade
        {
            assertEquals("false", getSetting(client(), backingIndex0, "index.mapping.pattern_text.disable_templating"));
            Map<String, Object> mapping = getMapping(client(), backingIndex0);
            Map<String, Object> patternFieldMapping = (Map<String, Object>) ((Map<String, Object>) mapping.get("properties")).get(
                "pattern_field"
            );
            assertThat(patternFieldMapping, not(hasKey("disable_templating")));
        }

        rolloverDataStream(client(), dataStreamName);

        String backingIndex1 = getDataStreamBackingIndex(client(), dataStreamName, 1);
        {
            assertEquals("true", getSetting(client(), backingIndex1, "index.mapping.pattern_text.disable_templating"));
            Map<String, Object> mapping = getMapping(client(), backingIndex1);
            Map<String, Object> patternFieldMapping = (Map<String, Object>) ((Map<String, Object>) mapping.get("properties")).get(
                "pattern_field"
            );
            assertThat(patternFieldMapping, hasEntry("disable_templating", true));
        }

        // Index docs into the new (downgraded) backing index
        List<String> postRolloverDocs = List.of("rolled over 333", "new index 444", "downgraded 555");
        indexDocs(dataStreamName, postRolloverDocs);

        // Verify search across all backing indices returns every doc
        List<String> allDocs = new ArrayList<>(allDocsBeforeRollover);
        allDocs.addAll(postRolloverDocs);
        assertSearchReturnsValues(allDocs);

        // Fetch pattern_field via the "fields" parameter, which exercises the valueFetcher() code path.
        // With disabled templating, valueFetcher must load values from binary doc values or stored fields
        // rather than from pattern_text template+args doc values.
        assertFieldsFetchReturnsValues(allDocs);
    }

    private static void indexDocs(String dataStreamName, List<String> values) throws IOException {
        StringBuilder sb = new StringBuilder();
        for (String value : values) {
            sb.append("{\"create\": {}}\n");
            sb.append("{\"pattern_field\": \"").append(value).append("\"}\n");
        }
        assertOK(bulkIndex(client(), dataStreamName, sb::toString));
    }

    @SuppressWarnings("unchecked")
    private static void assertSearchReturnsValues(List<String> expectedValues) throws IOException {
        Request searchRequest = new Request("GET", "/" + "logs-test-pattern-text" + "/_search");
        searchRequest.setJsonEntity("""
            {
                "query": {"match_all": {}},
                "size": 100
            }
            """);
        Response response = client().performRequest(searchRequest);
        assertThat(response.getStatusLine().getStatusCode(), is(200));
        ObjectPath objectPath = ObjectPath.createFromResponse(response);

        assertThat(objectPath.evaluate("hits.total.value"), equalTo(expectedValues.size()));
        List<Map<String, Object>> hits = objectPath.evaluate("hits.hits");
        Set<String> actual = new HashSet<>();
        for (Map<String, Object> hit : hits) {
            Map<String, Object> source = (Map<String, Object>) hit.get("_source");
            actual.add((String) source.get("pattern_field"));
        }
        assertEquals(new HashSet<>(expectedValues), actual);
    }

    @SuppressWarnings("unchecked")
    private static void assertFieldsFetchReturnsValues(List<String> expectedValues)
        throws IOException {
        Request searchRequest = new Request("GET", "/" + "logs-test-pattern-text" + "/_search");
        searchRequest.setJsonEntity("""
            {
                "query": {"match_all": {}},
                "fields": ["pattern_field"],
                "size": 100
            }
            """);
        Response response = client().performRequest(searchRequest);
        assertThat(response.getStatusLine().getStatusCode(), is(200));
        ObjectPath objectPath = ObjectPath.createFromResponse(response);

        assertThat(objectPath.evaluate("hits.total.value"), equalTo(expectedValues.size()));
        List<Map<String, Object>> hits = objectPath.evaluate("hits.hits");
        Set<String> actual = new HashSet<>();
        for (Map<String, Object> hit : hits) {
            Map<String, Object> fields = (Map<String, Object>) hit.get("fields");
            List<String> values = (List<String>) fields.get("pattern_field");
            assertThat(values.size(), equalTo(1));
            actual.add(values.get(0));
        }
        assertEquals(new HashSet<>(expectedValues), actual);
    }
}
