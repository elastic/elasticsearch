/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.test.rest.ObjectPath;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

public class KeywordRollingUpgradeIT extends AbstractLogsdbRollingUpgradeTestCase {

    private static final String DATA_STREAM_NAME = "logs-bwc-test";

    private static final List<String> FIELD_VALUES_1 = Arrays.asList(
        "short value 1",
        "short value 2",
        "this value definitely exceeds ignore_above and wont be indexed",
        "another very long value that exceeds ignore_above",
        "another very long value that exceeds ignore_above 2"
    );

    private static final List<String> FIELD_VALUES_2 = Arrays.asList(
        "short value 3",
        "short value 4",
        "short value 5",
        "another very long value that exceeds ignore_above 3",
        "another very long value that exceeds ignore_above 4",
        "another very long value that exceeds ignore_above 5"
    );

    static String ITEM_TEMPLATE = """
        {"@timestamp": "$now", "message": "$message"}
        """;

    private static final String TEMPLATE = """
        {
            "mappings": {
              "properties": {
                "@timestamp" : {
                  "type": "date"
                },
                "message": {
                  "type": "keyword",
                  "ignore_above": 20
                }
              }
            }
        }""";

    public void testIndexingWithIgnoreAbove() throws Exception {
        beforeUpgrade();
        upgradeNodes();
        afterUpgrade();
    }

    private void beforeUpgrade() throws Exception {
        // given - enable logsdb, create a template + index
        LogsdbIndexingRollingUpgradeIT.maybeEnableLogsdbByDefault();
        String templateId = getClass().getSimpleName().toLowerCase(Locale.ROOT);
        LogsdbIndexingRollingUpgradeIT.createTemplate(DATA_STREAM_NAME, templateId, TEMPLATE);

        // when - index a document
        indexDocuments(FIELD_VALUES_1);

        // then - verify that logsdb and synthetic source are enabled before proceeding futher
        String firstBackingIndex = LogsdbIndexingRollingUpgradeIT.getWriteBackingIndex(client(), DATA_STREAM_NAME, 0);
        var settings = (Map<?, ?>) LogsdbIndexingRollingUpgradeIT.getIndexSettingsWithDefaults(firstBackingIndex).get(firstBackingIndex);
        assertThat(((Map<?, ?>) settings.get("settings")).get("index.mode"), equalTo("logsdb"));
        assertThat(((Map<?, ?>) settings.get("defaults")).get("index.mapping.source.mode"), equalTo("SYNTHETIC"));

        LogsdbIndexingRollingUpgradeIT.assertDataStream(DATA_STREAM_NAME, templateId);
        ensureGreen(DATA_STREAM_NAME);

        // then - perform a search, expect all values to be in the response
        search(FIELD_VALUES_1);
    }

    private void afterUpgrade() throws Exception {
        // given - implicitly start from the leftover state after upgrading the cluster

        // when - index a new document
        indexDocuments(FIELD_VALUES_2);

        // then - query the result, expect to find all new values, as well as values from before the cluster was upgraded
        List<String> allValues = new ArrayList<>(FIELD_VALUES_1);
        allValues.addAll(FIELD_VALUES_2);
        search(allValues);
    }

    /**
    * Create an arbitrary document containing the given values.
    */
    private void indexDocuments(List<String> values) throws Exception {
        var request = new Request("POST", "/" + DATA_STREAM_NAME + "/_bulk");
        StringBuilder requestBody = new StringBuilder();

        Instant startTime = Instant.now();

        for (String value : values) {
            requestBody.append("{\"create\": {}}");
            requestBody.append('\n');
            requestBody.append(
                ITEM_TEMPLATE.replace("$now", LogsdbIndexingRollingUpgradeIT.formatInstant(startTime)).replace("$message", value)
            );
            requestBody.append('\n');

            startTime = startTime.plusMillis(1);
        }
        request.setJsonEntity(requestBody.toString());
        request.addParameter("refresh", "true");

        var response = client().performRequest(request);
        assertOK(response);
        var responseBody = entityAsMap(response);
        assertThat("errors in response:\n " + responseBody, responseBody.get("errors"), equalTo(false));
    }

    @SuppressWarnings("unchecked")
    private void search(List<String> expectedValues) throws IOException {
        Request searchRequest = new Request("GET", "/" + DATA_STREAM_NAME + "/_search");
        searchRequest.setJsonEntity("""
            {
            "query": { "match_all": {} },
            "size": 100
            }
            """);

        Response response = client().performRequest(searchRequest);
        assertOK(response);

        // parse the response
        Map<String, Object> responseMap = entityAsMap(response);

        Integer totalCount = ObjectPath.evaluate(responseMap, "hits.total.value");
        assertThat(totalCount, equalTo(expectedValues.size()));

        List<String> values = ((List<Map<String, Object>>) ObjectPath.evaluate(responseMap, "hits.hits")).stream()
            .map(map -> (Map<String, Object>) map.get("_source"))
            .map(source -> {
                assertThat(source.get("message"), notNullValue());
                // The value of FIELD_NAME is now a single String, not a List<String>
                return (String) source.get("message");
            })
            .toList();

        assertThat(values, containsInAnyOrder(expectedValues.toArray()));
    }

    private void upgradeNodes() throws IOException {
        int numNodes = Integer.parseInt(System.getProperty("tests.num_nodes", "3"));
        for (int i = 0; i < numNodes; i++) {
            upgradeNode(i);
        }
    }

}
