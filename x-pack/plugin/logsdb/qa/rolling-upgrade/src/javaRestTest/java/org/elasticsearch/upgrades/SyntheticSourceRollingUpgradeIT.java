/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.upgrades;

import com.carrotsearch.randomizedtesting.annotations.Name;

import org.elasticsearch.client.Request;
import org.elasticsearch.test.rest.ObjectPath;
import org.hamcrest.Matchers;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

import static org.elasticsearch.upgrades.MatchOnlyTextRollingUpgradeIT.createTemplate;
import static org.elasticsearch.upgrades.MatchOnlyTextRollingUpgradeIT.formatInstant;
import static org.elasticsearch.upgrades.MatchOnlyTextRollingUpgradeIT.getIndexSettingsWithDefaults;
import static org.elasticsearch.upgrades.MatchOnlyTextRollingUpgradeIT.startTrial;
import static org.elasticsearch.upgrades.StandardToLogsDbIndexModeRollingUpgradeIT.enableLogsdbByDefault;
import static org.elasticsearch.upgrades.StandardToLogsDbIndexModeRollingUpgradeIT.getWriteBackingIndex;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;

public class SyntheticSourceRollingUpgradeIT extends AbstractRollingUpgradeWithSecurityTestCase {

    static String BULK_ITEM_TEMPLATE = """
        {"@timestamp": "$now", "field1": "$field1", "field2": $field2, "field3": $field3, "field4": $field4}
        """;

    private static final String TEMPLATE = """
        {
            "mappings": {
              "properties": {
                "@timestamp" : {
                  "type": "date"
                },
                "field1": {
                  "type": "keyword"
                },
                "field2": {
                  "type": "keyword"
                },
                "field3": {
                  "type": "long"
                },
                "field4": {
                  "type": "long"
                }
              }
            }
        }""";

    private static final Integer[] VALUES = new Integer[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };

    public SyntheticSourceRollingUpgradeIT(@Name("upgradedNodes") int upgradedNodes) {
        super(upgradedNodes);
    }

    public void testIndexing() throws Exception {
        assumeTrue("requires storing leaf array offsets", oldClusterHasFeature("gte_v9.1.0"));
        String dataStreamName = "logs-bwc-test";
        if (isOldCluster()) {
            startTrial();
            enableLogsdbByDefault();
            createTemplate(dataStreamName, getClass().getSimpleName().toLowerCase(Locale.ROOT), TEMPLATE);

            Instant startTime = Instant.now().minusSeconds(60 * 60);
            bulkIndex(dataStreamName, 4, 1024, startTime);

            String firstBackingIndex = getWriteBackingIndex(client(), dataStreamName, 0);
            var settings = (Map<?, ?>) getIndexSettingsWithDefaults(firstBackingIndex).get(firstBackingIndex);
            assertThat(((Map<?, ?>) settings.get("settings")).get("index.mode"), equalTo("logsdb"));
            assertThat(((Map<?, ?>) settings.get("defaults")).get("index.mapping.source.mode"), equalTo("SYNTHETIC"));

            ensureGreen(dataStreamName);
            search(dataStreamName);
            query(dataStreamName);
        } else if (isMixedCluster()) {
            Instant startTime = Instant.now().minusSeconds(60 * 30);
            bulkIndex(dataStreamName, 4, 1024, startTime);

            ensureGreen(dataStreamName);
            search(dataStreamName);
            query(dataStreamName);
        } else if (isUpgradedCluster()) {
            ensureGreen(dataStreamName);
            Instant startTime = Instant.now();
            bulkIndex(dataStreamName, 4, 1024, startTime);
            search(dataStreamName);
            query(dataStreamName);

            var forceMergeRequest = new Request("POST", "/" + dataStreamName + "/_forcemerge");
            forceMergeRequest.addParameter("max_num_segments", "1");
            assertOK(client().performRequest(forceMergeRequest));

            ensureGreen(dataStreamName);
            search(dataStreamName);
            query(dataStreamName);
        }
    }

    static String bulkIndex(String dataStreamName, int numRequest, int numDocs, Instant startTime) throws Exception {
        String firstIndex = null;
        for (int i = 0; i < numRequest; i++) {
            var bulkRequest = new Request("POST", "/" + dataStreamName + "/_bulk");
            StringBuilder requestBody = new StringBuilder();
            for (int j = 0; j < numDocs; j++) {
                String field1 = Integer.toString(randomFrom(VALUES));
                var randomArray = randomArray(1, 3, Integer[]::new, () -> randomFrom(VALUES));
                String field2 = Arrays.stream(randomArray).map(s -> "\"" + s + "\"").collect(Collectors.joining(","));
                int field3 = randomFrom(VALUES);
                String field4 = Arrays.stream(randomArray).map(String::valueOf).collect(Collectors.joining(","));

                requestBody.append("{\"create\": {}}");
                requestBody.append('\n');
                requestBody.append(
                    BULK_ITEM_TEMPLATE.replace("$now", formatInstant(startTime))
                        .replace("$field1", field1)
                        .replace("$field2", "[" + field2 + "]")
                        .replace("$field3", Long.toString(field3))
                        .replace("$field4", "[" + field4 + "]")
                );
                requestBody.append('\n');

                startTime = startTime.plusMillis(1);
            }
            bulkRequest.setJsonEntity(requestBody.toString());
            bulkRequest.addParameter("refresh", "true");
            var response = client().performRequest(bulkRequest);
            assertOK(response);
            var responseBody = entityAsMap(response);
            assertThat("errors in response:\n " + responseBody, responseBody.get("errors"), equalTo(false));
            if (firstIndex == null) {
                firstIndex = (String) ((Map<?, ?>) ((Map<?, ?>) ((List<?>) responseBody.get("items")).get(0)).get("create")).get("_index");
            }
        }
        return firstIndex;
    }

    void search(String dataStreamName) throws Exception {
        var searchRequest = new Request("POST", "/" + dataStreamName + "/_search");
        searchRequest.addParameter("pretty", "true");
        searchRequest.setJsonEntity("""
            {
                "size": 500
            }
            """);
        var response = client().performRequest(searchRequest);
        assertOK(response);
        var responseBody = entityAsMap(response);
        logger.info("{}", responseBody);
        assertThat(ObjectPath.evaluate(responseBody, "_shards.failed"), Matchers.equalTo(0));
        Integer totalCount = ObjectPath.evaluate(responseBody, "hits.total.value");
        assertThat(totalCount, greaterThanOrEqualTo(512));

        Map<?, ?> firstSource = ObjectPath.evaluate(responseBody, "hits.hits.0._source");
        Integer field1 = Integer.valueOf((String) firstSource.get("field1"));
        assertThat(field1, both(greaterThanOrEqualTo(0)).and(lessThanOrEqualTo(9)));
        List<?> field2 = (List<?>) firstSource.get("field2");
        assertThat(field2, not(emptyIterable()));
        for (var e : field2) {
            Integer value = Integer.valueOf((String) e);
            assertThat(value, both(greaterThanOrEqualTo(0)).and(lessThanOrEqualTo(9)));
        }
        Integer field3 = (Integer) firstSource.get("field3");
        assertThat(field3, both(greaterThanOrEqualTo(0)).and(lessThanOrEqualTo(9)));
        List<?> field4 = (List<?>) firstSource.get("field4");
        assertThat(field4, not(emptyIterable()));
        for (var e : field4) {
            Integer value = (Integer) e;
            assertThat(value, both(greaterThanOrEqualTo(0)).and(lessThanOrEqualTo(9)));
        }
    }

    void query(String dataStreamName) throws Exception {
        var queryRequest = new Request("POST", "/_query");
        queryRequest.addParameter("pretty", "true");
        queryRequest.setJsonEntity("""
            {
                "query": "FROM $ds | SORT @timestamp | KEEP field1,field2,field3,field4 | LIMIT 5"
            }
            """.replace("$ds", dataStreamName));
        var response = client().performRequest(queryRequest);
        assertOK(response);
        var responseBody = entityAsMap(response);
        logger.info("{}", responseBody);

        String column1 = ObjectPath.evaluate(responseBody, "columns.0.name");
        String column2 = ObjectPath.evaluate(responseBody, "columns.1.name");
        String column3 = ObjectPath.evaluate(responseBody, "columns.2.name");
        String column4 = ObjectPath.evaluate(responseBody, "columns.3.name");
        assertThat(column1, equalTo("field1"));
        assertThat(column2, equalTo("field2"));
        assertThat(column3, equalTo("field3"));
        assertThat(column4, equalTo("field4"));

        {
            var field1 = Integer.valueOf(ObjectPath.evaluate(responseBody, "values.0.0"));
            assertThat(field1, both(greaterThanOrEqualTo(0)).and(lessThanOrEqualTo(9)));
        }
        {
            var object = ObjectPath.evaluate(responseBody, "values.0.1");
            if (object instanceof List<?> field2) {
                assertThat(field2, not(emptyIterable()));
                for (var e : field2) {
                    Integer value = Integer.valueOf((String) e);
                    assertThat(value, both(greaterThanOrEqualTo(0)).and(lessThanOrEqualTo(9)));
                }
            } else {
                Integer field2 = Integer.valueOf((String) object);
                assertThat(field2, both(greaterThanOrEqualTo(0)).and(lessThanOrEqualTo(9)));
            }
        }
        {
            Integer field3 = ObjectPath.evaluate(responseBody, "values.0.2");
            assertThat(field3, both(greaterThanOrEqualTo(0)).and(lessThanOrEqualTo(9)));
        }
        {
            var object = ObjectPath.evaluate(responseBody, "values.0.3");
            if (object instanceof List<?> field4) {
                assertThat(field4, not(emptyIterable()));
                for (var e : field4) {
                    Integer value = (Integer) e;
                    assertThat(value, both(greaterThanOrEqualTo(0)).and(lessThanOrEqualTo(9)));
                }
            } else {
                Integer field4 = (Integer) object;
                assertThat(field4, both(greaterThanOrEqualTo(0)).and(lessThanOrEqualTo(9)));
            }
        }
    }

}
