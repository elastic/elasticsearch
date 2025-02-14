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
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.test.rest.ObjectPath;

import java.time.Instant;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.upgrades.LogsIndexModeRollingUpgradeIT.getWriteBackingIndex;
import static org.elasticsearch.upgrades.LogsdbIndexingRollingUpgradeIT.createTemplate;
import static org.elasticsearch.upgrades.LogsdbIndexingRollingUpgradeIT.getIndexSettingsWithDefaults;
import static org.elasticsearch.upgrades.LogsdbIndexingRollingUpgradeIT.startTrial;
import static org.elasticsearch.upgrades.TsdbIT.TEMPLATE;
import static org.elasticsearch.upgrades.TsdbIT.formatInstant;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;

public class TsdbIndexingRollingUpgradeIT extends AbstractRollingUpgradeTestCase {

    static String BULK_ITEM_TEMPLATE =
        """
            {"@timestamp": "$now", "metricset": "pod", "k8s": {"pod": {"name": "$name", "uid":"$uid", "ip": "$ip", "network": {"tx": $tx, "rx": $rx}}}}
            """;

    public TsdbIndexingRollingUpgradeIT(@Name("upgradedNodes") int upgradedNodes) {
        super(upgradedNodes);
    }

    public void testIndexing() throws Exception {
        String dataStreamName = "k9s";
        if (isOldCluster()) {
            startTrial();
            createTemplate(dataStreamName, getClass().getSimpleName().toLowerCase(Locale.ROOT), TEMPLATE);

            Instant startTime = Instant.now().minusSeconds(60 * 60);
            bulkIndex(dataStreamName, 4, 1024, startTime);

            String firstBackingIndex = getWriteBackingIndex(client(), dataStreamName, 0);
            var settings = (Map<?, ?>) getIndexSettingsWithDefaults(firstBackingIndex).get(firstBackingIndex);
            assertThat(((Map<?, ?>) settings.get("settings")).get("index.mode"), equalTo("time_series"));
            assertThat(((Map<?, ?>) settings.get("defaults")).get("index.mapping.source.mode"), equalTo("SYNTHETIC"));

            var mapping = getIndexMappingAsMap(firstBackingIndex);
            assertThat(
                "incorrect k8s.pod.name field in mapping:" + mapping,
                "keyword",
                equalTo(ObjectPath.evaluate(mapping, "properties.k8s.properties.pod.properties.name.type"))
            );

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

    static void bulkIndex(String dataStreamName, int numRequest, int numDocs, Instant startTime) throws Exception {
        for (int i = 0; i < numRequest; i++) {
            var bulkRequest = new Request("POST", "/" + dataStreamName + "/_bulk");
            StringBuilder requestBody = new StringBuilder();
            for (int j = 0; j < numDocs; j++) {
                String podName = "pod" + j % 5; // Not realistic, but makes asserting search / query response easier.
                String podUid = randomUUID();
                String podIp = NetworkAddress.format(randomIp(true));
                long podTx = randomLong();
                long podRx = randomLong();

                requestBody.append("{\"create\": {}}");
                requestBody.append('\n');
                requestBody.append(
                    BULK_ITEM_TEMPLATE.replace("$now", formatInstant(startTime))
                        .replace("$name", podName)
                        .replace("$uid", podUid)
                        .replace("$ip", podIp)
                        .replace("$tx", Long.toString(podTx))
                        .replace("$rx", Long.toString(podRx))
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
        }
    }

    void search(String dataStreamName) throws Exception {
        var searchRequest = new Request("POST", "/" + dataStreamName + "/_search");
        searchRequest.addParameter("pretty", "true");
        searchRequest.setJsonEntity("""
            {
                "size": 0,
                "aggs": {
                    "pod_name": {
                        "terms": {
                            "field": "k8s.pod.name",
                            "order": { "_key": "asc" }
                        },
                        "aggs": {
                            "max_tx": {
                                "max": {
                                    "field": "k8s.pod.network.tx"
                                }
                            },
                            "max_rx": {
                                "max": {
                                    "field": "k8s.pod.network.rx"
                                }
                            }
                        }
                    }
                }
            }
            """);
        var response = client().performRequest(searchRequest);
        assertOK(response);
        var responseBody = entityAsMap(response);

        Integer totalCount = ObjectPath.evaluate(responseBody, "hits.total.value");
        assertThat(totalCount, greaterThanOrEqualTo(4096));
        String key = ObjectPath.evaluate(responseBody, "aggregations.pod_name.buckets.0.key");
        assertThat(key, equalTo("pod0"));
        Integer docCount = ObjectPath.evaluate(responseBody, "aggregations.pod_name.buckets.0.doc_count");
        assertThat(docCount, greaterThan(0));
        Double maxTx = ObjectPath.evaluate(responseBody, "aggregations.pod_name.buckets.0.max_tx.value");
        assertThat(maxTx, notNullValue());
        Double maxRx = ObjectPath.evaluate(responseBody, "aggregations.pod_name.buckets.0.max_rx.value");
        assertThat(maxRx, notNullValue());
    }

    void query(String dataStreamName) throws Exception {
        var queryRequest = new Request("POST", "/_query");
        queryRequest.addParameter("pretty", "true");
        queryRequest.setJsonEntity("""
            {
                "query": "FROM $ds | STATS max(k8s.pod.network.rx), max(k8s.pod.network.tx) BY k8s.pod.name | SORT k8s.pod.name | LIMIT 5"
            }
            """.replace("$ds", dataStreamName));
        var response = client().performRequest(queryRequest);
        assertOK(response);
        var responseBody = entityAsMap(response);

        String column1 = ObjectPath.evaluate(responseBody, "columns.0.name");
        String column2 = ObjectPath.evaluate(responseBody, "columns.1.name");
        String column3 = ObjectPath.evaluate(responseBody, "columns.2.name");
        assertThat(column1, equalTo("max(k8s.pod.network.rx)"));
        assertThat(column2, equalTo("max(k8s.pod.network.tx)"));
        assertThat(column3, equalTo("k8s.pod.name"));

        String key = ObjectPath.evaluate(responseBody, "values.0.2");
        assertThat(key, equalTo("pod0"));
        Long maxRx = ObjectPath.evaluate(responseBody, "values.0.0");
        assertThat(maxRx, notNullValue());
        Long maxTx = ObjectPath.evaluate(responseBody, "values.0.1");
        assertThat(maxTx, notNullValue());
    }

}
