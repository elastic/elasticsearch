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
import org.elasticsearch.client.Response;
import org.elasticsearch.common.Strings;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.FeatureFlag;
import org.elasticsearch.test.cluster.local.LocalClusterConfigProvider;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.equalTo;

public class FullClusterRestartDownsampleIT extends ParameterizedFullClusterRestartTestCase {

    private static final String FIXED_INTERVAL = "1h";
    private String index;
    private String policy;
    private String dataStream;

    private static TemporaryFolder repoDirectory = new TemporaryFolder();

    protected static LocalClusterConfigProvider clusterConfig = c -> {};

    private static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .version(getOldClusterTestVersion())
        .nodes(2)
        .setting("xpack.security.enabled", "false")
        .setting("indices.lifecycle.poll_interval", "5s")
        .apply(() -> clusterConfig)
        .feature(FeatureFlag.TIME_SERIES_MODE)
        .feature(FeatureFlag.FAILURE_STORE_ENABLED)
        .build();

    @ClassRule
    public static TestRule ruleChain = RuleChain.outerRule(repoDirectory).around(cluster);

    public FullClusterRestartDownsampleIT(@Name("cluster") FullClusterRestartUpgradeStatus upgradeStatus) {
        super(upgradeStatus);
    }

    @Override
    protected ElasticsearchCluster getUpgradeCluster() {
        return cluster;
    }

    private static final String POLICY = """
        {
          "policy": {
            "phases": {
              "hot": {
                "actions": {
                  "rollover" : {
                    "max_age": "30s"
                  },
                  "downsample": {
                    "fixed_interval": "$interval"
                  }
                }
              }
            }
          }
        }
        """;

    private static final String TEMPLATE = """
        {
            "index_patterns": ["%s*"],
            "template": {
                "settings":{
                    "index": {
                        "number_of_replicas": 0,
                        "number_of_shards": 1,
                        "time_series": {
                          "start_time": "2010-01-01T00:00:00.000Z",
                          "end_time": "2022-01-01T00:00:00.000Z"
                        },
                        "routing_path": ["metricset"],
                        "mode": "time_series",
                        "look_ahead_time": "1m",
                        "lifecycle.name": "%s"
                    }
                },
                "mappings":{
                    "properties": {
                        "@timestamp" : {
                            "type": "date"
                        },
                        "metricset": {
                            "type": "keyword",
                            "time_series_dimension": true
                        },
                        "volume": {
                            "type": "double",
                            "time_series_metric": "gauge"
                        }
                    }
                }
            },
            "data_stream": { }
        }""";

    private static final String TEMPLATE_NO_TIME_BOUNDARIES = """
        {
            "index_patterns": ["%s*"],
            "template": {
                "settings":{
                    "index": {
                        "number_of_replicas": 0,
                        "number_of_shards": 1,
                        "routing_path": ["metricset"],
                        "mode": "time_series",
                        "lifecycle.name": "%s"
                    }
                },
                "mappings":{
                    "properties": {
                        "@timestamp" : {
                            "type": "date"
                        },
                        "metricset": {
                            "type": "keyword",
                            "time_series_dimension": true
                        },
                        "volume": {
                            "type": "double",
                            "time_series_metric": "gauge"
                        }
                    }
                }
            },
            "data_stream": { }
        }""";

    private static final String BULK = """
        {"create": {}}
        {"@timestamp": "2020-01-01T05:10:00Z", "metricset": "pod", "volume" : 10}
        {"create": {}}
        {"@timestamp": "2020-01-01T05:20:00Z", "metricset": "pod", "volume" : 20}
        {"create": {}}
        {"@timestamp": "2020-01-01T05:30:00Z", "metricset": "pod", "volume" : 30}
        {"create": {}}
        {"@timestamp": "2020-01-01T05:40:00Z", "metricset": "pod", "volume" : 40}
        {"create": {}}
        {"@timestamp": "2020-01-01T06:10:00Z", "metricset": "pod", "volume" : 50}
        {"create": {}}
        {"@timestamp": "2020-01-01T07:10:00Z", "metricset": "pod", "volume" : 60}
        {"create": {}}
        {"@timestamp": "2020-01-01T09:10:00Z", "metricset": "pod", "volume" : 70}
        {"create": {}}
        {"@timestamp": "2020-01-01T09:20:00Z", "metricset": "pod", "volume" : 80}
        """;

    @Before
    public void refreshAbstractions() {
        policy = "policy-" + randomAlphaOfLength(5);
        dataStream = "ds-" + randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        index = ".ds-" + dataStream;
        logger.info("--> running [{}] with index [{}], data stream [{}], and policy [{}]", getTestName(), index, dataStream, policy);
    }

    private void createIndex() throws IOException {
        var putIndexTemplateRequest = new Request("POST", "/_index_template/1");
        putIndexTemplateRequest.setJsonEntity(Strings.format(TEMPLATE, dataStream, policy));
        assertOK(client().performRequest(putIndexTemplateRequest));
    }

    private void bulk() throws IOException {
        var bulkRequest = new Request("POST", "/" + dataStream + "/_bulk");
        bulkRequest.setJsonEntity(BULK);
        bulkRequest.addParameter("refresh", "true");
        var response = client().performRequest(bulkRequest);
        assertOK(response);
        var responseBody = entityAsMap(response);
        assertThat("errors in response:\n " + responseBody, responseBody.get("errors"), equalTo(false));
    }

    private void createIlmPolicy() throws IOException {
        Request request = new Request("PUT", "_ilm/policy/" + policy);
        request.setJsonEntity(POLICY.replace("$interval", FIXED_INTERVAL));
        client().performRequest(request);
    }

    private void startDownsampling() throws Exception {
        // Update template to not contain time boundaries anymore (rollover is blocked otherwise due to index time
        // boundaries overlapping after rollover)
        Request updateIndexTemplateRequest = new Request("POST", "/_index_template/1");
        updateIndexTemplateRequest.setJsonEntity(Strings.format(TEMPLATE_NO_TIME_BOUNDARIES, dataStream, policy));
        assertOK(client().performRequest(updateIndexTemplateRequest));

        // Manual rollover the original index such that it's not the write index in the data stream anymore
        Request rolloverRequest = new Request("POST", "/" + dataStream + "/_rollover");
        rolloverRequest.setJsonEntity("""
            {
              "conditions": {
                "max_docs": "1"
              }
            }""");
        client().performRequest(rolloverRequest);
        logger.info("rollover complete");
    }

    private void runQuery() throws Exception {
        String rollup = waitAndGetRollupIndexName();
        assertFalse(rollup.isEmpty());

        // Retry until the downsample index is populated.
        assertBusy(() -> {
            Request request = new Request("POST", "/" + dataStream + "/_search");
            var map = entityAsMap(client().performRequest(request));
            var hits = (List<?>) ((Map<?, ?>) map.get("hits")).get("hits");
            assertEquals(4, hits.size());
            for (var hit : hits) {
                assertEquals(rollup, ((Map<?, ?>) hit).get("_index"));
            }
        }, 30, TimeUnit.SECONDS);
    }

    private String waitAndGetRollupIndexName() throws InterruptedException, IOException {
        final String[] rollupIndexName = new String[1];
        waitUntil(() -> {
            try {
                rollupIndexName[0] = getRollupIndexName();
                return rollupIndexName[0] != null;
            } catch (IOException e) {
                return false;
            }
        }, 120, TimeUnit.SECONDS);
        if (rollupIndexName[0] == null) {
            logger.warn("--> rollup index name is NULL");
        } else {
            logger.info("--> original index name is [{}], rollup index name is [{}]", index, rollupIndexName[0]);
        }
        return rollupIndexName[0];
    }

    private String getRollupIndexName() throws IOException {
        String endpoint = "/downsample-" + FIXED_INTERVAL + "-" + index + "-*/?expand_wildcards=all";
        Response response = client().performRequest(new Request("GET", endpoint));
        Map<String, Object> asMap = responseAsMap(response);
        if (asMap.size() == 1) {
            return (String) asMap.keySet().toArray()[0];
        }
        logger.warn("--> No matching rollup name for path [%s]", endpoint);
        return null;
    }

    public void testRollupIndex() throws Exception {
        assumeTrue("Downsample got many stability improvements in 8.10.0", oldClusterHasFeature("gte_v8.10.0"));
        if (isRunningAgainstOldCluster()) {
            createIlmPolicy();
            createIndex();
            bulk();
            startDownsampling();
        } else {
            runQuery();
        }
    }
}
