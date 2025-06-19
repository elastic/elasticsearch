/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.xpack.ml.datafeed;

import org.apache.http.HttpHost;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;

/**
 * A test to check that remote_cluster_client errors are correctly reported when a datafeed job is started.
 * The local datafeed job references a remote index in a local anomaly detection job. When the
 * remote_cluster_client role is missing in the local cluster. This prevents remote indices from being
 * resolved to their cluster names.
 *
 * @see <a href="https://github.com/elastic/elasticsearch/issues/121149">GitHub issue 121149</a>
 */
public class DatafeedRemoteClusterClientIT extends ESRestTestCase {
    public static ElasticsearchCluster remoteCluster = ElasticsearchCluster.local()
        .name("remote_cluster")
        .distribution(DistributionType.DEFAULT)
        .module("data-streams")
        .module("x-pack-stack")
        .setting("xpack.security.enabled", "false")
        .setting("xpack.license.self_generated.type", "trial")
        .setting("cluster.logsdb.enabled", "true")
        .build();

    public static ElasticsearchCluster localCluster = ElasticsearchCluster.local()
        .name("local_cluster")
        .distribution(DistributionType.DEFAULT)
        .module("data-streams")
        .module("x-pack-stack")
        .setting("xpack.security.enabled", "false")
        .setting("xpack.license.self_generated.type", "trial")
        .setting("cluster.logsdb.enabled", "true")
        .setting("node.roles", "[data,ingest,master,ml]") // remote_cluster_client not included
        .setting("cluster.remote.remote_cluster.seeds", () -> "\"" + remoteCluster.getTransportEndpoint(0) + "\"")
        .setting("cluster.remote.connections_per_cluster", "1")
        .setting("cluster.remote.remote_cluster.skip_unavailable", "false")
        .build();

    @ClassRule
    public static TestRule clusterRule = RuleChain.outerRule(remoteCluster).around(localCluster);

    private RestClient localClusterClient() throws IOException {
        var clusterHosts = parseClusterHosts(localCluster.getHttpAddresses());
        return buildClient(restClientSettings(), clusterHosts.toArray(new HttpHost[0]));
    }

    private RestClient remoteClusterClient() throws IOException {
        var clusterHosts = parseClusterHosts(remoteCluster.getHttpAddresses());
        return buildClient(restClientSettings(), clusterHosts.toArray(new HttpHost[0]));
    }

    public void testSource() throws IOException {
        String localIndex = "local_index";
        String remoteIndex = "remote_index";
        String mapping = """
                {
                    "properties": {
                        "timestamp": {
                            "type": "date"
                        },
                        "bytes": {
                            "type": "integer"
                        }
                    }
                }
            """;
        try (RestClient localClient = localClusterClient(); RestClient remoteClient = remoteClusterClient()) {
            createIndex(
                remoteClient,
                remoteIndex,
                Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, between(5, 20)).build(),
                mapping
            );

            Request request = new Request("PUT", "_ml/anomaly_detectors/test_anomaly_detector");
            request.setJsonEntity("""
                {
                  "analysis_config": {
                    "bucket_span": "15m",
                    "detectors": [
                      {
                        "detector_description": "Sum of bytes",
                        "function": "sum",
                        "field_name": "bytes"
                      }
                    ]
                  },
                  "data_description": {
                    "time_field": "timestamp",
                    "time_format": "epoch_ms"
                  },
                  "analysis_limits": {
                    "model_memory_limit": "11MB"
                  },
                  "model_plot_config": {
                    "enabled": true,
                    "annotations_enabled": true
                  },
                  "results_index_name": "test_datafeed_out",
                  "datafeed_config": {
                    "indices": [
                      "remote_cluster:remote_index"
                    ],
                    "query": {
                      "bool": {
                        "must": [
                          {
                            "match_all": {}
                          }
                        ]
                      }
                    },
                    "runtime_mappings": {
                      "hour_of_day": {
                        "type": "long",
                        "script": {
                          "source": "emit(doc['timestamp'].value.getHour());"
                        }
                      }
                    },
                    "datafeed_id": "test_datafeed"
                  }
                }""");
            Response response = localClient.performRequest(request);
            logger.info("Anomaly Detection Response:", response.getStatusLine());

            request = new Request("GET", "_ml/anomaly_detectors/test_anomaly_detector");
            response = localClient.performRequest(request);
            logger.info("Anomaly detection get:", response.getEntity());

            request = new Request("POST", "_ml/anomaly_detectors/test_anomaly_detector/_open");
            response = localClient.performRequest(request);

            final Request startRequest = new Request("POST", "_ml/datafeeds/test_datafeed/_start");
            request.setJsonEntity("""
                {
                    "start": "2019-04-07T18:22:16Z"
                }
                """);
            ResponseException e = assertThrows(ResponseException.class, () -> localClient.performRequest(startRequest));
            assertThat(e.getMessage(), containsString("""
                Datafeed [test_datafeed] is configured with a remote index pattern(s) \
                [remote_cluster:remote_index] but the current node [local_cluster-0] \
                is not allowed to connect to remote clusters. Please enable node.remote_cluster_client \
                for all machine learning nodes and master-eligible nodes"""));
        }
    }

    @Override
    protected String getTestRestCluster() {
        return localCluster.getHttpAddresses();
    }
}
