/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.remotecluster;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.core.Strings;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.elasticsearch.test.rest.ObjectPath;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class RemoteClusterSecurityMlIT extends AbstractRemoteClusterSecurityTestCase {

    private static final AtomicReference<Map<String, Object>> API_KEY_MAP_REF = new AtomicReference<>();
    private static final String REMOTE_ML_USER = "remote_ml_user";

    static {
        fulfillingCluster = ElasticsearchCluster.local()
            .name("fulfilling-cluster")
            .apply(commonClusterConfig)
            .distribution(DistributionType.DEFAULT)
            .setting("remote_cluster_server.enabled", "true")
            .setting("remote_cluster.port", "0")
            .setting("xpack.security.remote_cluster_server.ssl.enabled", "true")
            .setting("xpack.security.remote_cluster_server.ssl.key", "remote-cluster.key")
            .setting("xpack.security.remote_cluster_server.ssl.certificate", "remote-cluster.crt")
            .setting("xpack.security.authc.token.enabled", "true")
            .keystore("xpack.security.remote_cluster_server.ssl.secure_key_passphrase", "remote-cluster-password")
            .build();

        queryCluster = ElasticsearchCluster.local()
            .name("query-cluster")
            .apply(commonClusterConfig)
            .distribution(DistributionType.DEFAULT)
            .setting("xpack.security.remote_cluster_client.ssl.enabled", "true")
            .setting("xpack.security.remote_cluster_client.ssl.certificate_authorities", "remote-cluster-ca.crt")
            .setting("xpack.security.authc.token.enabled", "true")
            .keystore("cluster.remote.my_remote_cluster.credentials", () -> {
                API_KEY_MAP_REF.compareAndSet(null, createCrossClusterAccessApiKey("""
                    {
                        "search": [
                          {
                              "names": ["shared-airline-data"]
                          }
                        ]
                    }"""));
                return (String) API_KEY_MAP_REF.get().get("encoded");
            })
            .rolesFile(Resource.fromClasspath("roles.yml"))
            .user(REMOTE_ML_USER, PASS.toString(), "ml_jobs_shared_airline_data", false)
            .build();
    }

    @ClassRule
    // Use a RuleChain to ensure that fulfilling cluster is started before query cluster
    public static TestRule clusterRule = RuleChain.outerRule(fulfillingCluster).around(queryCluster);

    public void testAnomalyDetectionAndDatafeed() throws Exception {
        configureRemoteCluster();

        // Fulfilling cluster
        {
            final Request createIndexRequest1 = new Request("PUT", "shared-airline-data");
            createIndexRequest1.setJsonEntity("""
                {
                  "mappings": {
                    "properties": {
                      "time": { "type": "date" },
                      "airline": { "type": "keyword" },
                      "responsetime": { "type": "float" },
                      "event_rate": { "type": "integer" }
                    }
                  }
                }""");
            assertOK(performRequestAgainstFulfillingCluster(createIndexRequest1));

            final Request bulkRequest1 = new Request("POST", "/_bulk?refresh=true");
            bulkRequest1.setJsonEntity(Strings.format("""
                {"index": {"_index": "shared-airline-data", "_id": "1"}}
                {"airline": "foo", "responsetime": 1.0, "time" : "2017-02-18T00:00:00Z"}
                {"index": {"_index": "shared-airline-data", "_id": "2"}}
                {"airline": "foo", "responsetime": 1.0, "time" : "2017-02-18T00:30:00Z"}
                {"index": {"_index": "shared-airline-data", "_id": "3"}}
                {"airline": "foo", "responsetime": 42.0, "time" : "2017-02-18T01:00:00Z"}
                {"index": {"_index": "shared-airline-data", "_id": "4"}}
                {"airline": "foo", "responsetime": 42.0, "time" : "2017-02-18T01:01:00Z"}
                """));
            assertOK(performRequestAgainstFulfillingCluster(bulkRequest1));

            // Just some index that is not accessible remotely
            assertOK(performRequestAgainstFulfillingCluster(new Request("PUT", "private-airline-data")));
        }

        // Query cluster
        {
            // A working anomaly detection job
            final var putMlJobRequest1 = new Request("PUT", "/_ml/anomaly_detectors/remote-detection-job");
            putMlJobRequest1.setJsonEntity("""
                {
                  "description":"Analysis of response time by airline",
                  "analysis_config" : {
                      "bucket_span": "1h",
                      "detectors": [{"function":"sum","field_name":"responsetime","by_field_name":"airline"}]
                  },
                  "data_description" : {
                      "format":"xcontent"
                  },
                  "datafeed_config": {
                    "indexes":["my_remote_cluster:shared-airline-data"]
                  }
                }
                """);
            final ObjectPath putMlJobResponse = assertOKAndCreateObjectPath(performRequestWithRemoteMlUser(putMlJobRequest1));
            assertThat(putMlJobResponse.evaluate("job_id"), equalTo("remote-detection-job"));
            assertThat(putMlJobResponse.evaluate("datafeed_config.job_id"), equalTo("remote-detection-job"));
            assertThat(putMlJobResponse.evaluate("datafeed_config.datafeed_id"), equalTo("remote-detection-job"));
            assertThat(putMlJobResponse.evaluate("datafeed_config.authorization.roles"), contains("ml_jobs_shared_airline_data"));

            assertOK(performRequestWithRemoteMlUser(new Request("POST", "/_ml/anomaly_detectors/remote-detection-job/_open")));
            assertOK(performRequestWithRemoteMlUser(new Request("POST", "/_ml/datafeeds/remote-detection-job/_start")));

            final ObjectPath previewDatafeedResponse = assertOKAndCreateObjectPath(
                performRequestWithRemoteMlUser(new Request("GET", "/_ml/datafeeds/remote-detection-job/_preview"))
            );
            assertThat(previewDatafeedResponse.evaluate("0.time"), equalTo(1487376000000L));
            assertThat(previewDatafeedResponse.evaluate("1.time"), equalTo(1487377800000L));
            assertThat(previewDatafeedResponse.evaluate("2.time"), equalTo(1487379600000L));
            assertThat(previewDatafeedResponse.evaluate("3.time"), equalTo(1487379660000L));

            // A failure case
            final var putMlJobRequest2 = new Request("PUT", "/_ml/anomaly_detectors/invalid");
            putMlJobRequest2.setJsonEntity("""
                {
                  "analysis_config" : {
                      "bucket_span": "1h",
                      "detectors": [{"function":"sum","field_name":"responsetime","by_field_name":"airline"}]
                  },
                  "data_description" : {
                      "format":"xcontent"
                  },
                  "datafeed_config": {
                    "indexes":["my_remote_cluster:private-airline-data"]
                  }
                }
                """);
            final ObjectPath putMlJobResponse2 = assertOKAndCreateObjectPath(performRequestWithRemoteMlUser(putMlJobRequest2));
            assertThat(putMlJobResponse2.evaluate("job_id"), equalTo("invalid"));
            assertThat(putMlJobResponse2.evaluate("datafeed_config.job_id"), equalTo("invalid"));
            assertThat(putMlJobResponse2.evaluate("datafeed_config.datafeed_id"), equalTo("invalid"));

            assertOK(performRequestWithRemoteMlUser(new Request("POST", "/_ml/anomaly_detectors/invalid/_open")));

            final ResponseException startDatafeedException = expectThrows(
                ResponseException.class,
                () -> performRequestWithRemoteMlUser(new Request("POST", "/_ml/datafeeds/invalid/_start"))
            );
            assertThat(startDatafeedException.getResponse().getStatusLine().getStatusCode(), equalTo(403));
            assertThat(startDatafeedException.getMessage(), containsString("unauthorized for user [" + REMOTE_ML_USER + "]"));

            final ResponseException previewDatafeedException = expectThrows(
                ResponseException.class,
                () -> performRequestWithRemoteMlUser(new Request("GET", "/_ml/datafeeds/invalid/_preview"))
            );
            assertThat(previewDatafeedException.getResponse().getStatusLine().getStatusCode(), equalTo(403));
            assertThat(previewDatafeedException.getMessage(), containsString("unauthorized for user [" + REMOTE_ML_USER + "]"));
        }
    }

    public void testDataframeAnalyticsNotSupportForRemoteIndices() {
        final Request putDataframeAnalytics = new Request("PUT", "/_ml/data_frame/analytics/invalid");
        putDataframeAnalytics.setJsonEntity("""
            {
              "source": {
                "index": "my_remote_cluster:shared-airline-data"
              },
              "dest": {
                "index": "data-frame-analytics-dest"
              },
              "analysis": {"outlier_detection":{}}
            }
            """);
        final ResponseException e = expectThrows(ResponseException.class, () -> performRequestWithRemoteMlUser(putDataframeAnalytics));
        assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(400));
        assertThat(e.getMessage(), containsString("remote source indices are not supported"));
    }

    private Response performRequestWithRemoteMlUser(final Request request) throws IOException {
        request.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", basicAuthHeaderValue(REMOTE_ML_USER, PASS)));
        return client().performRequest(request);
    }
}
