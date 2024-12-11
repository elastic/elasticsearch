/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.remotecluster;

import org.elasticsearch.Build;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.MapMatcher;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.elasticsearch.test.junit.RunnableTestRuleAdapter;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.test.ListMatcher.matchesList;
import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.elasticsearch.test.MapMatcher.matchesMap;

public class RemoteClusterSecurityDataStreamEsqlIT extends AbstractRemoteClusterSecurityTestCase {
    private static final AtomicReference<Map<String, Object>> API_KEY_MAP_REF = new AtomicReference<>();
    private static final AtomicBoolean SSL_ENABLED_REF = new AtomicBoolean();
    private static final AtomicBoolean NODE1_RCS_SERVER_ENABLED = new AtomicBoolean();
    private static final AtomicBoolean NODE2_RCS_SERVER_ENABLED = new AtomicBoolean();

    static {
        fulfillingCluster = ElasticsearchCluster.local()
            .name("fulfilling-cluster")
            .nodes(3)
            .module("x-pack-autoscaling")
            .module("x-pack-esql")
            .module("x-pack-enrich")
            .module("x-pack-ml")
            .module("x-pack-ilm")
            .module("ingest-common")
            .apply(commonClusterConfig)
            .setting("remote_cluster.port", "0")
            .setting("xpack.ml.enabled", "false")
            .setting("xpack.security.remote_cluster_server.ssl.enabled", () -> String.valueOf(SSL_ENABLED_REF.get()))
            .setting("xpack.security.remote_cluster_server.ssl.key", "remote-cluster.key")
            .setting("xpack.security.remote_cluster_server.ssl.certificate", "remote-cluster.crt")
            .setting("xpack.security.authc.token.enabled", "true")
            .keystore("xpack.security.remote_cluster_server.ssl.secure_key_passphrase", "remote-cluster-password")
            .node(0, spec -> spec.setting("remote_cluster_server.enabled", "true"))
            .node(1, spec -> spec.setting("remote_cluster_server.enabled", () -> String.valueOf(NODE1_RCS_SERVER_ENABLED.get())))
            .node(2, spec -> spec.setting("remote_cluster_server.enabled", () -> String.valueOf(NODE2_RCS_SERVER_ENABLED.get())))
            .build();

        queryCluster = ElasticsearchCluster.local()
            .name("query-cluster")
            .module("x-pack-autoscaling")
            .module("x-pack-esql")
            .module("x-pack-enrich")
            .module("x-pack-ml")
            .module("x-pack-ilm")
            .module("ingest-common")
            .apply(commonClusterConfig)
            .setting("xpack.ml.enabled", "false")
            .setting("xpack.security.remote_cluster_client.ssl.enabled", () -> String.valueOf(SSL_ENABLED_REF.get()))
            .setting("xpack.security.remote_cluster_client.ssl.certificate_authorities", "remote-cluster-ca.crt")
            .setting("xpack.security.authc.token.enabled", "true")
            .keystore("cluster.remote.my_remote_cluster.credentials", () -> {
                if (API_KEY_MAP_REF.get() == null) {
                    final Map<String, Object> apiKeyMap = createCrossClusterAccessApiKey("""
                        {
                          "search": [
                            {
                                "names": ["logs-*", "alias-*"]
                            }
                          ]
                        }""");
                    API_KEY_MAP_REF.set(apiKeyMap);
                }
                return (String) API_KEY_MAP_REF.get().get("encoded");
            })
            .rolesFile(Resource.fromClasspath("roles.yml"))
            .user("logs_foo_all", "x-pack-test-password", "logs_foo_all", false)
            .user("logs_foo_16_only", "x-pack-test-password", "logs_foo_16_only", false)
            .user("logs_foo_after_2021", "x-pack-test-password", "logs_foo_after_2021", false)
            .user("logs_foo_after_2021_pattern", "x-pack-test-password", "logs_foo_after_2021_pattern", false)
            .user("logs_foo_after_2021_alias", "x-pack-test-password", "logs_foo_after_2021_alias", false)
            .build();
    }

    @ClassRule
    // Use a RuleChain to ensure that fulfilling cluster is started before query cluster
    // `SSL_ENABLED_REF` is used to control the SSL-enabled setting on the test clusters
    // We set it here, since randomization methods are not available in the static initialize context above
    public static TestRule clusterRule = RuleChain.outerRule(new RunnableTestRuleAdapter(() -> {
        SSL_ENABLED_REF.set(usually());
        NODE1_RCS_SERVER_ENABLED.set(randomBoolean());
        NODE2_RCS_SERVER_ENABLED.set(randomBoolean());
    })).around(fulfillingCluster).around(queryCluster);

    public void testDataStreamWithDls() throws Exception {
        configureRemoteCluster();
        createDataStream();
        MapMatcher twoResults = matchesMap().extraOk().entry("values", matchesList().item(matchesList().item(2)));
        MapMatcher oneResult = matchesMap().extraOk().entry("values", matchesList().item(matchesList().item(1)));
        assertMap(entityAsMap(runESQLCommand("logs_foo_all", "FROM my_remote_cluster:logs-foo | STATS COUNT(*)")), twoResults);
        assertMap(entityAsMap(runESQLCommand("logs_foo_16_only", "FROM my_remote_cluster:logs-foo | STATS COUNT(*)")), oneResult);
        assertMap(entityAsMap(runESQLCommand("logs_foo_after_2021", "FROM my_remote_cluster:logs-foo | STATS COUNT(*)")), oneResult);
        assertMap(
            entityAsMap(runESQLCommand("logs_foo_after_2021_pattern", "FROM my_remote_cluster:logs-foo | STATS COUNT(*)")),
            oneResult
        );
        assertMap(entityAsMap(runESQLCommand("logs_foo_all", "FROM my_remote_cluster:logs-* | STATS COUNT(*)")), twoResults);
        assertMap(entityAsMap(runESQLCommand("logs_foo_16_only", "FROM my_remote_cluster:logs-* | STATS COUNT(*)")), oneResult);
        assertMap(entityAsMap(runESQLCommand("logs_foo_after_2021", "FROM my_remote_cluster:logs-* | STATS COUNT(*)")), oneResult);
        assertMap(entityAsMap(runESQLCommand("logs_foo_after_2021_pattern", "FROM my_remote_cluster:logs-* | STATS COUNT(*)")), oneResult);

        assertMap(entityAsMap(runESQLCommand("logs_foo_after_2021_alias", "FROM my_remote_cluster:alias-foo | STATS COUNT(*)")), oneResult);
        assertMap(entityAsMap(runESQLCommand("logs_foo_after_2021_alias", "FROM my_remote_cluster:alias-* | STATS COUNT(*)")), oneResult);
    }

    protected Response runESQLCommand(String user, String command) throws IOException {
        if (command.toLowerCase(Locale.ROOT).contains("limit") == false) {
            // add a (high) limit to avoid warnings on default limit
            command += " | limit 10000000";
        }
        XContentBuilder json = JsonXContent.contentBuilder();
        json.startObject();
        json.field("query", command);
        addRandomPragmas(json);
        json.endObject();
        Request request = new Request("POST", "_query");
        request.setJsonEntity(org.elasticsearch.common.Strings.toString(json));
        request.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("es-security-runas-user", user));
        request.addParameter("error_trace", "true");
        return adminClient().performRequest(request);
    }

    static void addRandomPragmas(XContentBuilder builder) throws IOException {
        if (Build.current().isSnapshot()) {
            Settings pragmas = randomPragmas();
            if (pragmas != Settings.EMPTY) {
                builder.startObject("pragma");
                builder.value(pragmas);
                builder.endObject();
            }
        }
    }

    static Settings randomPragmas() {
        Settings.Builder settings = Settings.builder();
        if (randomBoolean()) {
            settings.put("page_size", between(1, 5));
        }
        if (randomBoolean()) {
            settings.put("exchange_buffer_size", between(1, 2));
        }
        if (randomBoolean()) {
            settings.put("data_partitioning", randomFrom("shard", "segment", "doc"));
        }
        if (randomBoolean()) {
            settings.put("enrich_max_workers", between(1, 5));
        }
        if (randomBoolean()) {
            settings.put("node_level_reduction", randomBoolean());
        }
        return settings.build();
    }

    private void createDataStream() throws IOException {
        createDataStreamPolicy();
        createDataStreamComponentTemplate();
        createDataStreamIndexTemplate();
        createDataStreamDocuments();
        createDataStreamAlias();
    }

    private void createDataStreamPolicy() throws IOException {
        Request request = new Request("PUT", "_ilm/policy/my-lifecycle-policy");
        request.setJsonEntity("""
            {
              "policy": {
                "phases": {
                  "hot": {
                    "actions": {
                      "rollover": {
                        "max_primary_shard_size": "50gb"
                      }
                    }
                  },
                  "delete": {
                    "min_age": "735d",
                    "actions": {
                      "delete": {}
                    }
                  }
                }
              }
            }""");

        performRequestAgainstFulfillingCluster(request);
    }

    private void createDataStreamComponentTemplate() throws IOException {
        Request request = new Request("PUT", "_component_template/my-template");
        request.setJsonEntity("""
            {
                "template": {
                   "settings": {
                        "index.lifecycle.name": "my-lifecycle-policy"
                   },
                   "mappings": {
                       "properties": {
                           "@timestamp": {
                               "type": "date",
                               "format": "date_optional_time||epoch_millis"
                           },
                           "data_stream": {
                               "properties": {
                                   "namespace": {"type": "keyword"}
                               }
                           }
                       }
                   }
                }
            }""");
        performRequestAgainstFulfillingCluster(request);
    }

    private void createDataStreamIndexTemplate() throws IOException {
        Request request = new Request("PUT", "_index_template/my-index-template");
        request.setJsonEntity("""
            {
                "index_patterns": ["logs-*"],
                "data_stream": {},
                "composed_of": ["my-template"],
                "priority": 500
            }""");
        performRequestAgainstFulfillingCluster(request);
    }

    private void createDataStreamDocuments() throws IOException {
        Request request = new Request("POST", "logs-foo/_bulk");
        request.addParameter("refresh", "");
        request.setJsonEntity("""
            { "create" : {} }
            { "@timestamp": "2099-05-06T16:21:15.000Z", "data_stream": {"namespace": "16"} }
            { "create" : {} }
            { "@timestamp": "2001-05-06T16:21:15.000Z", "data_stream": {"namespace": "17"} }
            """);
        assertMap(entityAsMap(performRequestAgainstFulfillingCluster(request)), matchesMap().extraOk().entry("errors", false));
    }

    private void createDataStreamAlias() throws IOException {
        Request request = new Request("PUT", "_alias");
        request.setJsonEntity("""
            {
              "actions": [
                {
                  "add": {
                    "index": "logs-foo",
                    "alias": "alias-foo"
                  }
                }
              ]
            }""");
        assertMap(entityAsMap(performRequestAgainstFulfillingCluster(request)), matchesMap().extraOk().entry("errors", false));
    }

    record ExpectedCluster(String clusterAlias, String indexExpression, String status, Integer totalShards) {}
}
