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
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.core.Strings;
import org.elasticsearch.test.MapMatcher;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.util.List;
import java.util.Locale;

import static org.elasticsearch.test.ListMatcher.matchesList;
import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.elasticsearch.test.MapMatcher.matchesMap;

// TODO consolidate me with RemoteClusterSecurityDataStreamEsqlRcs2IT
public class RemoteClusterSecurityDataStreamEsqlRcs1IT extends AbstractRemoteClusterSecurityTestCase {
    static {
        fulfillingCluster = ElasticsearchCluster.local()
            .name("fulfilling-cluster")
            .module("x-pack-autoscaling")
            .module("x-pack-esql")
            .module("x-pack-enrich")
            .module("x-pack-ml")
            .module("x-pack-ilm")
            .module("ingest-common")
            .apply(commonClusterConfig)
            .setting("xpack.ml.enabled", "false")
            .setting("xpack.security.authc.token.enabled", "true")
            .rolesFile(Resource.fromClasspath("roles.yml"))
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
            .setting("xpack.security.authc.token.enabled", "true")
            .rolesFile(Resource.fromClasspath("roles.yml"))
            .user("logs_foo_all", "x-pack-test-password", "logs_foo_all", false)
            .user("logs_foo_16_only", "x-pack-test-password", "logs_foo_16_only", false)
            .user("logs_foo_after_2021", "x-pack-test-password", "logs_foo_after_2021", false)
            .user("logs_foo_after_2021_pattern", "x-pack-test-password", "logs_foo_after_2021_pattern", false)
            .user("logs_foo_after_2021_alias", "x-pack-test-password", "logs_foo_after_2021_alias", false)
            .build();
    }

    @ClassRule
    public static TestRule clusterRule = RuleChain.outerRule(fulfillingCluster).around(queryCluster);

    public void testDataStreamsWithDlsAndFls() throws Exception {
        configureRemoteCluster(REMOTE_CLUSTER_ALIAS, fulfillingCluster, true, randomBoolean(), randomBoolean());
        createDataStreamOnFulfillingCluster();
        setupAdditionalUsersAndRoles();

        doTestDataStreamsWithFlsAndDls();
    }

    private void setupAdditionalUsersAndRoles() throws IOException {
        createUserAndRoleOnQueryCluster("fls_user_logs_pattern", "fls_user_logs_pattern", """
            {
              "indices": [
                {
                  "names": ["logs-*"],
                  "privileges": ["read"],
                  "field_security": {
                    "grant": ["@timestamp", "data_stream.namespace"]
                  }
                }
              ]
            }""");
        createUserAndRoleOnFulfillingCluster("fls_user_logs_pattern", "fls_user_logs_pattern", """
            {
              "indices": [
                {
                  "names": ["logs-*"],
                  "privileges": ["read"],
                  "field_security": {
                    "grant": ["@timestamp", "data_stream.namespace"]
                  }
                }
              ]
            }""");
    }

    static void createUserAndRoleOnQueryCluster(String username, String roleName, String roleJson) throws IOException {
        final var putRoleRequest = new Request("PUT", "/_security/role/" + roleName);
        putRoleRequest.setJsonEntity(roleJson);
        assertOK(adminClient().performRequest(putRoleRequest));

        final var putUserRequest = new Request("PUT", "/_security/user/" + username);
        putUserRequest.setJsonEntity(Strings.format("""
            {
              "password": "%s",
              "roles" : ["%s"]
            }""", PASS, roleName));
        assertOK(adminClient().performRequest(putUserRequest));
    }

    static void createUserAndRoleOnFulfillingCluster(String username, String roleName, String roleJson) throws IOException {
        final var putRoleRequest = new Request("PUT", "/_security/role/" + roleName);
        putRoleRequest.setJsonEntity(roleJson);
        assertOK(performRequestAgainstFulfillingCluster(putRoleRequest));

        final var putUserRequest = new Request("PUT", "/_security/user/" + username);
        putUserRequest.setJsonEntity(Strings.format("""
            {
              "password": "%s",
              "roles" : ["%s"]
            }""", PASS, roleName));
        assertOK(performRequestAgainstFulfillingCluster(putUserRequest));
    }

    static Response runESQLCommandAgainstQueryCluster(String user, String command) throws IOException {
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
        Response response = adminClient().performRequest(request);
        assertOK(response);
        return response;
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

    static void createDataStreamOnFulfillingCluster() throws Exception {
        createDataStreamPolicy(AbstractRemoteClusterSecurityTestCase::performRequestAgainstFulfillingCluster);
        createDataStreamComponentTemplate(AbstractRemoteClusterSecurityTestCase::performRequestAgainstFulfillingCluster);
        createDataStreamIndexTemplate(AbstractRemoteClusterSecurityTestCase::performRequestAgainstFulfillingCluster);
        createDataStreamDocuments(AbstractRemoteClusterSecurityTestCase::performRequestAgainstFulfillingCluster);
        createDataStreamAlias(AbstractRemoteClusterSecurityTestCase::performRequestAgainstFulfillingCluster);
    }

    private static void createDataStreamPolicy(CheckedFunction<Request, Response, Exception> requestConsumer) throws Exception {
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

        requestConsumer.apply(request);
    }

    private static void createDataStreamComponentTemplate(CheckedFunction<Request, Response, Exception> requestConsumer) throws Exception {
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
                                   "namespace": {"type": "keyword"},
                                   "environment": {"type": "keyword"}
                               }
                           }
                       }
                   }
                }
            }""");
        requestConsumer.apply(request);
    }

    private static void createDataStreamIndexTemplate(CheckedFunction<Request, Response, Exception> requestConsumer) throws Exception {
        Request request = new Request("PUT", "_index_template/my-index-template");
        request.setJsonEntity("""
            {
                "index_patterns": ["logs-*"],
                "data_stream": {},
                "composed_of": ["my-template"],
                "priority": 500
            }""");
        requestConsumer.apply(request);
    }

    private static void createDataStreamDocuments(CheckedFunction<Request, Response, Exception> requestConsumer) throws Exception {
        Request request = new Request("POST", "logs-foo/_bulk");
        request.addParameter("refresh", "");
        request.setJsonEntity("""
            { "create" : {} }
            { "@timestamp": "2099-05-06T16:21:15.000Z", "data_stream": {"namespace": "16", "environment": "dev"} }
            { "create" : {} }
            { "@timestamp": "2001-05-06T16:21:15.000Z", "data_stream": {"namespace": "17", "environment": "prod"} }
            """);
        assertMap(entityAsMap(requestConsumer.apply(request)), matchesMap().extraOk().entry("errors", false));
    }

    private static void createDataStreamAlias(CheckedFunction<Request, Response, Exception> requestConsumer) throws Exception {
        Request request = new Request("POST", "_aliases");
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
        assertMap(entityAsMap(requestConsumer.apply(request)), matchesMap().extraOk().entry("errors", false));
    }

    static void doTestDataStreamsWithFlsAndDls() throws IOException {
        // DLS
        MapMatcher twoResults = matchesMap().extraOk().entry("values", matchesList().item(matchesList().item(2)));
        MapMatcher oneResult = matchesMap().extraOk().entry("values", matchesList().item(matchesList().item(1)));
        assertMap(
            entityAsMap(runESQLCommandAgainstQueryCluster("logs_foo_all", "FROM my_remote_cluster:logs-foo | STATS COUNT(*)")),
            twoResults
        );
        assertMap(
            entityAsMap(runESQLCommandAgainstQueryCluster("logs_foo_16_only", "FROM my_remote_cluster:logs-foo | STATS COUNT(*)")),
            oneResult
        );
        assertMap(
            entityAsMap(runESQLCommandAgainstQueryCluster("logs_foo_after_2021", "FROM my_remote_cluster:logs-foo | STATS COUNT(*)")),
            oneResult
        );
        assertMap(
            entityAsMap(
                runESQLCommandAgainstQueryCluster("logs_foo_after_2021_pattern", "FROM my_remote_cluster:logs-foo | STATS COUNT(*)")
            ),
            oneResult
        );
        assertMap(
            entityAsMap(runESQLCommandAgainstQueryCluster("logs_foo_all", "FROM my_remote_cluster:logs-* | STATS COUNT(*)")),
            twoResults
        );
        assertMap(
            entityAsMap(runESQLCommandAgainstQueryCluster("logs_foo_16_only", "FROM my_remote_cluster:logs-* | STATS COUNT(*)")),
            oneResult
        );
        assertMap(
            entityAsMap(runESQLCommandAgainstQueryCluster("logs_foo_after_2021", "FROM my_remote_cluster:logs-* | STATS COUNT(*)")),
            oneResult
        );
        assertMap(
            entityAsMap(runESQLCommandAgainstQueryCluster("logs_foo_after_2021_pattern", "FROM my_remote_cluster:logs-* | STATS COUNT(*)")),
            oneResult
        );

        assertMap(
            entityAsMap(
                runESQLCommandAgainstQueryCluster("logs_foo_after_2021_alias", "FROM my_remote_cluster:alias-foo | STATS COUNT(*)")
            ),
            oneResult
        );
        assertMap(
            entityAsMap(runESQLCommandAgainstQueryCluster("logs_foo_after_2021_alias", "FROM my_remote_cluster:alias-* | STATS COUNT(*)")),
            oneResult
        );

        // FLS
        // logs_foo_all does not have FLS restrictions so should be able to access all fields
        assertMap(
            entityAsMap(
                runESQLCommandAgainstQueryCluster("logs_foo_all", "FROM my_remote_cluster:logs-foo | SORT data_stream.namespace | LIMIT 1")
            ),
            matchesMap().extraOk()
                .entry(
                    "columns",
                    List.of(
                        matchesMap().entry("name", "@timestamp").entry("type", "date"),
                        matchesMap().entry("name", "data_stream.environment").entry("type", "keyword"),
                        matchesMap().entry("name", "data_stream.namespace").entry("type", "keyword")
                    )
                )
        );
        assertMap(
            entityAsMap(
                runESQLCommandAgainstQueryCluster("logs_foo_all", "FROM my_remote_cluster:logs-* | SORT data_stream.namespace | LIMIT 1")
            ),
            matchesMap().extraOk()
                .entry(
                    "columns",
                    List.of(
                        matchesMap().entry("name", "@timestamp").entry("type", "date"),
                        matchesMap().entry("name", "data_stream.environment").entry("type", "keyword"),
                        matchesMap().entry("name", "data_stream.namespace").entry("type", "keyword")
                    )
                )
        );

        assertMap(
            entityAsMap(
                runESQLCommandAgainstQueryCluster(
                    "fls_user_logs_pattern",
                    "FROM my_remote_cluster:logs-foo | SORT data_stream.namespace | LIMIT 1"
                )
            ),
            matchesMap().extraOk()
                .entry(
                    "columns",
                    List.of(
                        matchesMap().entry("name", "@timestamp").entry("type", "date"),
                        matchesMap().entry("name", "data_stream.namespace").entry("type", "keyword")
                    )
                )
        );
        assertMap(
            entityAsMap(
                runESQLCommandAgainstQueryCluster(
                    "fls_user_logs_pattern",
                    "FROM my_remote_cluster:logs-* | SORT data_stream.namespace | LIMIT 1"
                )
            ),
            matchesMap().extraOk()
                .entry(
                    "columns",
                    List.of(
                        matchesMap().entry("name", "@timestamp").entry("type", "date"),
                        matchesMap().entry("name", "data_stream.namespace").entry("type", "keyword")
                    )
                )
        );
    }
}
