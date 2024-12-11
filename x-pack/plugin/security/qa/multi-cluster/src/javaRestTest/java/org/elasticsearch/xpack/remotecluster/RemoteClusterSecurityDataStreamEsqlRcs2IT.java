/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.remotecluster;

import org.elasticsearch.test.MapMatcher;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.elasticsearch.test.junit.RunnableTestRuleAdapter;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.test.ListMatcher.matchesList;
import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.elasticsearch.test.MapMatcher.matchesMap;
import static org.elasticsearch.xpack.remotecluster.RemoteClusterSecurityDataStreamEsqlRcs1IT.createDataStreamOnFulfillingCluster;
import static org.elasticsearch.xpack.remotecluster.RemoteClusterSecurityDataStreamEsqlRcs1IT.runESQLCommandAgainstQueryCluster;

// TODO consolidate me with RemoteClusterSecurityDataStreamEsqlRcs1IT
public class RemoteClusterSecurityDataStreamEsqlRcs2IT extends AbstractRemoteClusterSecurityTestCase {
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

    public void testDataStreamsWithDls() throws Exception {
        configureRemoteCluster();
        createDataStreamOnFulfillingCluster();
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
    }
}
