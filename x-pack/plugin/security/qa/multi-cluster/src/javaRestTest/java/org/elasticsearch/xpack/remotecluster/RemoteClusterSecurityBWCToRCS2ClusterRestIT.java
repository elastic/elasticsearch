/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.remotecluster;

import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.cluster.util.Version;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * BWC test which ensures that users and API keys with defined {@code remote_indices}/{@code remote_cluster} privileges can be used
 * to query older remote clusters when using RCS 2.0. We send the request the to an older fulfilling cluster using RCS 2.0 with a user/role
 * and API key where the {@code remote_indices}/{@code remote_cluster} are defined in the newer query cluster.
 * All new RCS 2.0 config should be effectively ignored when sending to older RCS 2.0. For example, a new privilege
 * sent to an old cluster should be ignored.
 */
public class RemoteClusterSecurityBWCToRCS2ClusterRestIT extends AbstractRemoteClusterSecurityBWCRestIT {

    private static final Version OLD_CLUSTER_VERSION = Version.fromString(System.getProperty("tests.old_cluster_version"));
    private static final AtomicReference<Map<String, Object>> API_KEY_MAP_REF = new AtomicReference<>();

    static {

        fulfillingCluster = ElasticsearchCluster.local()
            .name("fulfilling-cluster")
            .version(OLD_CLUSTER_VERSION)
            .distribution(DistributionType.DEFAULT)
            .apply(commonClusterConfig)
            .setting("xpack.ml.enabled", "false")
            .setting("remote_cluster_server.enabled", "true")
            .setting("remote_cluster.port", "0")
            .setting("xpack.security.remote_cluster_server.ssl.enabled", "true")
            .setting("xpack.security.remote_cluster_server.ssl.key", "remote-cluster.key")
            .setting("xpack.security.remote_cluster_server.ssl.certificate", "remote-cluster.crt")
            .keystore("xpack.security.remote_cluster_server.ssl.secure_key_passphrase", "remote-cluster-password")
            // .setting("logger.org.elasticsearch.xpack.core", "trace") //useful for human debugging
            // .setting("logger.org.elasticsearch.xpack.security", "trace") //useful for human debugging
            .build();

        queryCluster = ElasticsearchCluster.local()
            .name("query-cluster")
            .distribution(DistributionType.DEFAULT)
            .setting("xpack.ml.enabled", "false")
            .apply(commonClusterConfig)
            .setting("xpack.security.remote_cluster_client.ssl.enabled", "true")
            .setting("xpack.security.remote_cluster_client.ssl.certificate_authorities", "remote-cluster-ca.crt")
            .keystore("cluster.remote.my_remote_cluster.credentials", () -> {
                if (API_KEY_MAP_REF.get() == null) {
                    final Map<String, Object> apiKeyMap = createCrossClusterAccessApiKey("""
                        {
                            "search": [
                              {
                                "names": ["*"]
                              }
                            ]
                        }""");
                    API_KEY_MAP_REF.set(apiKeyMap);
                }
                return (String) API_KEY_MAP_REF.get().get("encoded");
            })
            .build();
    }

    @ClassRule
    // Use a RuleChain to ensure that fulfilling cluster is started before query cluster
    public static TestRule clusterRule = RuleChain.outerRule(fulfillingCluster).around(queryCluster);

    @Override
    protected boolean isRCS2() {
        return true;
    }

    @Before
    @Override
    public void setUp() throws Exception {
        configureRemoteCluster(REMOTE_CLUSTER_ALIAS, fulfillingCluster, false, randomBoolean(), false);
        super.setUp();
    }
}
