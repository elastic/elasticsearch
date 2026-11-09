/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.remotecluster;

import org.elasticsearch.client.Request;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The ES|QL fetch phase over a remote cluster under the API key security model. Only the gateway node of the fulfilling
 * cluster runs the remote cluster server, so the querying cluster reaches the other nodes through it in either connection
 * mode. The fulfilling cluster authorizes each request with the remote indices privileges of the role of the user on the
 * querying cluster, within the access of the cross-cluster API key.
 */
public class RemoteClusterSecurityEsqlFetchRcs2IT extends AbstractRemoteClusterSecurityEsqlFetchIT {
    private static final AtomicReference<Map<String, Object>> API_KEY_MAP_REF = new AtomicReference<>();

    static {
        fulfillingCluster = ElasticsearchCluster.local()
            .name(FULFILLING_CLUSTER_NAME)
            .nodes(3)
            .module("x-pack-autoscaling")
            .module("x-pack-esql")
            .module("x-pack-enrich")
            .module("x-pack-ml")
            .module("ingest-common")
            .module("test-error-query")
            .apply(commonClusterConfig)
            .setting("remote_cluster.port", "0")
            .setting("xpack.ml.enabled", "false")
            .setting("xpack.security.remote_cluster_server.ssl.enabled", "true")
            .setting("xpack.security.remote_cluster_server.ssl.key", "remote-cluster.key")
            .setting("xpack.security.remote_cluster_server.ssl.certificate", "remote-cluster.crt")
            .keystore("xpack.security.remote_cluster_server.ssl.secure_key_passphrase", "remote-cluster-password")
            // frees an expired context within a second, for the contexts that a user who lost read can't free
            .setting("search.keep_alive_interval", "1s")
            .setting("xpack.security.audit.enabled", "true")
            .setting("xpack.security.audit.logfile.events.include", "[\"access_granted\", \"access_denied\"]")
            .node(GATEWAY_NODE_INDEX, spec -> spec.setting("remote_cluster_server.enabled", "true"))
            .node(DATA_NODE_INDEX, spec -> spec.setting("remote_cluster_server.enabled", "false"))
            .node(SLOW_NODE_INDEX, spec -> spec.setting("remote_cluster_server.enabled", "false"))
            .build();

        queryCluster = ElasticsearchCluster.local()
            .name("query-cluster")
            .module("x-pack-autoscaling")
            .module("x-pack-esql")
            .module("x-pack-enrich")
            .module("x-pack-ml")
            .module("ingest-common")
            .module("test-error-query")
            .apply(commonClusterConfig)
            .setting("xpack.ml.enabled", "false")
            .setting("xpack.security.remote_cluster_client.ssl.enabled", "true")
            .setting("xpack.security.remote_cluster_client.ssl.certificate_authorities", "remote-cluster-ca.crt")
            .keystore("cluster.remote." + REMOTE_CLUSTER_ALIAS + ".credentials", () -> {
                if (API_KEY_MAP_REF.get() == null) {
                    API_KEY_MAP_REF.set(createCrossClusterAccessApiKey("""
                        {"search": [{"names": ["fetch-*"]}]}"""));
                }
                return (String) API_KEY_MAP_REF.get().get("encoded");
            })
            .build();
    }

    @ClassRule
    public static TestRule clusterRule = RuleChain.outerRule(fulfillingCluster).around(queryCluster);

    @Override
    protected void configureRemoteClusterThroughTheGateway() throws Exception {
        // in either mode, the querying cluster connects to the gateway node only, the one that runs the remote cluster server
        configureRemoteCluster(REMOTE_CLUSTER_ALIAS, fulfillingCluster, false, randomBoolean(), false);
    }

    @Override
    protected void putRole(String... indexPrivileges) throws IOException {
        String[] remoteIndices = Arrays.stream(indexPrivileges)
            .map(privileges -> "{\"clusters\": [\"" + REMOTE_CLUSTER_ALIAS + "\"], " + privileges.substring(1))
            .toArray(String[]::new);
        // the same privileges on the querying cluster read its slow index, and authorize the query there
        Request request = new Request("PUT", "/_security/role/" + ROLE);
        request.setJsonEntity(
            "{\"indices\": [" + String.join(", ", indexPrivileges) + "], \"remote_indices\": [" + String.join(", ", remoteIndices) + "]}"
        );
        assertOK(adminClient().performRequest(request));
    }
}
