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

/**
 * The ES|QL fetch phase over a remote cluster under the certificate security model. The querying cluster connects in proxy
 * mode to the gateway node, and the fulfilling cluster authorizes each request with the roles of the same name it defines.
 */
public class RemoteClusterSecurityEsqlFetchRcs1IT extends AbstractRemoteClusterSecurityEsqlFetchIT {
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
            .setting("xpack.ml.enabled", "false")
            // frees an expired context within a second, for the contexts that a user who lost read can't free
            .setting("search.keep_alive_interval", "1s")
            .setting("xpack.security.audit.enabled", "true")
            .setting("xpack.security.audit.logfile.events.include", "[\"access_granted\", \"access_denied\"]")
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
            .build();
    }

    @ClassRule
    public static TestRule clusterRule = RuleChain.outerRule(fulfillingCluster).around(queryCluster);

    @Override
    protected void configureRemoteClusterThroughTheGateway() throws Exception {
        // the proxy address is the gateway node, the first node of the fulfilling cluster
        configureRemoteCluster(REMOTE_CLUSTER_ALIAS, fulfillingCluster, true, true, false);
    }

    @Override
    protected void putRole(String... indexPrivileges) throws IOException {
        String role = "{\"indices\": [" + String.join(", ", indexPrivileges) + "]}";
        Request onTheFulfillingCluster = new Request("PUT", "/_security/role/" + ROLE);
        onTheFulfillingCluster.setJsonEntity(role);
        assertOK(performRequestAgainstFulfillingCluster(onTheFulfillingCluster));
        // the querying cluster resolves the role of the user by name too, and its index patterns match no local index
        Request onTheQueryingCluster = new Request("PUT", "/_security/role/" + ROLE);
        onTheQueryingCluster.setJsonEntity(role);
        assertOK(adminClient().performRequest(onTheQueryingCluster));
    }
}
