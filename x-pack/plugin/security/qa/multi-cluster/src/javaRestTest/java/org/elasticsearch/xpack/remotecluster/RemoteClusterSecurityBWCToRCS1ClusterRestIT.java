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
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

/**
 * BWC test which ensures that users and API keys with defined {@code remote_indices}/{@code remote_cluster} privileges can be used
 * to query legacy remote clusters when using RCS 1.0. We send the request the to an older fulfilling cluster using RCS 1.0 with a user/role
 * and API key where the {@code remote_indices}/{@code remote_cluster} are defined in the newer query cluster.
 * All RCS 2.0 config should be effectively ignored when using RCS 1 for CCS. We send to an elder fulfil cluster to help ensure that
 * newly introduced RCS 2.0 artifacts are forward compatible from the perspective of the old cluster. For example, a new privilege
 * sent to an old cluster should be ignored.
 */
public class RemoteClusterSecurityBWCToRCS1ClusterRestIT extends AbstractRemoteClusterSecurityBWCRestIT {

    private static final Version OLD_CLUSTER_VERSION = Version.fromString(System.getProperty("tests.old_cluster_version"));

    static {
        fulfillingCluster = ElasticsearchCluster.local()
            .version(OLD_CLUSTER_VERSION)
            .distribution(DistributionType.DEFAULT)
            .name("fulfilling-cluster")
            .apply(commonClusterConfig)
            .setting("xpack.ml.enabled", "false")
            // .setting("logger.org.elasticsearch.xpack.core", "trace") //useful for human debugging
            // .setting("logger.org.elasticsearch.xpack.security", "trace") //useful for human debugging
            .build();

        queryCluster = ElasticsearchCluster.local()
            .version(Version.CURRENT)
            .distribution(DistributionType.DEFAULT)
            .setting("xpack.ml.enabled", "false")
            .name("query-cluster")
            .apply(commonClusterConfig)
            .setting("xpack.security.remote_cluster_client.ssl.enabled", "true")
            .setting("xpack.security.remote_cluster_client.ssl.certificate_authorities", "remote-cluster-ca.crt")
            .rolesFile(Resource.fromClasspath("roles.yml"))
            .build();
    }

    @ClassRule
    // Use a RuleChain to ensure that fulfilling cluster is started before query cluster
    public static TestRule clusterRule = RuleChain.outerRule(fulfillingCluster).around(queryCluster);

    @Override
    protected boolean isRCS2() {
        return false;
    }

    @Before
    @Override
    public void setUp() throws Exception {
        configureRemoteCluster(REMOTE_CLUSTER_ALIAS, fulfillingCluster, true, randomBoolean(), false);
        super.setUp();
    }
}
