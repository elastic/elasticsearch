/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.cluster.util.resource.Resource;

/**
 * The single 3-node cluster shared by every IT class in this project, whether it talks to the cluster over the
 * transport client (see {@link MlNativeIntegTestCase}) or the REST client ({@code ESRestTestCase} subclasses).
 */
public abstract class Clusters {

    // Name must match the transport client's discovery.seed_hosts / cluster.name handshake in
    // MlNativeIntegTestCase#buildTestCluster.
    protected static final String CLUSTER_NAME = "native-multi-node-tests";

    public static final ElasticsearchCluster CLUSTER = ElasticsearchCluster.local()
        .name(CLUSTER_NAME)
        .distribution(DistributionType.DEFAULT)
        .nodes(3)
        .setting("xpack.security.enabled", "true")
        .setting("xpack.ml.enabled", "true")
        .setting("xpack.watcher.enabled", "false")
        .setting("xpack.security.authc.token.enabled", "true")
        .setting("xpack.security.transport.ssl.enabled", "true")
        .setting("xpack.security.transport.ssl.key", "testnode.pem")
        .setting("xpack.security.transport.ssl.certificate", "testnode.crt")
        .setting("xpack.security.transport.ssl.verification_mode", "certificate")
        .setting("xpack.security.audit.enabled", "false")
        .setting("xpack.license.self_generated.type", "trial")
        .setting("xpack.ml.min_disk_space_off_heap", "200mb")
        .setting("indices.lifecycle.history_index_enabled", "false")
        .setting("slm.history_index_enabled", "false")
        .setting("stack.templates.enabled", "false")
        .setting("xpack.ent_search.enabled", "false")
        .setting("xpack.apm_data.enabled", "false")
        .setting("xpack.otel_data.registry.enabled", "false")
        .setting("xpack.prometheus.registry.enabled", "false")
        .setting("xpack.stack.querylog.registry.enabled", "false")
        // MlNativeIntegTestCase joins this cluster with a transport MockNode built from a curated
        // nodePlugins() list, not the full DEFAULT distribution's plugin set. ElasticsearchCluster nodes
        // enable test features by default (DefaultSystemPropertyProvider), which would make them advertise
        // test-only NodeFeatures from plugins (rank-vectors, prometheus, ingest-attachment, ...) that
        // MockNode doesn't load, tripping the node-join feature barrier. Disable test features here to
        // match the transport client's feature set.
        .systemProperty("tests.testfeatures.enabled", "false")
        // To spice things up a bit, one of the nodes is not an ML node
        .node(0, spec -> spec.setting("node.roles", "[\"master\",\"data\",\"ingest\"]"))
        .node(1, spec -> spec.setting("node.roles", "[\"master\",\"data\",\"ingest\",\"ml\"]"))
        .node(2, spec -> spec.setting("node.roles", "[\"master\",\"data\",\"ingest\",\"ml\"]"))
        .keystore("bootstrap.password", "x-pack-test-password")
        .keystore("xpack.security.transport.ssl.secure_key_passphrase", "testnode")
        .user("x_pack_rest_user", "x-pack-test-password")
        .configFile("testnode.pem", Resource.fromClasspath("testnode.pem"))
        .configFile("testnode.crt", Resource.fromClasspath("testnode.crt"))
        .shared(true)
        .build();

    private Clusters() {}
}
