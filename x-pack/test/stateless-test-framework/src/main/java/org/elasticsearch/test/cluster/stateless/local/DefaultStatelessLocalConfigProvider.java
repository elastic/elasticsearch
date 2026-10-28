/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.test.cluster.stateless.local;

import org.elasticsearch.test.cluster.FeatureFlag;
import org.elasticsearch.test.cluster.local.LocalClusterConfigProvider;
import org.elasticsearch.test.cluster.local.LocalClusterSpecBuilder;
import org.elasticsearch.test.cluster.local.LocalNodeSpecBuilder;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;

import java.util.function.Consumer;

/**
 * Default configuration applied to all stateless clusters.
 */
public class DefaultStatelessLocalConfigProvider implements LocalClusterConfigProvider {
    private final boolean addDefaultNodes;

    public DefaultStatelessLocalConfigProvider(boolean addDefaultNodes) {
        this.addDefaultNodes = addDefaultNodes;
    }

    @Override
    public void apply(LocalClusterSpecBuilder<?> builder) {
        // Stateless self-managed tests run against the INTEG_TEST distribution plus explicit modules per test
        // (rather than the DEFAULT distribution) because:
        // 1. Until the stateless code is moved to the public elasticsearch repo, the upstream DEFAULT
        // distribution does not include all plugins that ship with our serverless distribution.
        // 2. Pinning modules explicitly keeps the task graph small and stable: test inputs don't change
        // every time an unrelated plugin's code changes.
        // 3. Each test's required modules are declared in the build, making module dependencies explicit
        // instead of implicitly pulling in "everything".
        builder.distribution(DistributionType.INTEG_TEST)
            .keystore("bootstrap.password", "x-pack-test-password")
            .setting("stateless.enabled", "true")
            .setting("stateless.object_store.type", "fs")
            .setting("stateless.object_store.bucket", "stateless")
            .setting("stateless.object_store.base_path", "base_path")
            .feature(FeatureFlag.TIME_SERIES_MODE);

        if (addDefaultNodes) {
            builder.withNode(node("index", "[master,remote_cluster_client,ingest,index]"))
                .withNode(node("search", "[remote_cluster_client,search]"));
        }
    }

    public static Consumer<? super LocalNodeSpecBuilder> node(String name, String nodeRoles) {
        return indexNodeSpec -> indexNodeSpec.name(name)
            .setting("node.roles", nodeRoles)
            .setting("xpack.searchable.snapshot.shared_cache.size", "16MB")
            .setting("xpack.searchable.snapshot.shared_cache.region_size", "256KB")
            .setting("stateless.translog.flush.interval", "20ms");
    }
}
