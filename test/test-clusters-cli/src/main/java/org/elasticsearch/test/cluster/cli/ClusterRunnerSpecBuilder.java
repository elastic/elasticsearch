/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.cluster.cli;

import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.AbstractLocalClusterSpecBuilder;
import org.elasticsearch.test.cluster.local.DefaultEnvironmentProvider;
import org.elasticsearch.test.cluster.local.DefaultSettingsProvider;
import org.elasticsearch.test.cluster.local.DefaultSystemPropertyProvider;
import org.elasticsearch.test.cluster.local.FipsEnabledClusterConfigProvider;
import org.elasticsearch.test.cluster.local.LocalClusterFactory;
import org.elasticsearch.test.cluster.local.LocalClusterHandle;
import org.elasticsearch.test.cluster.local.LocalClusterSpec;
import org.elasticsearch.test.cluster.local.MultiProjectEnabledClusterConfigProvider;
import org.elasticsearch.test.cluster.util.resource.Resource;

/**
 * Cluster spec builder used by {@link ClusterRunner}. It is configured exactly like the builder returned by
 * {@link ElasticsearchCluster#local()}, but rather than producing a JUnit rule it creates a {@link LocalClusterHandle} directly,
 * since the runner manages the cluster lifecycle itself.
 */
final class ClusterRunnerSpecBuilder extends AbstractLocalClusterSpecBuilder<ElasticsearchCluster> {

    ClusterRunnerSpecBuilder() {
        super();
        this.apply(new FipsEnabledClusterConfigProvider());
        this.apply(new MultiProjectEnabledClusterConfigProvider());
        this.systemProperties(new DefaultSystemPropertyProvider());
        this.settings(new DefaultSettingsProvider());
        this.environment(new DefaultEnvironmentProvider());
        this.rolesFile(Resource.fromClasspath("default_test_roles.yml"));
    }

    <H extends LocalClusterHandle> H createHandle(LocalClusterFactory<LocalClusterSpec, H> factory) {
        return factory.create(buildClusterSpec());
    }

    @Override
    public ElasticsearchCluster build() {
        throw new UnsupportedOperationException("The cluster runner does not create JUnit rules, use createHandle() instead");
    }
}
