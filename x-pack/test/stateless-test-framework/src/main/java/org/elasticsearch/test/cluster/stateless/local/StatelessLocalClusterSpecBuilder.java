/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.test.cluster.stateless.local;

import org.elasticsearch.test.cluster.local.AbstractLocalClusterSpecBuilder;
import org.elasticsearch.test.cluster.local.DefaultEnvironmentProvider;
import org.elasticsearch.test.cluster.local.distribution.LocalDistributionResolver;
import org.elasticsearch.test.cluster.local.distribution.ReleasedDistributionResolver;
import org.elasticsearch.test.cluster.local.distribution.SnapshotDistributionResolver;
import org.elasticsearch.test.cluster.stateless.StatelessElasticsearchCluster;
import org.elasticsearch.test.cluster.stateless.distribution.StatelessDistributionResolver;
import org.elasticsearch.test.cluster.util.resource.Resource;

public class StatelessLocalClusterSpecBuilder extends AbstractLocalClusterSpecBuilder<StatelessElasticsearchCluster> {

    @SuppressWarnings("this-escape")
    public StatelessLocalClusterSpecBuilder(boolean addDefaultNodes) {
        this.settings(new DefaultStatelessSettingsProvider());
        this.systemProperties(new DefaultStatelessSystemPropertiesProvider());
        this.environment(new DefaultEnvironmentProvider());
        this.apply(new DefaultStatelessLocalConfigProvider(addDefaultNodes));
        this.rolesFile(Resource.fromClasspath("default_test_roles.yml"));
    }

    @Override
    public StatelessElasticsearchCluster build() {
        return new DefaultLocalStatelessElasticsearchCluster(
            this::buildClusterSpec,
            new StatelessLocalClusterFactory(
                new StatelessDistributionResolver(
                    new LocalDistributionResolver(new SnapshotDistributionResolver(new ReleasedDistributionResolver()))
                )
            )
        );
    }
}
