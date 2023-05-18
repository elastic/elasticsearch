/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.cluster.local;

import org.elasticsearch.test.cluster.DefaultElasticsearchCluster;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.LocalDistributionResolver;
import org.elasticsearch.test.cluster.local.distribution.ReleasedDistributionResolver;
import org.elasticsearch.test.cluster.local.distribution.SnapshotDistributionResolver;
import org.elasticsearch.test.cluster.util.resource.Resource;

public class DefaultLocalClusterSpecBuilder extends AbstractLocalClusterSpecBuilder<ElasticsearchCluster> {

    public DefaultLocalClusterSpecBuilder() {
        super();
        this.apply(new FipsEnabledClusterConfigProvider());
        this.settings(new DefaultSettingsProvider());
        this.environment(new DefaultEnvironmentProvider());
        this.rolesFile(Resource.fromClasspath("default_test_roles.yml"));
    }

    @Override
    public ElasticsearchCluster build() {
        return new DefaultElasticsearchCluster<>(
            this::buildClusterSpec,
            new LocalClusterFactory(new LocalDistributionResolver(new SnapshotDistributionResolver(new ReleasedDistributionResolver())))
        );
    }
}
