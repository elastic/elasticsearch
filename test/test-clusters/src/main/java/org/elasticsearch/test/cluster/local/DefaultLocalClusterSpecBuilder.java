/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.cluster.local;

import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.LocalDistributionResolver;
import org.elasticsearch.test.cluster.local.distribution.ReleasedDistributionResolver;
import org.elasticsearch.test.cluster.local.distribution.SnapshotDistributionResolver;
import org.elasticsearch.test.cluster.util.resource.Resource;

public final class DefaultLocalClusterSpecBuilder extends AbstractLocalClusterSpecBuilder<ElasticsearchCluster> {

    public DefaultLocalClusterSpecBuilder() {
        super();
        this.apply(
            c -> c.systemProperty("ingest.geoip.downloader.enabled.default", "false").systemProperty("tests.testfeatures.enabled", "true")
        );
        this.apply(new FipsEnabledClusterConfigProvider());
        this.settings(new DefaultSettingsProvider());
        this.environment(new DefaultEnvironmentProvider());
        this.rolesFile(Resource.fromClasspath("default_test_roles.yml"));
    }

    @Override
    public ElasticsearchCluster build() {
        return new DefaultLocalElasticsearchCluster<>(
            this::buildClusterSpec,
            new DefaultLocalClusterFactory(
                new LocalDistributionResolver(new SnapshotDistributionResolver(new ReleasedDistributionResolver()))
            )
        );
    }

}
