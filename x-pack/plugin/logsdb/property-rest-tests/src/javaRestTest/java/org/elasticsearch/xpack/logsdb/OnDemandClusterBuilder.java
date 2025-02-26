/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb;

import org.elasticsearch.test.cluster.local.AbstractLocalClusterSpecBuilder;
import org.elasticsearch.test.cluster.local.DefaultEnvironmentProvider;
import org.elasticsearch.test.cluster.local.DefaultLocalClusterFactory;
import org.elasticsearch.test.cluster.local.DefaultLocalElasticsearchCluster;
import org.elasticsearch.test.cluster.local.DefaultSettingsProvider;
import org.elasticsearch.test.cluster.local.LocalClusterFactory;
import org.elasticsearch.test.cluster.local.LocalClusterHandle;
import org.elasticsearch.test.cluster.local.LocalClusterSpec;
import org.elasticsearch.test.cluster.local.distribution.LocalDistributionResolver;
import org.elasticsearch.test.cluster.local.distribution.ReleasedDistributionResolver;
import org.elasticsearch.test.cluster.local.distribution.SnapshotDistributionResolver;

import java.util.function.Supplier;

public class OnDemandClusterBuilder extends AbstractLocalClusterSpecBuilder<OnDemandLocalCluster> {
    public OnDemandClusterBuilder() {
        this.settings(new DefaultSettingsProvider());
        this.environment(new DefaultEnvironmentProvider());
    }

    @Override
    public OnDemandLocalCluster build() {
        return new ElasticSearchClusterWrapper<>(
            this::buildClusterSpec,
            new DefaultLocalClusterFactory(
                new LocalDistributionResolver(new SnapshotDistributionResolver(new ReleasedDistributionResolver()))
            )
        );
    }

    private static class ElasticSearchClusterWrapper<S extends LocalClusterSpec, H extends LocalClusterHandle> extends
        DefaultLocalElasticsearchCluster<S, H>
        implements
            OnDemandLocalCluster {
        private final Supplier<S> specProvider;
        private final LocalClusterFactory<S, H> clusterFactory;

        ElasticSearchClusterWrapper(Supplier<S> specProvider, LocalClusterFactory<S, H> clusterFactory) {
            super(specProvider, clusterFactory);
            this.specProvider = specProvider;
            this.clusterFactory = clusterFactory;
        }

        @Override
        public void init() {
            S spec = specProvider.get();
            if (spec.isShared() == false || handle == null) {
                handle = clusterFactory.create(spec);
                handle.start();
            }
        }

        @Override
        public void teardown() {
            S spec = specProvider.get();
            if (spec.isShared() == false) {
                close();
            }
        }
    }
}
