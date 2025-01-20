/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.cluster;

import org.elasticsearch.test.cluster.local.DefaultLocalClusterSpecBuilder;
import org.elasticsearch.test.cluster.local.LocalClusterHandle;
import org.elasticsearch.test.cluster.local.LocalClusterSpecBuilder;
import org.junit.rules.TestRule;

import java.util.List;
import java.util.ServiceLoader;
import java.util.stream.Collectors;

/**
 * <p>A JUnit test rule for orchestrating an Elasticsearch cluster for local integration testing. New clusters can be created via one of the
 * various static builder methods. For example:</p>
 * <pre>
 * &#064;ClassRule
 * public static ElasticsearchCluster myCluster = ElasticsearchCluster.local().build();
 * </pre>
 */
public interface ElasticsearchCluster extends TestRule, LocalClusterHandle {

    /**
     * Creates a new {@link LocalClusterSpecBuilder} for defining a locally orchestrated cluster. Local clusters use a locally built
     * Elasticsearch distribution.
     *
     * @return a builder for a local cluster
     */
    static LocalClusterSpecBuilder<ElasticsearchCluster> local() {
        return locateBuilderImpl();
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private static LocalClusterSpecBuilder<ElasticsearchCluster> locateBuilderImpl() {
        ServiceLoader<LocalClusterSpecBuilder> loader = ServiceLoader.load(LocalClusterSpecBuilder.class);
        List<ServiceLoader.Provider<LocalClusterSpecBuilder>> providers = loader.stream().toList();

        if (providers.isEmpty()) {
            return new DefaultLocalClusterSpecBuilder();
        } else if (providers.size() > 1) {
            String providerTypes = providers.stream().map(p -> p.type().getName()).collect(Collectors.joining(","));
            throw new IllegalStateException("Located multiple LocalClusterSpecBuilder providers [" + providerTypes + "]");
        }

        return providers.get(0).get();
    }
}
