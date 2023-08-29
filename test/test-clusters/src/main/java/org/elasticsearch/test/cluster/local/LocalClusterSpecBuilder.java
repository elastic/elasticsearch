/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.cluster.local;

import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.util.resource.Resource;

import java.util.function.Consumer;
import java.util.function.Supplier;

public interface LocalClusterSpecBuilder<T extends ElasticsearchCluster> extends LocalSpecBuilder<LocalClusterSpecBuilder<T>> {
    /**
     * Sets the node name. By default, "test-cluster" is used.
     */
    LocalClusterSpecBuilder<T> name(String name);

    /**
     * Apply configuration from a {@link LocalClusterConfigProvider}. This configuration is applied eagerly. Subsequent calls to this
     * builder will override provider settings.
     */
    LocalClusterSpecBuilder<T> apply(LocalClusterConfigProvider configProvider);

    /**
     * Apply configuration from a {@link LocalClusterConfigProvider} created by the given {@link Supplier}. This configuration is applied
     * lazily and will override existing builder settings.
     */
    LocalClusterSpecBuilder<T> apply(Supplier<LocalClusterConfigProvider> configProvider);

    /**
     * Sets the number of nodes for the cluster.
     */
    LocalClusterSpecBuilder<T> nodes(int nodes);

    /**
     * Adds a new node to the cluster and configures the node.
     */
    LocalClusterSpecBuilder<T> withNode(Consumer<? super LocalNodeSpecBuilder> config);

    /**
     * Configures an existing node.
     *
     * @param index the index of the node to configure
     * @param config configuration to apply to the node
     */
    LocalClusterSpecBuilder<T> node(int index, Consumer<? super LocalNodeSpecBuilder> config);

    /**
     * Register a user using the default test role, as an operator
     */
    LocalClusterSpecBuilder<T> user(String username, String password);

    /**
     * Register a user using the given role.
     * @param operator If true, configure the user as an operator.
     *                 <em>Note</em>: This does <strong>not</strong> automatically enable operator privileges on the cluster
     */
    LocalClusterSpecBuilder<T> user(String username, String password, String role, boolean operator);

    /**
     * Register a roles file with cluster via the supplied {@link Resource}.
     */
    LocalClusterSpecBuilder<T> rolesFile(Resource rolesFile);

    T build();
}
