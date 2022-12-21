/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.cluster.local;

import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.util.resource.TextResource;

import java.util.function.Consumer;

public interface LocalClusterSpecBuilder extends LocalSpecBuilder<LocalClusterSpecBuilder> {
    /**
     * Sets the node name. By default, "test-cluster" is used.
     */
    LocalClusterSpecBuilder name(String name);

    LocalClusterSpecBuilder apply(LocalClusterConfigProvider configProvider);

    /**
     * Sets the number of nodes for the cluster.
     */
    LocalClusterSpecBuilder nodes(int nodes);

    /**
     * Adds a new node to the cluster and configures the node.
     */
    LocalClusterSpecBuilder withNode(Consumer<? super LocalNodeSpecBuilder> config);

    /**
     * Configures an existing node.
     *
     * @param index the index of the node to configure
     * @param config configuration to apply to the node
     */
    LocalClusterSpecBuilder node(int index, Consumer<? super LocalNodeSpecBuilder> config);

    /**
     * Register a user using the default test role.
     */
    LocalClusterSpecBuilder user(String username, String password);

    /**
     * Register a user using the given role.
     */
    LocalClusterSpecBuilder user(String username, String password, String role);

    /**
     * Register a roles file with cluster via the supplied {@link TextResource}.
     */
    LocalClusterSpecBuilder rolesFile(TextResource rolesFile);

    ElasticsearchCluster build();
}
