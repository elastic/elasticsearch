/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.cluster;

import org.elasticsearch.test.cluster.local.LocalClusterSpec.LocalNodeSpec;

import java.util.Map;

/**
 * Functional interface for supplying environment variables to an Elasticsearch node. This interface is designed to be implemented by tests
 * and fixtures wanting to provide settings to an {@link ElasticsearchCluster} in a dynamic fashion. Instances are evaluated lazily at
 * cluster start time.
 */
public interface EnvironmentProvider {

    /**
     * Returns a collection of environment variables to apply to an Elasticsearch cluster node. This method is called when the cluster is
     * started so implementors can return dynamic environment values that may or may not be based on the given node spec.
     *
     * @param nodeSpec the specification for the given node to apply settings to
     * @return environment variables to add to the node
     */
    Map<String, String> get(LocalNodeSpec nodeSpec);
}
