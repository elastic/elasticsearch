/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.cluster;

import org.elasticsearch.test.cluster.local.LocalClusterSpec;

import java.util.Map;

/**
 * Functional interface for supplying system properties to an Elasticsearch node. This interface is designed to be implemented by tests
 * and fixtures wanting to provide system properties to an {@link ElasticsearchCluster} in a dynamic fashion.
 * Instances are evaluated lazily at cluster start time.
 */
public interface SystemPropertyProvider {

    /**
     * Returns a collection of system properties to apply to an Elasticsearch cluster node. This method is called when the cluster is
     * started so implementors can return dynamic environment values that may or may not be based on the given node spec.
     *
     * @param nodeSpec the specification for the given node to apply settings to
     * @return system property variables to add to the node
     */
    Map<String, String> get(LocalClusterSpec.LocalNodeSpec nodeSpec);
}
