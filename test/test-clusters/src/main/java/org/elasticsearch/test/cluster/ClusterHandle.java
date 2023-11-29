/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.cluster;

import java.io.Closeable;

/**
 * A handle to an {@link ElasticsearchCluster}.
 */
public interface ClusterHandle extends Closeable {
    /**
     * Starts the cluster. This method will block until all nodes are started and cluster is ready to serve requests.
     */
    void start();

    /**
     * Stops the cluster. This method will block until all cluster node processes have exited. This method is thread-safe and subsequent
     * calls will wait for the exiting termination to complete.
     *
     * @param forcibly whether to forcibly terminate the cluster
     */
    void stop(boolean forcibly);

    /**
     * Whether the cluster is started or not. This method makes no guarantees on cluster availability, only that the node processes have
     * been started.
     *
     * @return whether the cluster has been started
     */
    boolean isStarted();

    /**
     * Returns a comma-separated list of HTTP transport endpoints for cluster. If this method is called on an unstarted cluster, the cluster
     * will be started. This method is thread-safe and subsequent calls will wait for cluster start and availability.
     *
     * @return cluster node HTTP transport addresses
     */
    String getHttpAddresses();

    /**
     * Returns the HTTP transport endpoint for the node at the given index. If this method is called on an unstarted cluster, the cluster
     * will be started. This method is thread-safe and subsequent calls will wait for cluster start and availability.
     *
     * @return cluster node HTTP transport addresses
     */
    String getHttpAddress(int index);

    /**
     * Cleans up any resources created by this cluster. Calling this method will forcibly terminate any running nodes.
     */
    void close();

}
