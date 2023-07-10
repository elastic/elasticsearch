/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.cluster;

import org.elasticsearch.test.cluster.util.Version;

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
     * Stops the node at a given index.
     * @param index of the node to stop
     */
    void stopNode(int index);

    /**
     * Restarts the cluster. Effectively the same as calling {@link #stop(boolean)} followed by {@link #start()}
     *
     * @param forcibly whether to ficibly terminate the cluster
     */
    void restart(boolean forcibly);

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
     * Get the name of the node for the given index.
     */
    String getName(int index);

    /**
     * Returns a comma-separated list of TCP transport endpoints for cluster. If this method is called on an unstarted cluster, the cluster
     * will be started. This method is thread-safe and subsequent calls will wait for cluster start and availability.
     *
     * @return cluster node TCP transport endpoints
     */
    String getTransportEndpoints();

    /**
     * Returns the TCP transport endpoint for the node at the given index. If this method is called on an unstarted cluster, the cluster
     * will be started. This method is thread-safe and subsequent calls will wait for cluster start and availability.
     *
     * @return cluster node TCP transport endpoints
     */
    String getTransportEndpoint(int index);

    /**
     * Returns a comma-separated list of remote cluster server endpoints for cluster. If this method is called on an unstarted cluster,
     * the cluster will be started. This method is thread-safe and subsequent calls will wait for cluster start and availability.
     *
     * @return cluster node remote cluster server endpoints
     */
    String getRemoteClusterServerEndpoint();

    /**
     * Returns the remote cluster server endpoint for the node at the given index. If this method is called on an unstarted cluster,
     * the cluster will be started. This method is thread-safe and subsequent calls will wait for cluster start and availability.
     *
     * @return cluster node remote cluster server endpoints
     */
    String getRemoteClusterServerEndpoint(int index);

    /**
     * Upgrades a single node to the given version. Method blocks until the node is back up and ready to respond to requests.
     *
     * @param index index of node ot upgrade
     * @param version version to upgrade to
     */
    void upgradeNodeToVersion(int index, Version version);

    /**
     * Performs a "full cluster restart" upgrade to the given version. Method blocks until the cluster is restarted and available.
     *
     * @param version version to upgrade to
     */
    void upgradeToVersion(Version version);

    /**
     * Cleans up any resources created by this cluster.
     */
    void close();
}
