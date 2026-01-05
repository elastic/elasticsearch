/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.cluster.local;

import org.elasticsearch.test.cluster.ClusterHandle;
import org.elasticsearch.test.cluster.LogType;
import org.elasticsearch.test.cluster.MutableSettingsProvider;
import org.elasticsearch.test.cluster.util.Version;

import java.io.InputStream;
import java.util.List;

public interface LocalClusterHandle extends ClusterHandle {

    /**
     * Returns the number of nodes that are part of this cluster.
     */
    int getNumNodes();

    /**
     * Stops the node at a given index.
     * @param index of the node to stop
     */
    void stopNode(int index, boolean forcibly);

    /**
     * Restarts the cluster. Effectively the same as calling {@link #stop(boolean)} followed by {@link #start()}
     *
     * @param forcibly whether to ficibly terminate the cluster
     */
    void restart(boolean forcibly);

    /**
     * Get the name of the node for the given index.
     */
    String getName(int index);

    /**
     * Get the pid of the node for the given index.
     */
    long getPid(int index);

    /**
     * Returns a comma-separated list of TCP transport endpoints for cluster. If this method is called on an unstarted cluster, the cluster
     * will be started. This method is thread-safe and subsequent calls will wait for cluster start and availability.\
     *
     * @return cluster node TCP transport endpoints
     */
    String getTransportEndpoints();

    /**
     * @return a list of all available TCP transport endpoints, which may be empty if none of the nodes in this cluster are started.
     */
    List<String> getAvailableTransportEndpoints();

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
     * Note individual node can enable or disable remote cluster server independently. When a node has remote cluster server disabled,
     * an empty string is returned for that node. Hence, it is possible for this method to return something like "[::1]:63300,,".
     *
     * @return cluster node remote cluster server endpoints
     */
    String getRemoteClusterServerEndpoints();

    /**
     * Returns the remote cluster server endpoint forx the node at the given index. If this method is called on an unstarted cluster,
     * the cluster will be started. This method is thread-safe and subsequent calls will wait for cluster start and availability.
     * Note individual node can enable or disable remote cluster server independently. When a node has remote cluster server disabled,
     * an empty string is returned.
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
     * Returns an {@link InputStream} for the given node log.
     */
    InputStream getNodeLog(int index, LogType logType);

    /**
     * Writes secure settings to the relevant secure config file on each node. Use this method if you are dynamically updating secure
     * settings via a {@link MutableSettingsProvider} and need the update to be written to file, without a cluster restart.
     *
     * @throws UnsupportedOperationException if secure settings are stored in a secrets file, i.e., in serverless. Only keystore-based
     * storage is currently supported
     */
    void updateStoredSecureSettings();
}
