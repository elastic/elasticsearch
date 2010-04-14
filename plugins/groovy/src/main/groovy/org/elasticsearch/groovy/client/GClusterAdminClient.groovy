package org.elasticsearch.groovy.client

import org.elasticsearch.client.ClusterAdminClient
import org.elasticsearch.client.internal.InternalClient

/**
 * @author kimchy (shay.banon)
 */
class GClusterAdminClient {

    private final GClient gClient

    private final InternalClient internalClient;

    final ClusterAdminClient clusterAdminClient;

    def GClusterAdminClient(gClient) {
        this.gClient = gClient;
        this.internalClient = gClient.client;
        this.clusterAdminClient = internalClient.admin().cluster();
    }
}
