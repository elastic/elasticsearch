package org.elasticsearch.groovy.client

import org.elasticsearch.client.ClusterAdminClient
import org.elasticsearch.client.internal.InternalClient

/**
 * @author kimchy (shay.banon)
 */
class GClusterAdminClient {

    private final InternalClient internalClient;

    private final ClusterAdminClient clusterAdminClient;

    def GClusterAdminClient(internalClient) {
        this.internalClient = internalClient;
        this.clusterAdminClient = internalClient.admin().cluster();
    }
}
