package org.elasticsearch.groovy.client

import org.elasticsearch.client.internal.InternalClient

/**
 * @author kimchy (shay.banon)
 */
class GAdminClient {

    private final InternalClient internalClient;

    final GIndicesAdminClient indices;

    final GClusterAdminClient cluster;

    def GAdminClient(internalClient) {
        this.internalClient = internalClient;

        this.indices = new GIndicesAdminClient(internalClient)
        this.cluster = new GClusterAdminClient(internalClient)
    }
}
