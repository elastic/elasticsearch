package org.elasticsearch.groovy.client

/**
 * @author kimchy (shay.banon)
 */
class GAdminClient {

    private final GClient gClient;

    final GIndicesAdminClient indices;

    final GClusterAdminClient cluster;

    def GAdminClient(gClient) {
        this.gClient = gClient;

        this.indices = new GIndicesAdminClient(gClient)
        this.cluster = new GClusterAdminClient(gClient)
    }
}
