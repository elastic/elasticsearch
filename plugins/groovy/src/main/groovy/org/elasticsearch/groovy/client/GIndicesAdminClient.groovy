package org.elasticsearch.groovy.client

import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse
import org.elasticsearch.client.IndicesAdminClient
import org.elasticsearch.client.internal.InternalClient
import org.elasticsearch.groovy.client.action.GActionFuture

/**
 * @author kimchy (shay.banon)
 */
class GIndicesAdminClient {

    private final GClient gClient

    private final InternalClient internalClient;

    final IndicesAdminClient indicesAdminClient;

    def GIndicesAdminClient(gClient) {
        this.gClient = gClient
        this.internalClient = gClient.client
        this.indicesAdminClient = internalClient.admin().indices();
    }

    GActionFuture<RefreshResponse> refresh(Closure c) {
        RefreshRequest request = new RefreshRequest()
        c.setDelegate request
        c.resolveStrategy = Closure.DELEGATE_FIRST
        c.call()
        refresh(request)
    }

    GActionFuture<RefreshResponse> refresh(RefreshRequest request) {
        GActionFuture<RefreshResponse> future = new GActionFuture<RefreshResponse>(internalClient.threadPool(), request);
        indicesAdminClient.refresh(request, future)
        return future
    }

    void refresh(RefreshRequest request, ActionListener<RefreshResponse> listener) {
        indicesAdminClient.refresh(request, listener)
    }
}
