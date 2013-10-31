package org.elasticsearch.cluster.ack;

/**
 * Listener used for cluster state updates processing
 * Supports acknowledgement logic
 */
public interface ClusterStateUpdateListener<UpdateResponse extends ClusterStateUpdateResponse> {

    /**
     * Called when the cluster state update is acknowledged
     */
    void onResponse(UpdateResponse response);

    /**
     * Called when any error is thrown during the cluster state update processing
     */
    void onFailure(Throwable t);
}
