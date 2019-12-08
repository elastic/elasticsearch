/*
 * Copyright (c) 2019. Lorem ipsum dolor sit amet, consectetur adipiscing elit.
 * Morbi non lorem porttitor neque feugiat blandit. Ut vitae ipsum eget quam lacinia accumsan.
 * Etiam sed turpis ac ipsum condimentum fringilla. Maecenas magna.
 * Proin dapibus sapien vel ante. Aliquam erat volutpat. Pellentesque sagittis ligula eget metus.
 * Vestibulum commodo. Ut rhoncus gravida arcu.
 */

package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.close.CloseIndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.xpack.core.ilm.AsyncActionStep;

/**
 * Invokes a Close Index Step on a index.
 */
public class CloseIndexStep extends AsyncActionStep {
    public static final String NAME = "close-index";

    CloseIndexStep(StepKey key, StepKey nextStepKey, Client client) {
        super(key, nextStepKey, client);
    }

    @Override
    public void performAction(IndexMetaData indexMetaData, ClusterState currentClusterState, ClusterStateObserver observer, Listener listener) {
        if(indexMetaData.getState() == IndexMetaData.State.OPEN) {
            CloseIndexRequest request = new CloseIndexRequest(indexMetaData.getIndex().getName());
            getClient().admin().indices()
                .close(request, ActionListener.wrap(closeIndexResponse -> listener.onResponse(true), listener::onFailure));
        }
        else {
            listener.onResponse(true);
        }
    }


}
