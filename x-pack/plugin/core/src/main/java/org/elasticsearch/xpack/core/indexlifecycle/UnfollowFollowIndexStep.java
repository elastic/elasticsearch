/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.indexlifecycle;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.close.CloseIndexRequest;
import org.elasticsearch.action.admin.indices.open.OpenIndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.xpack.core.ccr.action.PauseFollowAction;
import org.elasticsearch.xpack.core.ccr.action.UnfollowAction;

import java.util.Map;

import static org.elasticsearch.xpack.core.indexlifecycle.UnfollowAction.CCR_METADATA_KEY;

final class UnfollowFollowIndexStep extends AsyncActionStep {

    static final String NAME = "unfollow-index";

    UnfollowFollowIndexStep(StepKey key, StepKey nextStepKey, Client client) {
        super(key, nextStepKey, client);
    }

    @Override
    public void performAction(IndexMetaData indexMetaData, ClusterState currentClusterState, Listener listener) {
        String followerIndex = indexMetaData.getIndex().getName();
        Map<String, String> customIndexMetadata = indexMetaData.getCustomData(CCR_METADATA_KEY);
        if (customIndexMetadata == null) {
            listener.onResponse(true);
            return;
        }

        pauseFollowerIndex(followerIndex, listener);
    }

    void pauseFollowerIndex(final String followerIndex, final Listener listener) {
        PauseFollowAction.Request request = new PauseFollowAction.Request(followerIndex);
        getClient().execute(PauseFollowAction.INSTANCE, request, ActionListener.wrap(
            r -> {
                assert r.isAcknowledged() : "pause follow response is not acknowledge";
                closeFollowerIndex(followerIndex, listener);
            },
            listener::onFailure
        ));
    }

    void closeFollowerIndex(final String followerIndex, final Listener listener) {
        CloseIndexRequest closeIndexRequest = new CloseIndexRequest(followerIndex);
        getClient().admin().indices().close(closeIndexRequest, ActionListener.wrap(
            r -> {
                assert r.isAcknowledged() : "close index response is not acknowledge";
                unfollow(followerIndex, listener);
            },
            listener::onFailure)
        );
    }

    void unfollow(final String followerIndex, final Listener listener) {
        UnfollowAction.Request request = new UnfollowAction.Request(followerIndex);
        getClient().execute(UnfollowAction.INSTANCE, request, ActionListener.wrap(
            r -> {
                assert r.isAcknowledged() : "unfollow response is not acknowledge";
                openIndex(followerIndex, listener);
            },
            listener::onFailure
        ));
    }

    void openIndex(final String index, final Listener listener) {
        OpenIndexRequest request = new OpenIndexRequest(index);
        getClient().admin().indices().open(request, ActionListener.wrap(
            r -> {
                assert r.isAcknowledged() :  "open index response is not acknowledge";
                listener.onResponse(true);
            },
            listener::onFailure
        ));
    }

}
