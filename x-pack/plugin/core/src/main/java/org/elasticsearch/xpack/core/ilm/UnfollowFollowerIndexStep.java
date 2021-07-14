/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.core.ccr.action.UnfollowAction;

import java.util.List;

final class UnfollowFollowerIndexStep extends AbstractUnfollowIndexStep {
    private static final Logger logger = LogManager.getLogger(UnfollowFollowerIndexStep.class);

    static final String NAME = "unfollow-follower-index";

    UnfollowFollowerIndexStep(StepKey key, StepKey nextStepKey, Client client) {
        super(key, nextStepKey, client);
    }

    @Override
    public boolean isRetryable() {
        return true;
    }

    @Override
    void innerPerformAction(String followerIndex, ClusterState currentClusterState, ActionListener<Boolean> listener) {
        UnfollowAction.Request request = new UnfollowAction.Request(followerIndex).masterNodeTimeout(TimeValue.MAX_VALUE);
        getClient().execute(UnfollowAction.INSTANCE, request, ActionListener.wrap(
            r -> {
                if (r.isAcknowledged() == false) {
                    throw new ElasticsearchException("unfollow request failed to be acknowledged");
                }
                listener.onResponse(true);
            },
            exception -> {
                if (exception instanceof ElasticsearchException
                        && ((ElasticsearchException) exception).getMetadata("es.failed_to_remove_retention_leases") != null) {
                    List<String> leasesNotRemoved = ((ElasticsearchException) exception)
                        .getMetadata("es.failed_to_remove_retention_leases");
                    logger.debug("failed to remove leader retention lease(s) {} while unfollowing index [{}], " +
                            "continuing with lifecycle execution",
                        leasesNotRemoved, followerIndex);
                    listener.onResponse(true);
                } else {
                    listener.onFailure(exception);
                }
            }
        ));
    }

}
