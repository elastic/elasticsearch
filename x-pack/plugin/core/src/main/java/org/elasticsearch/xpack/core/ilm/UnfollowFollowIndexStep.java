/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ilm;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.xpack.core.ccr.action.UnfollowAction;

import java.util.List;

final class UnfollowFollowIndexStep extends AbstractUnfollowIndexStep {
    private static final Logger logger = LogManager.getLogger(UnfollowFollowIndexStep.class);

    static final String NAME = "unfollow-follower-index";

    UnfollowFollowIndexStep(StepKey key, StepKey nextStepKey, Client client) {
        super(key, nextStepKey, client);
    }

    @Override
    void innerPerformAction(String followerIndex, Listener listener) {
        UnfollowAction.Request request = new UnfollowAction.Request(followerIndex);
        getClient().execute(UnfollowAction.INSTANCE, request, ActionListener.wrap(
            r -> {
                assert r.isAcknowledged() : "unfollow response is not acknowledged";
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
