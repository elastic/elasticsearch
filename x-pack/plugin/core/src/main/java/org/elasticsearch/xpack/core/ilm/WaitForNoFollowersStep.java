/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ilm;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.stats.IndexStats;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.protocol.xpack.XPackInfoRequest;
import org.elasticsearch.protocol.xpack.XPackInfoResponse;
import org.elasticsearch.xpack.core.action.XPackInfoFeatureAction;
import org.elasticsearch.xpack.core.ilm.step.info.SingleMessageFieldInfo;

import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.Optional;

/**
 * A step that waits until the managed index is no longer a leader index.
 * This is necessary as there are some actions which are not safe to perform on
 * a leader index, such as those which delete the index, including Shrink and
 * Delete.
 */
public class WaitForNoFollowersStep extends AsyncWaitStep {

    private static final Logger logger = LogManager.getLogger(WaitForNoFollowersStep.class);

    public static final String NAME = "wait-for-shard-history-leases";
    static final String CCR_LEASE_KEY = "ccr";
    private static final String WAIT_MESSAGE = "this index is a leader index; waiting for all following indices to cease "
        + "following before proceeding";

    WaitForNoFollowersStep(StepKey key, StepKey nextStepKey, Client client) {
        super(key, nextStepKey, client);
    }

    @Override
    public boolean isRetryable() {
        return true;
    }

    @Override
    public void evaluateCondition(Metadata metadata, Index index, Listener listener, TimeValue masterTimeout) {
        XPackInfoRequest xPackInfoRequest = new XPackInfoRequest();
        xPackInfoRequest.setCategories(EnumSet.of(XPackInfoRequest.Category.FEATURES));
        getClient().execute(XPackInfoFeatureAction.CCR, xPackInfoRequest, ActionListener.wrap((xPackInfoResponse) -> {
            XPackInfoResponse.FeatureSetsInfo.FeatureSet featureSet = xPackInfoResponse.getInfo();
            if (featureSet != null && featureSet.enabled() == false) {
                listener.onResponse(true, null);
                return;
            }
            leaderIndexCheck(metadata, index, listener, masterTimeout);
        }, listener::onFailure));
    }

    private void leaderIndexCheck(Metadata metadata, Index index, Listener listener, TimeValue masterTimeout) {
        IndicesStatsRequest request = new IndicesStatsRequest();
        request.clear();
        String indexName = index.getName();
        request.indices(indexName);

        getClient().admin().indices().stats(request, ActionListener.wrap((response) -> {
            IndexStats indexStats = response.getIndex(indexName);
            if (indexStats == null) {
                // Index was probably deleted
                logger.debug("got null shard stats for index {}, proceeding on the assumption it has been deleted", indexName);
                listener.onResponse(true, null);
                return;
            }

            boolean isCurrentlyLeaderIndex = Arrays.stream(indexStats.getShards())
                .map(ShardStats::getRetentionLeaseStats)
                .map(Optional::ofNullable)
                .map(o -> o.flatMap(stats -> Optional.ofNullable(stats.retentionLeases())))
                .map(o -> o.flatMap(leases -> Optional.ofNullable(leases.leases())))
                .map(o -> o.map(Collection::stream))
                .anyMatch(lease -> lease.isPresent() && lease.get().anyMatch(l -> CCR_LEASE_KEY.equals(l.source())));

            if (isCurrentlyLeaderIndex) {
                listener.onResponse(false, new SingleMessageFieldInfo(WAIT_MESSAGE));
            } else {
                listener.onResponse(true, null);
            }
        }, listener::onFailure));
    }
}
