/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.Index;
import org.elasticsearch.xpack.cluster.routing.allocation.DataTierAllocationDecider;

import java.io.IOException;
import java.util.Objects;

/**
 * This step waits for one of the data tiers to be available in the cluster. This has two purposes:
 * <ul>
 *     <li>Avoid a mounted index going RED, it is better to pause ILM on this condition</li>
 *     <li>Leave a signal to autoscaling to scale up the first node for the tier</li>
 * </ul>
 */
public class WaitForDataTierStep extends ClusterStateWaitStep {
    public static final String NAME = "wait-for-data-tier";
    private final String tierPreference;

    public WaitForDataTierStep(StepKey key, StepKey nextStepKey, String tierPreference) {
        super(key, nextStepKey);
        this.tierPreference = Objects.requireNonNull(tierPreference);
    }

    @Override
    public Result isConditionMet(Index index, ClusterState clusterState) {
        boolean present = DataTierAllocationDecider.preferredAvailableTier(tierPreference, clusterState.nodes()).isPresent();
        Info info = present ? null : new Info("no nodes for tiers [" + tierPreference + "] available");
        return new Result(present, info);
    }

    @Override
    public boolean isRetryable() {
        return true;
    }

    String tierPreference() {
        return tierPreference;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (super.equals(o) == false) return false;
        WaitForDataTierStep that = (WaitForDataTierStep) o;
        return tierPreference.equals(that.tierPreference);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), tierPreference);
    }

    static final class Info implements ToXContentObject {

        private final String message;

        static final ParseField MESSAGE = new ParseField("message");

        Info(String message) {
            this.message = message;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(MESSAGE.getPreferredName(), message);
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Info info = (Info) o;
            return Objects.equals(message, info.message);
        }

        @Override
        public int hashCode() {
            return Objects.hash(message);
        }
    }
}
