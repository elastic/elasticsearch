/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ccr.action;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ChunkedToXContentHelper;
import org.elasticsearch.common.xcontent.ChunkedToXContentObject;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xpack.core.ccr.AutoFollowStats;

import java.io.IOException;
import java.util.Iterator;
import java.util.Objects;

public class CcrStatsAction extends ActionType<CcrStatsAction.Response> {

    public static final String NAME = "cluster:monitor/ccr/stats";
    public static final CcrStatsAction INSTANCE = new CcrStatsAction();

    private CcrStatsAction() {
        super(NAME);
    }

    public static class Request extends MasterNodeRequest<Request> {

        private TimeValue timeout;

        public Request(StreamInput in) throws IOException {
            super(in);
            if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_14_0)) {
                timeout = in.readOptionalTimeValue();
            }
        }

        public Request(TimeValue masterNodeTimeout) {
            super(masterNodeTimeout);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_14_0)) {
                out.writeOptionalTimeValue(timeout);
            }
        }

        public TimeValue getTimeout() {
            return this.timeout;
        }

        public void setTimeout(TimeValue timeout) {
            this.timeout = timeout;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Request that = (Request) o;
            return Objects.equals(this.timeout, that.timeout) && Objects.equals(this.masterNodeTimeout(), that.masterNodeTimeout());
        }

        @Override
        public int hashCode() {
            return Objects.hash(this.timeout, this.masterNodeTimeout());
        }

        @Override
        public String toString() {
            return "CcrStatsAction.Request[timeout=" + timeout + ", masterNodeTimeout=" + masterNodeTimeout() + "]";
        }
    }

    public static class Response extends ActionResponse implements ChunkedToXContentObject {

        private final AutoFollowStats autoFollowStats;
        private final FollowStatsAction.StatsResponses followStats;

        public Response(AutoFollowStats autoFollowStats, FollowStatsAction.StatsResponses followStats) {
            this.autoFollowStats = Objects.requireNonNull(autoFollowStats);
            this.followStats = Objects.requireNonNull(followStats);
        }

        public Response(StreamInput in) throws IOException {
            autoFollowStats = new AutoFollowStats(in);
            followStats = new FollowStatsAction.StatsResponses(in);
        }

        public AutoFollowStats getAutoFollowStats() {
            return autoFollowStats;
        }

        public FollowStatsAction.StatsResponses getFollowStats() {
            return followStats;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            autoFollowStats.writeTo(out);
            followStats.writeTo(out);
        }

        @Override
        public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params outerParams) {
            return Iterators.concat(
                Iterators.single(
                    (builder, params) -> builder.startObject().field("auto_follow_stats", autoFollowStats, params).field("follow_stats")
                ),
                followStats.toXContentChunked(outerParams),
                ChunkedToXContentHelper.endObject()
            );
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Response response = (Response) o;
            return Objects.equals(autoFollowStats, response.autoFollowStats) && Objects.equals(followStats, response.followStats);
        }

        @Override
        public int hashCode() {
            return Objects.hash(autoFollowStats, followStats);
        }
    }

}
