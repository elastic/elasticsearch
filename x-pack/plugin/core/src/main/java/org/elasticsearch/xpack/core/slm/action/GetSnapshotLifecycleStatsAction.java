/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.slm.action;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.local.LocalClusterStateRequest;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.UpdateForV10;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.slm.SnapshotLifecycleStats;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.TransportVersions.SLM_GET_STATS_CHANGE_REQUEST_TYPE;

/**
 * This class represents the action of retriving the stats for snapshot lifecycle management.
 * These are retrieved from the master's cluster state and contain numbers related to the count of
 * snapshots taken or deleted, as well as retention runs and time spent deleting snapshots.
 */
public class GetSnapshotLifecycleStatsAction extends ActionType<GetSnapshotLifecycleStatsAction.Response> {
    public static final GetSnapshotLifecycleStatsAction INSTANCE = new GetSnapshotLifecycleStatsAction();
    public static final String NAME = "cluster:admin/slm/stats";

    protected GetSnapshotLifecycleStatsAction() {
        super(NAME);
    }

    public static class Request extends LocalClusterStateRequest {

        public Request(TimeValue masterNodeTimeout) {
            super(masterNodeTimeout);
        }

        // private, to avoid non-backwards compatible use
        private Request(StreamInput input) throws IOException {
            super(input);
        }

        /**
         * Previously this request was an AcknowledgedRequest, which had an ack timeout, and the action was an MasterNodeAction.
         * This method only exists for backward compatibility to deserialize request from previous versions.
         */
        @UpdateForV10(owner = UpdateForV10.Owner.DATA_MANAGEMENT)
        public static Request read(StreamInput input) throws IOException {
            if (input.getTransportVersion().onOrAfter(SLM_GET_STATS_CHANGE_REQUEST_TYPE)) {
                return new Request(input);
            } else {
                var requestBwc = new AcknowledgedRequest.Plain(input);
                return new Request(requestBwc.masterNodeTimeout());
            }
        }

    }

    public static class Response extends ActionResponse implements ToXContentObject {

        private SnapshotLifecycleStats slmStats;

        public Response() {}

        public Response(SnapshotLifecycleStats slmStats) {
            this.slmStats = slmStats;
        }

        public Response(StreamInput in) throws IOException {
            this.slmStats = new SnapshotLifecycleStats(in);
        }

        public SnapshotLifecycleStats getSlmStats() {
            return slmStats;
        }

        @Override
        public String toString() {
            return Strings.toString(this);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return this.slmStats.toXContent(builder, params);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            this.slmStats.writeTo(out);
        }

        @Override
        public int hashCode() {
            return Objects.hash(this.slmStats);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (obj.getClass() != getClass()) {
                return false;
            }
            GetSnapshotLifecycleStatsAction.Response other = (GetSnapshotLifecycleStatsAction.Response) obj;
            return this.slmStats.equals(other.slmStats);
        }
    }

}
