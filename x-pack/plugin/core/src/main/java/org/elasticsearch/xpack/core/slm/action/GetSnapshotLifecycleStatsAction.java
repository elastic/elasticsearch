/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.slm.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.slm.SnapshotLifecycleStats;

import java.io.IOException;
import java.util.Objects;

/**
 * This class represents the action of retriving the stats for snapshot lifecycle management.
 * These are retrieved from the master's cluster state and contain numbers related to the count of
 * snapshots taken or deleted, as well as retention runs and time spent deleting snapshots.
 */
public class GetSnapshotLifecycleStatsAction extends ActionType<GetSnapshotLifecycleStatsAction.Response> {
    public static final GetSnapshotLifecycleStatsAction INSTANCE = new GetSnapshotLifecycleStatsAction();
    public static final String NAME = "cluster:admin/slm/stats";

    protected GetSnapshotLifecycleStatsAction() {
        super(NAME, GetSnapshotLifecycleStatsAction.Response::new);
    }

    public static class Request extends AcknowledgedRequest<GetSnapshotLifecycleStatsAction.Request> {

        public Request() { }

        public Request(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {

        private SnapshotLifecycleStats slmStats;

        public Response() { }

        public Response(SnapshotLifecycleStats slmStats) {
            this.slmStats = slmStats;
        }

        public Response(StreamInput in) throws IOException {
            this.slmStats = new SnapshotLifecycleStats(in);
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
