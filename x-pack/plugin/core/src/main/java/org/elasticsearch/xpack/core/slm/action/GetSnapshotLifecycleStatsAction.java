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
import org.elasticsearch.action.support.master.MasterNodeReadRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
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

    public static class Request extends MasterNodeReadRequest<Request> {
        private static final Logger logger = LogManager.getLogger(GetSnapshotLifecycleStatsAction.Request.class);

        // for backwards compatibility, store the ack timeout to maintain compatibility with AcknowledgedRequest used in previous versions
        private final TimeValue ackTimeout;

        // private to avoid non-backwards compatible instantiation
        private Request(StreamInput input) throws IOException {
            super(input);
            this.ackTimeout = null;
        }

        // private, should not be used directly
        private Request(TimeValue masterNodeTimeout, TimeValue ackTimeout) {
            super(masterNodeTimeout);
            this.ackTimeout = ackTimeout;
        }

        public Request(TimeValue masterNodeTimeout) throws IOException {
            super(masterNodeTimeout);
            this.ackTimeout = null;
        }

        public static Request read(StreamInput input) throws IOException {
            logger.info("Reading GetSnapshotLifecycleStatsAction.Request from stream input");
            if (input.getTransportVersion().onOrAfter(SLM_GET_STATS_CHANGE_REQUEST_TYPE)) {
                logger.info("Reading old GetSnapshotLifecycleStatsAction.Request format");
                return new Request(input);
            } else {
                logger.info("Reading new GetSnapshotLifecycleStatsAction.Request format");
                var requestBwc = new AcknowledgedRequest.Plain(input);
                return new GetSnapshotLifecycleStatsAction.Request(requestBwc.masterNodeTimeout(), requestBwc.ackTimeout());
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            if (out.getTransportVersion().onOrAfter(SLM_GET_STATS_CHANGE_REQUEST_TYPE)) {
                super.writeTo(out);
            } else {
                // For backwards compatibility, write the request as an AcknowledgedRequest
                new AcknowledgedRequest.Plain(this.masterNodeTimeout(), this.ackTimeout).writeTo(out);
            }
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
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
