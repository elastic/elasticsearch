/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.action.datastreams;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.TimeValue;

import java.io.IOException;
import java.time.Instant;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * Internal action that creates one or more past TSDB backing indices to cover past timestamps.
 * Each new backing index is clamped to avoid overlap with existing backing indices.
 */
public final class PastTimeSeriesIndexCreationAction extends ActionType<PastTimeSeriesIndexCreationAction.Response> {

    public static final PastTimeSeriesIndexCreationAction INSTANCE = new PastTimeSeriesIndexCreationAction();
    public static final String NAME = "indices:admin/data_stream/auto_create_past_tsdb";

    private PastTimeSeriesIndexCreationAction() {
        super(NAME);
    }

    public static class Request extends AcknowledgedRequest<Request> implements IndicesRequest {

        private final long requestStartTime;
        private final String dataStreamName;
        private final Collection<Instant> timestamps;

        public Request(TimeValue masterNodeTimeout, String dataStreamName, Collection<Instant> timestamps, long requestStartTime) {
            this(masterNodeTimeout, DEFAULT_ACK_TIMEOUT, dataStreamName, timestamps, requestStartTime);
        }

        public Request(
            TimeValue masterNodeTimeout,
            TimeValue ackTimeout,
            String dataStreamName,
            Collection<Instant> timestamps,
            long requestStartTime
        ) {
            super(masterNodeTimeout, ackTimeout);
            this.requestStartTime = requestStartTime;
            this.dataStreamName = dataStreamName;
            this.timestamps = timestamps;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.dataStreamName = in.readString();
            this.timestamps = in.readCollectionAsList(StreamInput::readInstant);
            this.requestStartTime = in.readVLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(dataStreamName);
            out.writeCollection(timestamps, StreamOutput::writeInstant);
            out.writeVLong(requestStartTime);
        }

        public String dataStreamName() {
            return dataStreamName;
        }

        public Collection<Instant> timestamps() {
            return timestamps;
        }

        public long requestStartTime() {
            return requestStartTime;
        }

        @Override
        public String[] indices() {
            return new String[] { dataStreamName };
        }

        @Override
        public IndicesOptions indicesOptions() {
            return IndicesOptions.strictSingleIndexNoExpandForbidClosed();
        }
    }

    /**
     * Reports the subset of the request's timestamps that are covered by a backing index after the action completes
     * (whether already covered before the call or covered by a newly created backing index), and the rejected timestamps
     * along with the reason why they could not be covered.
     */
    public static class Response extends AcknowledgedResponse {

        private final Set<Instant> coveredTimestamps;
        private final Map<Instant, String> rejectedTimestamps;

        public Response(boolean acknowledged, Set<Instant> coveredTimestamps) {
            this(acknowledged, coveredTimestamps, Map.of());
        }

        public Response(boolean acknowledged, Set<Instant> coveredTimestamps, Map<Instant, String> rejectedTimestamps) {
            super(acknowledged);
            this.coveredTimestamps = coveredTimestamps;
            this.rejectedTimestamps = rejectedTimestamps;
        }

        public Response(StreamInput in) throws IOException {
            super(in);
            this.coveredTimestamps = in.readCollectionAsSet(StreamInput::readInstant);
            this.rejectedTimestamps = in.readMap(StreamInput::readInstant, StreamInput::readString);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeCollection(coveredTimestamps, StreamOutput::writeInstant);
            out.writeMap(rejectedTimestamps, StreamOutput::writeInstant, StreamOutput::writeString);
        }

        public Set<Instant> coveredTimestamps() {
            return coveredTimestamps;
        }

        /**
         * The rejected timestamps and the reason why, mainly used for debugging purposes.
         */
        public Map<Instant, String> rejectedTimestamps() {
            return rejectedTimestamps;
        }
    }
}
