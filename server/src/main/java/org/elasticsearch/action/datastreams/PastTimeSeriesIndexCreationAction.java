/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.action.datastreams;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.TimeValue;

import java.io.IOException;
import java.time.Instant;
import java.util.Collection;
import java.util.Set;

/**
 * Internal action that creates one or more historical TSDB backing indices to cover past timestamps.
 * Each new backing index is clamped to avoid overlap with existing backing indices.
 */
public final class PastTimeSeriesIndexCreationAction extends ActionType<PastTimeSeriesIndexCreationAction.Response> {

    public static final PastTimeSeriesIndexCreationAction INSTANCE = new PastTimeSeriesIndexCreationAction();
    public static final String NAME = "indices:admin/data_stream/auto_create_past_tsdb";

    private PastTimeSeriesIndexCreationAction() {
        super(NAME);
    }

    public static class Request extends MasterNodeRequest<Request> implements IndicesRequest {

        private final String dataStreamName;
        private final Collection<Instant> timestamps;

        public Request(TimeValue masterNodeTimeout, String dataStreamName, Collection<Instant> timestamps) {
            super(masterNodeTimeout);
            this.dataStreamName = dataStreamName;
            this.timestamps = timestamps;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.dataStreamName = in.readString();
            this.timestamps = in.readCollectionAsList(StreamInput::readInstant);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(dataStreamName);
            out.writeCollection(timestamps, StreamOutput::writeInstant);
        }

        public String dataStreamName() {
            return dataStreamName;
        }

        public Collection<Instant> timestamps() {
            return timestamps;
        }

        @Override
        public String[] indices() {
            return new String[] { dataStreamName };
        }

        @Override
        public IndicesOptions indicesOptions() {
            return IndicesOptions.strictSingleIndexNoExpandForbidClosed();
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }
    }

    /**
     * Reports the subset of the request's timestamps that are covered by a backing index after the action completes
     * (whether already covered before the call or covered by a newly created backing index). The caller can derive
     * the uncovered set by subtracting these from what was requested.
     */
    public static class Response extends ActionResponse {

        private final Set<Instant> coveredTimestamps;

        public Response(Set<Instant> coveredTimestamps) {
            this.coveredTimestamps = coveredTimestamps;
        }

        public Response(StreamInput in) throws IOException {
            this.coveredTimestamps = in.readCollectionAsSet(StreamInput::readInstant);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeCollection(coveredTimestamps, StreamOutput::writeInstant);
        }

        public Set<Instant> coveredTimestamps() {
            return coveredTimestamps;
        }
    }
}
