/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.datastreams.lifecycle.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.MasterNodeReadRequest;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ChunkedToXContentObject;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.ToXContent;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

/**
 * This action retrieves the data stream lifecycle stats from the master node.
 */
public class GetDataStreamLifecycleStatsAction extends ActionType<GetDataStreamLifecycleStatsAction.Response> {

    public static final GetDataStreamLifecycleStatsAction INSTANCE = new GetDataStreamLifecycleStatsAction();
    public static final String NAME = "cluster:monitor/data_stream/lifecycle/stats";

    private GetDataStreamLifecycleStatsAction() {
        super(NAME);
    }

    public static class Request extends MasterNodeReadRequest<Request> {

        public Request(StreamInput in) throws IOException {
            super(in);
        }

        public Request(TimeValue masterNodeTimeout) {
            super(masterNodeTimeout);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }
    }

    public static class Response extends ActionResponse implements ChunkedToXContentObject {

        private final Long runDuration;
        private final Long timeBetweenStarts;
        private final List<DataStreamStats> dataStreamStats;

        public Response(@Nullable Long runDuration, @Nullable Long timeBetweenStarts, List<DataStreamStats> dataStreamStats) {
            this.runDuration = runDuration;
            this.timeBetweenStarts = timeBetweenStarts;
            this.dataStreamStats = dataStreamStats;
        }

        public Response(StreamInput in) throws IOException {
            this.runDuration = in.readOptionalVLong();
            this.timeBetweenStarts = in.readOptionalVLong();
            this.dataStreamStats = in.readCollectionAsImmutableList(DataStreamStats::read);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeOptionalVLong(runDuration);
            out.writeOptionalVLong(timeBetweenStarts);
            out.writeCollection(dataStreamStats, StreamOutput::writeWriteable);
        }

        public Long getRunDuration() {
            return runDuration;
        }

        public Long getTimeBetweenStarts() {
            return timeBetweenStarts;
        }

        public List<DataStreamStats> getDataStreamStats() {
            return dataStreamStats;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Response other = (Response) o;
            return Objects.equals(runDuration, other.runDuration)
                && Objects.equals(timeBetweenStarts, other.timeBetweenStarts)
                && Objects.equals(dataStreamStats, other.dataStreamStats);
        }

        @Override
        public int hashCode() {
            return Objects.hash(runDuration, timeBetweenStarts, dataStreamStats);
        }

        @Override
        public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params outerParams) {
            return Iterators.concat(Iterators.single((builder, params) -> {
                builder.startObject();
                if (runDuration != null) {
                    builder.field("last_run_duration_in_millis", runDuration);
                    if (builder.humanReadable()) {
                        builder.field("last_run_duration", TimeValue.timeValueMillis(runDuration).toHumanReadableString(2));
                    }
                }
                if (timeBetweenStarts != null) {
                    builder.field("time_between_starts_in_millis", timeBetweenStarts);
                    if (builder.humanReadable()) {
                        builder.field("time_between_starts", TimeValue.timeValueMillis(timeBetweenStarts).toHumanReadableString(2));
                    }
                }
                builder.field("data_stream_count", dataStreamStats.size());
                builder.startArray("data_streams");
                return builder;
            }), Iterators.map(dataStreamStats.iterator(), stat -> (builder, params) -> {
                builder.startObject();
                builder.field("name", stat.dataStreamName);
                builder.field("backing_indices_in_total", stat.backingIndicesInTotal);
                builder.field("backing_indices_in_error", stat.backingIndicesInError);
                builder.endObject();
                return builder;
            }), Iterators.single((builder, params) -> {
                builder.endArray();
                builder.endObject();
                return builder;
            }));
        }

        public record DataStreamStats(String dataStreamName, int backingIndicesInTotal, int backingIndicesInError) implements Writeable {

            public static DataStreamStats read(StreamInput in) throws IOException {
                return new DataStreamStats(in.readString(), in.readVInt(), in.readVInt());
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                out.writeString(dataStreamName);
                out.writeVInt(backingIndicesInTotal);
                out.writeVInt(backingIndicesInError);
            }
        }
    }
}
