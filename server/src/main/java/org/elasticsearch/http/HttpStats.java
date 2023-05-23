/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.http;

import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

public record HttpStats(long serverOpen, long totalOpen, List<ClientStats> clientStats) implements Writeable, ChunkedToXContent {

    public static final HttpStats IDENTITY = new HttpStats(0, 0, List.of());

    public HttpStats(long serverOpen, long totalOpened) {
        this(serverOpen, totalOpened, List.of());
    }

    public HttpStats(StreamInput in) throws IOException {
        this(in.readVLong(), in.readVLong(), in.readList(ClientStats::new));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(serverOpen);
        out.writeVLong(totalOpen);
        out.writeList(clientStats);
    }

    public long getServerOpen() {
        return this.serverOpen;
    }

    public long getTotalOpen() {
        return this.totalOpen;
    }

    public List<ClientStats> getClientStats() {
        return this.clientStats;
    }

    public static HttpStats merge(HttpStats first, HttpStats second) {
        return new HttpStats(
            first.serverOpen + second.serverOpen,
            first.totalOpen + second.totalOpen,
            Stream.concat(first.clientStats.stream(), second.clientStats.stream()).toList()
        );
    }

    static final class Fields {
        static final String HTTP = "http";
        static final String CURRENT_OPEN = "current_open";
        static final String TOTAL_OPENED = "total_opened";
        static final String CLIENTS = "clients";
        static final String CLIENT_ID = "id";
        static final String CLIENT_AGENT = "agent";
        static final String CLIENT_LOCAL_ADDRESS = "local_address";
        static final String CLIENT_REMOTE_ADDRESS = "remote_address";
        static final String CLIENT_LAST_URI = "last_uri";
        static final String CLIENT_OPENED_TIME_MILLIS = "opened_time_millis";
        static final String CLIENT_CLOSED_TIME_MILLIS = "closed_time_millis";
        static final String CLIENT_LAST_REQUEST_TIME_MILLIS = "last_request_time_millis";
        static final String CLIENT_REQUEST_COUNT = "request_count";
        static final String CLIENT_REQUEST_SIZE_BYTES = "request_size_bytes";
        static final String CLIENT_FORWARDED_FOR = "x_forwarded_for";
        static final String CLIENT_OPAQUE_ID = "x_opaque_id";
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params outerParams) {
        return Iterators.<ToXContent>concat(
            Iterators.single(
                (builder, params) -> builder.startObject(Fields.HTTP)
                    .field(Fields.CURRENT_OPEN, serverOpen)
                    .field(Fields.TOTAL_OPENED, totalOpen)
                    .startArray(Fields.CLIENTS)
            ),
            Iterators.flatMap(clientStats.iterator(), Iterators::<ToXContent>single),
            Iterators.single((builder, params) -> builder.endArray().endObject())
        );
    }

    public record ClientStats(
        int id,
        String agent,
        String localAddress,
        String remoteAddress,
        String lastUri,
        String forwardedFor,
        String opaqueId,
        long openedTimeMillis,
        long closedTimeMillis,
        long lastRequestTimeMillis,
        long requestCount,
        long requestSizeBytes
    ) implements Writeable, ToXContentFragment {

        public static final long NOT_CLOSED = -1L;

        ClientStats(StreamInput in) throws IOException {
            this(
                in.readInt(),
                in.readOptionalString(),
                in.readOptionalString(),
                in.readOptionalString(),
                in.readOptionalString(),
                in.readOptionalString(),
                in.readOptionalString(),
                in.readLong(),
                in.readLong(),
                in.readLong(),
                in.readLong(),
                in.readLong()
            );
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(Fields.CLIENT_ID, id);
            if (agent != null) {
                builder.field(Fields.CLIENT_AGENT, agent);
            }
            if (localAddress != null) {
                builder.field(Fields.CLIENT_LOCAL_ADDRESS, localAddress);
            }
            if (remoteAddress != null) {
                builder.field(Fields.CLIENT_REMOTE_ADDRESS, remoteAddress);
            }
            if (lastUri != null) {
                builder.field(Fields.CLIENT_LAST_URI, lastUri);
            }
            if (forwardedFor != null) {
                builder.field(Fields.CLIENT_FORWARDED_FOR, forwardedFor);
            }
            if (opaqueId != null) {
                builder.field(Fields.CLIENT_OPAQUE_ID, opaqueId);
            }
            builder.field(Fields.CLIENT_OPENED_TIME_MILLIS, openedTimeMillis);
            if (closedTimeMillis != NOT_CLOSED) {
                builder.field(Fields.CLIENT_CLOSED_TIME_MILLIS, closedTimeMillis);
            }
            builder.field(Fields.CLIENT_LAST_REQUEST_TIME_MILLIS, lastRequestTimeMillis);
            builder.field(Fields.CLIENT_REQUEST_COUNT, requestCount);
            builder.field(Fields.CLIENT_REQUEST_SIZE_BYTES, requestSizeBytes);
            builder.endObject();
            return builder;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeInt(id);
            out.writeOptionalString(agent);
            out.writeOptionalString(localAddress);
            out.writeOptionalString(remoteAddress);
            out.writeOptionalString(lastUri);
            out.writeOptionalString(forwardedFor);
            out.writeOptionalString(opaqueId);
            out.writeLong(openedTimeMillis);
            out.writeLong(closedTimeMillis);
            out.writeLong(lastRequestTimeMillis);
            out.writeLong(requestCount);
            out.writeLong(requestSizeBytes);
        }
    }
}
