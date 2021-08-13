/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.http;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.LongAdder;

public class HttpStats implements Writeable, ToXContentFragment {

    private final long serverOpen;
    private final long totalOpen;
    private final List<ClientStats> clientStats;

    public HttpStats(List<ClientStats> clientStats, long serverOpen, long totalOpened) {
        this.clientStats = clientStats;
        this.serverOpen = serverOpen;
        this.totalOpen = totalOpened;
    }

    public HttpStats(long serverOpen, long totalOpened) {
        this(List.of(), serverOpen, totalOpened);
    }

    public HttpStats(StreamInput in) throws IOException {
        serverOpen = in.readVLong();
        totalOpen = in.readVLong();
        clientStats = in.readList(ClientStats::new);
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
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.HTTP);
        builder.field(Fields.CURRENT_OPEN, serverOpen);
        builder.field(Fields.TOTAL_OPENED, totalOpen);
        builder.startArray(Fields.CLIENTS);
        for (ClientStats clientStats : this.clientStats) {
            clientStats.toXContent(builder, params);
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }

    public static class ClientStats implements Writeable, ToXContentFragment {
        final int id;
        String agent;
        String localAddress;
        String remoteAddress;
        String lastUri;
        String forwardedFor;
        String opaqueId;
        long openedTimeMillis;
        long closedTimeMillis = -1;
        volatile long lastRequestTimeMillis = -1;
        final LongAdder requestCount = new LongAdder();
        final LongAdder requestSizeBytes = new LongAdder();

        ClientStats(long openedTimeMillis) {
            this.id = System.identityHashCode(this);
            this.openedTimeMillis = openedTimeMillis;
        }

        // visible for testing
        public ClientStats(String agent, String localAddress, String remoteAddress, String lastUri, String forwardedFor, String opaqueId,
            long openedTimeMillis, long closedTimeMillis, long lastRequestTimeMillis, long requestCount, long requestSizeBytes) {
            this.id = System.identityHashCode(this);
            this.agent = agent;
            this.localAddress = localAddress;
            this.remoteAddress = remoteAddress;
            this.lastUri = lastUri;
            this.forwardedFor = forwardedFor;
            this.opaqueId = opaqueId;
            this.openedTimeMillis = openedTimeMillis;
            this.closedTimeMillis = closedTimeMillis;
            this.lastRequestTimeMillis = lastRequestTimeMillis;
            this.requestCount.add(requestCount);
            this.requestSizeBytes.add(requestSizeBytes);
        }

        ClientStats(StreamInput in) throws IOException {
            this.id = in.readInt();
            this.agent = in.readOptionalString();
            this.localAddress = in.readOptionalString();
            this.remoteAddress = in.readOptionalString();
            this.lastUri = in.readOptionalString();
            this.forwardedFor = in.readOptionalString();
            this.opaqueId = in.readOptionalString();
            this.openedTimeMillis = in.readLong();
            this.closedTimeMillis = in.readLong();
            this.lastRequestTimeMillis = in.readLong();
            this.requestCount.add(in.readLong());
            this.requestSizeBytes.add(in.readLong());
        }

        @Override public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
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
            if (closedTimeMillis != -1) {
                builder.field(Fields.CLIENT_CLOSED_TIME_MILLIS, closedTimeMillis);
            }
            builder.field(Fields.CLIENT_LAST_REQUEST_TIME_MILLIS, lastRequestTimeMillis);
            builder.field(Fields.CLIENT_REQUEST_COUNT, requestCount.longValue());
            builder.field(Fields.CLIENT_REQUEST_SIZE_BYTES, requestSizeBytes.longValue());
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
            out.writeLong(requestCount.longValue());
            out.writeLong(requestSizeBytes.longValue());
        }
    }
}
