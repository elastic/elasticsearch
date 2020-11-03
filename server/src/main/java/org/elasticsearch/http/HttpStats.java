/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.http;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

public class HttpStats implements Writeable, ToXContentFragment {

    private final long serverOpen;
    private final long totalOpen;
    private final Collection<ClientStats> clientStats;

    public HttpStats(Collection<ClientStats> clientStats, long serverOpen, long totalOpened) {
        this.clientStats = clientStats;
        this.serverOpen = serverOpen;
        this.totalOpen = totalOpened;
    }

    public HttpStats(long serverOpen, long totalOpened) {
        this(null, serverOpen, totalOpened);
    }

    public HttpStats(StreamInput in) throws IOException {
        serverOpen = in.readVLong();
        totalOpen = in.readVLong();
        clientStats = List.of();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(serverOpen);
        out.writeVLong(totalOpen);
    }

    public long getServerOpen() {
        return this.serverOpen;
    }

    public long getTotalOpen() {
        return this.totalOpen;
    }

    static final class Fields {
        static final String HTTP = "http";
        static final String CURRENT_OPEN = "current_open";
        static final String TOTAL_OPENED = "total_opened";
        static final String CLIENTS = "clients";
        static final String CLIENT_AGENT = "agent";
        static final String CLIENT_LOCAL_ADDRESS = "local_address";
        static final String CLIENT_REMOTE_ADDRESS = "remote_address";
        static final String CLIENT_LAST_URI = "last_uri";
        static final String CLIENT_OPENED_TIME_MILLIS = "opened_time_millis";
        static final String CLIENT_CLOSED_TIME_MILLIS = "closed_time_millis";
        static final String CLIENT_LAST_REQUEST_TIME_MILLIS = "last_request_time_millis";
        static final String CLIENT_REQUEST_COUNT = "request_count";
        static final String CLIENT_REQUEST_SIZE_BYTES = "request_size_bytes";
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.HTTP);
        builder.field(Fields.CURRENT_OPEN, serverOpen);
        builder.field(Fields.TOTAL_OPENED, totalOpen);
        builder.startArray(Fields.CLIENTS);
        for (ClientStats clientStats : this.clientStats) {
            builder.startObject();
            builder.field(Fields.CLIENT_AGENT, clientStats.agent);
            builder.field(Fields.CLIENT_LOCAL_ADDRESS, clientStats.localAddress);
            builder.field(Fields.CLIENT_REMOTE_ADDRESS, clientStats.remoteAddress);
            builder.field(Fields.CLIENT_LAST_URI, clientStats.lastUri);
            builder.field(Fields.CLIENT_OPENED_TIME_MILLIS, clientStats.openedTimeMillis);
            builder.field(Fields.CLIENT_CLOSED_TIME_MILLIS, clientStats.closedTimeMillis);
            builder.field(Fields.CLIENT_LAST_REQUEST_TIME_MILLIS, clientStats.lastRequestTimeMillis);
            builder.field(Fields.CLIENT_REQUEST_COUNT, clientStats.requestCount);
            builder.field(Fields.CLIENT_REQUEST_SIZE_BYTES, clientStats.requestSizeBytes);
            builder.endObject();
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }

    static class ClientStats {
        String agent;
        String localAddress;
        String remoteAddress;
        String lastUri;
        long openedTimeMillis;
        long closedTimeMillis = -1;
        volatile long lastRequestTimeMillis = -1;
        volatile long requestCount;
        volatile long requestSizeBytes;

        ClientStats(HttpChannel httpChannel, long openedTimeMillis) {
            this.localAddress = NetworkAddress.format(httpChannel.getLocalAddress());
            this.remoteAddress = NetworkAddress.format(httpChannel.getRemoteAddress());
            this.openedTimeMillis = openedTimeMillis;
        }
    }
}
