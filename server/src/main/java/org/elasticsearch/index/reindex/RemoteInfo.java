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

package org.elasticsearch.index.reindex;

import org.elasticsearch.Version;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.TimeValue;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.unmodifiableMap;
import static java.util.Objects.requireNonNull;
import static org.elasticsearch.common.unit.TimeValue.timeValueSeconds;

public class RemoteInfo implements Writeable {
    /**
     * Default {@link #socketTimeout} for requests that don't have one set.
     */
    public static final TimeValue DEFAULT_SOCKET_TIMEOUT = timeValueSeconds(30);
    /**
     * Default {@link #connectTimeout} for requests that don't have one set.
     */
    public static final TimeValue DEFAULT_CONNECT_TIMEOUT = timeValueSeconds(30);

    private final String scheme;
    private final String host;
    private final int port;
    private final BytesReference query;
    private final String username;
    private final String password;
    private final Map<String, String> headers;
    /**
     * Time to wait for a response from each request.
     */
    private final TimeValue socketTimeout;
    /**
     * Time to wait for a connecting to the remote cluster.
     */
    private final TimeValue connectTimeout;

    public RemoteInfo(String scheme, String host, int port, BytesReference query, String username, String password,
            Map<String, String> headers, TimeValue socketTimeout, TimeValue connectTimeout) {
        this.scheme = requireNonNull(scheme, "[scheme] must be specified to reindex from a remote cluster");
        this.host = requireNonNull(host, "[host] must be specified to reindex from a remote cluster");
        this.port = port;
        this.query = requireNonNull(query, "[query] must be specified to reindex from a remote cluster");
        this.username = username;
        this.password = password;
        this.headers = unmodifiableMap(requireNonNull(headers, "[headers] is required"));
        this.socketTimeout = requireNonNull(socketTimeout, "[socketTimeout] must be specified");
        this.connectTimeout = requireNonNull(connectTimeout, "[connectTimeout] must be specified");
    }

    /**
     * Read from a stream.
     */
    public RemoteInfo(StreamInput in) throws IOException {
        scheme = in.readString();
        host = in.readString();
        port = in.readVInt();
        query = in.readBytesReference();
        username = in.readOptionalString();
        password = in.readOptionalString();
        int headersLength = in.readVInt();
        Map<String, String> headers = new HashMap<>(headersLength);
        for (int i = 0; i < headersLength; i++) {
            headers.put(in.readString(), in.readString());
        }
        this.headers = unmodifiableMap(headers);
        if (in.getVersion().onOrAfter(Version.V_5_2_0)) {
            socketTimeout = new TimeValue(in);
            connectTimeout = new TimeValue(in);
        } else {
            socketTimeout = DEFAULT_SOCKET_TIMEOUT;
            connectTimeout = DEFAULT_CONNECT_TIMEOUT;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(scheme);
        out.writeString(host);
        out.writeVInt(port);
        out.writeBytesReference(query);
        out.writeOptionalString(username);
        out.writeOptionalString(password);
        out.writeVInt(headers.size());
        for (Map.Entry<String, String> header : headers.entrySet()) {
            out.writeString(header.getKey());
            out.writeString(header.getValue());
        }
        if (out.getVersion().onOrAfter(Version.V_5_2_0)) {
            socketTimeout.writeTo(out);
            connectTimeout.writeTo(out);
        }
    }

    public String getScheme() {
        return scheme;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public BytesReference getQuery() {
        return query;
    }

    @Nullable
    public String getUsername() {
        return username;
    }

    @Nullable
    public String getPassword() {
        return password;
    }

    public Map<String, String> getHeaders() {
        return headers;
    }

    /**
     * Time to wait for a response from each request.
     */
    public TimeValue getSocketTimeout() {
        return socketTimeout;
    }

    /**
     * Time to wait to connect to the external cluster.
     */
    public TimeValue getConnectTimeout() {
        return connectTimeout;
    }

    @Override
    public String toString() {
        StringBuilder b = new StringBuilder();
        if (false == "http".equals(scheme)) {
            // http is the default so it isn't worth taking up space if it is the scheme
            b.append("scheme=").append(scheme).append(' ');
        }
        b.append("host=").append(host).append(" port=").append(port).append(" query=").append(query.utf8ToString());
        if (username != null) {
            b.append(" username=").append(username);
        }
        if (password != null) {
            b.append(" password=<<>>");
        }
        return b.toString();
    }
}
