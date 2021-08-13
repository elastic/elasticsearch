/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.reindex;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static java.util.Collections.unmodifiableMap;
import static java.util.Objects.requireNonNull;
import static org.elasticsearch.core.TimeValue.timeValueSeconds;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;

public class RemoteInfo implements Writeable, ToXContentObject {
    /**
     * Default {@link #socketTimeout} for requests that don't have one set.
     */
    public static final TimeValue DEFAULT_SOCKET_TIMEOUT = timeValueSeconds(30);
    /**
     * Default {@link #connectTimeout} for requests that don't have one set.
     */
    public static final TimeValue DEFAULT_CONNECT_TIMEOUT = timeValueSeconds(30);

    public static final XContent QUERY_CONTENT_TYPE = JsonXContent.jsonXContent;

    private final String scheme;
    private final String host;
    private final int port;
    private final String pathPrefix;
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

    public RemoteInfo(String scheme, String host, int port, String pathPrefix, BytesReference query, String username, String password,
                      Map<String, String> headers, TimeValue socketTimeout, TimeValue connectTimeout) {
        assert isQueryJson(query) : "Query does not appear to be JSON";
        this.scheme = requireNonNull(scheme, "[scheme] must be specified to reindex from a remote cluster");
        this.host = requireNonNull(host, "[host] must be specified to reindex from a remote cluster");
        this.port = port;
        this.pathPrefix = pathPrefix;
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
        socketTimeout = in.readTimeValue();
        connectTimeout = in.readTimeValue();
        pathPrefix = in.readOptionalString();
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
        out.writeTimeValue(socketTimeout);
        out.writeTimeValue(connectTimeout);
        out.writeOptionalString(pathPrefix);
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

    @Nullable
    public String getPathPrefix() {
        return pathPrefix;
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
        b.append("host=").append(host).append(" port=").append(port);
        if (pathPrefix != null) {
            b.append(" pathPrefix=").append(pathPrefix);
        }
        b.append(" query=").append(query.utf8ToString());
        if (username != null) {
            b.append(" username=").append(username);
        }
        if (password != null) {
            b.append(" password=<<>>");
        }
        return b.toString();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (username != null) {
            builder.field("username", username);
        }
        if (password != null) {
            builder.field("password", password);
        }
        builder.field("host", scheme + "://" + host + ":" + port +
            (pathPrefix == null ? "" : "/" + pathPrefix));
        if (headers.size() >0 ) {
            builder.field("headers", headers);
        }
        builder.field("socket_timeout", socketTimeout.getStringRep());
        builder.field("connect_timeout", connectTimeout.getStringRep());
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RemoteInfo that = (RemoteInfo) o;
        return port == that.port &&
            Objects.equals(scheme, that.scheme) &&
            Objects.equals(host, that.host) &&
            Objects.equals(pathPrefix, that.pathPrefix) &&
            Objects.equals(query, that.query) &&
            Objects.equals(username, that.username) &&
            Objects.equals(password, that.password) &&
            Objects.equals(headers, that.headers) &&
            Objects.equals(socketTimeout, that.socketTimeout) &&
            Objects.equals(connectTimeout, that.connectTimeout);
    }

    @Override
    public int hashCode() {
        return Objects.hash(scheme, host, port, pathPrefix, query, username, password, headers, socketTimeout, connectTimeout);
    }

    static BytesReference queryForRemote(Map<String, Object> source) throws IOException {
        XContentBuilder builder = XContentBuilder.builder(QUERY_CONTENT_TYPE).prettyPrint();
        Object query = source.remove("query");
        if (query == null) {
            return BytesReference.bytes(matchAllQuery().toXContent(builder, ToXContent.EMPTY_PARAMS));
        }
        if ((query instanceof Map) == false) {
            throw new IllegalArgumentException("Expected [query] to be an object but was [" + query + "]");
        }
        @SuppressWarnings("unchecked")
        Map<String, Object> map = (Map<String, Object>) query;
        return BytesReference.bytes(builder.map(map));
    }

    private static boolean isQueryJson(BytesReference bytesReference) {
        try (XContentParser parser = QUERY_CONTENT_TYPE.createParser(NamedXContentRegistry.EMPTY,
            DeprecationHandler.THROW_UNSUPPORTED_OPERATION, bytesReference.streamInput())) {
            Map<String, Object> query = parser.map();
            return true;
        } catch (IOException e) {
            throw new AssertionError("Could not parse JSON", e);
        }
    }
}
