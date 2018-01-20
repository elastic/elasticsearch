/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.common.http;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.RestUtils;
import org.elasticsearch.xpack.watcher.common.http.auth.HttpAuth;
import org.elasticsearch.xpack.watcher.common.http.auth.HttpAuthRegistry;
import org.elasticsearch.xpack.watcher.support.WatcherDateTimeUtils;
import org.elasticsearch.xpack.watcher.support.WatcherUtils;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableMap;

public class HttpRequest implements ToXContentObject {

    final String host;
    final int port;
    final Scheme scheme;
    final HttpMethod method;
    @Nullable final String path;
    final Map<String, String> params;
    final Map<String, String> headers;
    @Nullable final HttpAuth auth;
    @Nullable final String body;
    @Nullable final TimeValue connectionTimeout;
    @Nullable final TimeValue readTimeout;
    @Nullable final HttpProxy proxy;

    public HttpRequest(String host, int port, @Nullable Scheme scheme, @Nullable HttpMethod method, @Nullable String path,
                       @Nullable Map<String, String> params, @Nullable Map<String, String> headers,
                       @Nullable HttpAuth auth, @Nullable String body, @Nullable TimeValue connectionTimeout,
                       @Nullable TimeValue readTimeout, @Nullable HttpProxy proxy) {
        this.host = host;
        this.port = port;
        this.scheme = scheme != null ? scheme : Scheme.HTTP;
        this.method = method != null ? method : HttpMethod.GET;
        this.path = path;
        this.params = params != null ? params : emptyMap();
        this.headers = headers != null ? headers : emptyMap();
        this.auth = auth;
        this.body = body;
        this.connectionTimeout = connectionTimeout;
        this.readTimeout = readTimeout;
        this.proxy = proxy;
    }

    public Scheme scheme() {
        return scheme;
    }

    public String host() {
        return host;
    }

    public int port() {
        return port;
    }

    public HttpMethod method() {
        return method;
    }

    public String path() {
        return path;
    }

    public Map<String, String> params() {
        return params;
    }

    public Map<String, String> headers() {
        return headers;
    }

    public HttpAuth auth() {
        return auth;
    }

    public boolean hasBody() {
        return body != null;
    }

    public String body() {
        return body;
    }

    public TimeValue connectionTimeout() {
        return connectionTimeout;
    }

    public TimeValue readTimeout() {
        return readTimeout;
    }

    public HttpProxy proxy() {
        return proxy;
    }

    public static String encodeUrl(String text) {
        try {
            return URLEncoder.encode(text, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new IllegalArgumentException("failed to URL encode text [" + text + "]", e);
        }
    }

    public static String decodeUrl(String text) {
        try {
            return URLDecoder.decode(text, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new IllegalArgumentException("failed to URL decode text [" + text + "]", e);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject();
        builder.field(Field.HOST.getPreferredName(), host);
        builder.field(Field.PORT.getPreferredName(), port);
        builder.field(Field.SCHEME.getPreferredName(), scheme.value());
        builder.field(Field.METHOD.getPreferredName(), method.value());
        if (path != null) {
            builder.field(Field.PATH.getPreferredName(), path);
        }
        if (!this.params.isEmpty()) {
            builder.field(Field.PARAMS.getPreferredName(), this.params);
        }
        if (!headers.isEmpty()) {
            builder.field(Field.HEADERS.getPreferredName(), headers);
        }
        if (auth != null) {
            builder.startObject(Field.AUTH.getPreferredName())
                        .field(auth.type(), auth, params)
                    .endObject();
        }
        if (body != null) {
            builder.field(Field.BODY.getPreferredName(), body);
        }
        if (connectionTimeout != null) {
            builder.timeValueField(HttpRequest.Field.CONNECTION_TIMEOUT.getPreferredName(),
                    HttpRequest.Field.CONNECTION_TIMEOUT_HUMAN.getPreferredName(), connectionTimeout);
        }
        if (readTimeout != null) {
            builder.timeValueField(HttpRequest.Field.READ_TIMEOUT.getPreferredName(),
                    HttpRequest.Field.READ_TIMEOUT_HUMAN.getPreferredName(), readTimeout);
        }
        if (proxy != null) {
            proxy.toXContent(builder, params);
        }
        return builder.endObject();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        HttpRequest that = (HttpRequest) o;

        if (port != that.port) return false;
        if (!host.equals(that.host)) return false;
        if (scheme != that.scheme) return false;
        if (method != that.method) return false;
        if (path != null ? !path.equals(that.path) : that.path != null) return false;
        if (!params.equals(that.params)) return false;
        if (!headers.equals(that.headers)) return false;
        if (auth != null ? !auth.equals(that.auth) : that.auth != null) return false;
        if (connectionTimeout != null ? !connectionTimeout.equals(that.connectionTimeout) : that.connectionTimeout != null) return false;
        if (readTimeout != null ? !readTimeout.equals(that.readTimeout) : that.readTimeout != null) return false;
        if (proxy != null ? !proxy.equals(that.proxy) : that.proxy != null) return false;
        return !(body != null ? !body.equals(that.body) : that.body != null);

    }

    @Override
    public int hashCode() {
        return Objects.hash(host, port, scheme, method, path, params, headers, auth, connectionTimeout, readTimeout, body, proxy);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("method=[").append(method).append("], ");
        sb.append("scheme=[").append(scheme).append("], ");
        sb.append("host=[").append(host).append("], ");
        sb.append("port=[").append(port).append("], ");
        sb.append("path=[").append(path).append("], ");
        if (!headers.isEmpty()) {
            sb.append(", headers=[");
            boolean first = true;
            for (Map.Entry<String, String> header : headers.entrySet()) {
                if (!first) {
                    sb.append(", ");
                }
                sb.append("[").append(header.getKey()).append(": ").append(header.getValue()).append("]");
                first = false;
            }
            sb.append("], ");
        }
        if (auth != null) {
            sb.append("auth=[").append(auth.type()).append("], ");
        }
        sb.append("connection_timeout=[").append(connectionTimeout).append("], ");
        sb.append("read_timeout=[").append(readTimeout).append("], ");
        if (proxy != null) {
            sb.append("proxy=[").append(proxy).append("], ");
        }
        sb.append("body=[").append(body).append("], ");
        return sb.toString();
    }

    public static Builder builder(String host, int port) {
        return new Builder(host, port);
    }

    static Builder builder() {
        return new Builder();
    }

    public static class Parser {

        private final HttpAuthRegistry httpAuthRegistry;

        public Parser(HttpAuthRegistry httpAuthRegistry) {
            this.httpAuthRegistry = httpAuthRegistry;
        }

        public HttpRequest parse(XContentParser parser) throws IOException {
            Builder builder = new Builder();
            XContentParser.Token token;
            String currentFieldName = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (Field.PROXY.match(currentFieldName)) {
                    try {
                        builder.proxy(HttpProxy.parse(parser));
                    } catch (Exception e) {
                        throw new ElasticsearchParseException("could not parse http request. could not parse [{}] field", currentFieldName);
                    }
                } else if (Field.AUTH.match(currentFieldName)) {
                    builder.auth(httpAuthRegistry.parse(parser));
                } else if (HttpRequest.Field.CONNECTION_TIMEOUT.match(currentFieldName)) {
                    builder.connectionTimeout(TimeValue.timeValueMillis(parser.longValue()));
                } else if (HttpRequest.Field.CONNECTION_TIMEOUT_HUMAN.match(currentFieldName)) {
                    // Users and 2.x specify the timeout this way
                    try {
                        builder.connectionTimeout(WatcherDateTimeUtils.parseTimeValue(parser,
                                HttpRequest.Field.CONNECTION_TIMEOUT.toString()));
                    } catch (ElasticsearchParseException pe) {
                        throw new ElasticsearchParseException("could not parse http request template. invalid time value for [{}] field",
                                pe, currentFieldName);
                    }
                } else if (HttpRequest.Field.READ_TIMEOUT.match(currentFieldName)) {
                    builder.readTimeout(TimeValue.timeValueMillis(parser.longValue()));
                } else if (HttpRequest.Field.READ_TIMEOUT_HUMAN.match(currentFieldName)) {
                    // Users and 2.x specify the timeout this way
                    try {
                        builder.readTimeout(WatcherDateTimeUtils.parseTimeValue(parser, HttpRequest.Field.READ_TIMEOUT.toString()));
                    } catch (ElasticsearchParseException pe) {
                        throw new ElasticsearchParseException("could not parse http request template. invalid time value for [{}] field",
                                pe, currentFieldName);
                    }
                } else if (token == XContentParser.Token.START_OBJECT) {
                    if (Field.HEADERS.match(currentFieldName)) {
                        builder.setHeaders((Map) WatcherUtils.flattenModel(parser.map()));
                    } else if (Field.PARAMS.match(currentFieldName)) {
                        builder.setParams((Map) WatcherUtils.flattenModel(parser.map()));
                    }  else if (Field.BODY.match(currentFieldName)) {
                        builder.body(parser.text());
                    } else {
                        throw new ElasticsearchParseException("could not parse http request. unexpected object field [{}]",
                                currentFieldName);
                    }
                } else if (token == XContentParser.Token.VALUE_STRING) {
                    if (Field.SCHEME.match(currentFieldName)) {
                        builder.scheme(Scheme.parse(parser.text()));
                    } else if (Field.METHOD.match(currentFieldName)) {
                        builder.method(HttpMethod.parse(parser.text()));
                    } else if (Field.HOST.match(currentFieldName)) {
                        builder.host = parser.text();
                    } else if (Field.PATH.match(currentFieldName)) {
                        builder.path(parser.text());
                    } else if (Field.BODY.match(currentFieldName)) {
                        builder.body(parser.text());
                    } else if (Field.URL.match(currentFieldName)) {
                        builder.fromUrl(parser.text());
                    } else {
                        throw new ElasticsearchParseException("could not parse http request. unexpected string field [{}]",
                                currentFieldName);
                    }
                } else if (token == XContentParser.Token.VALUE_NUMBER) {
                    if (Field.PORT.match(currentFieldName)) {
                        builder.port = parser.intValue();
                    } else {
                        throw new ElasticsearchParseException("could not parse http request. unexpected numeric field [{}]",
                                currentFieldName);
                    }
                } else {
                    throw new ElasticsearchParseException("could not parse http request. unexpected token [{}]", token);
                }
            }

            if (builder.host == null) {
                throw new ElasticsearchParseException("could not parse http request. missing required [{}] field",
                        Field.HOST.getPreferredName());
            }

            if (builder.port < 0) {
                throw new ElasticsearchParseException("could not parse http request. missing required [{}] field",
                        Field.PORT.getPreferredName());
            }

            return builder.build();
        }
    }

    public static class Builder {

        private String host;
        private int port = -1;
        private Scheme scheme;
        private HttpMethod method;
        private String path;
        private Map<String, String> params = new HashMap<>();
        private Map<String, String> headers = new HashMap<>();
        private HttpAuth auth;
        private String body;
        private TimeValue connectionTimeout;
        private TimeValue readTimeout;
        private HttpProxy proxy;

        private Builder(String host, int port) {
            this.host = host;
            this.port = port;
        }

        private Builder() {
        }

        public Builder scheme(Scheme scheme) {
            this.scheme = scheme;
            return this;
        }

        public Builder method(HttpMethod method) {
            this.method = method;
            return this;
        }

        public Builder path(String path) {
            this.path = path;
            return this;
        }

        public Builder setParams(Map<String, String> params) {
            if (this.params == null) {
                throw new IllegalStateException("Request has already been built!");
            }
            this.params.putAll(params);
            return this;
        }

        public Builder setParam(String key, String value) {
            if (params == null) {
                throw new IllegalStateException("Request has already been built!");
            }
            this.params.put(key, value);
            return this;
        }

        public Builder setHeaders(Map<String, String> headers) {
            if (this.headers == null) {
                throw new IllegalStateException("Request has already been built!");
            }
            this.headers.putAll(headers);
            return this;
        }

        public Builder setHeader(String key, String value) {
            if (headers == null) {
                throw new IllegalStateException("Request has already been built!");
            }
            this.headers.put(key, value);
            return this;
        }

        public Builder auth(HttpAuth auth) {
            this.auth = auth;
            return this;
        }

        public Builder body(String body) {
            this.body = body;
            return this;
        }

        public Builder jsonBody(ToXContent xContent) {
            return body(Strings.toString(xContent)).setHeader("Content-Type", XContentType.JSON.mediaType());
        }

        public Builder connectionTimeout(TimeValue timeout) {
            this.connectionTimeout = timeout;
            return this;
        }

        public Builder readTimeout(TimeValue timeout) {
            this.readTimeout = timeout;
            return this;
        }

        public Builder proxy(HttpProxy proxy) {
            this.proxy = proxy;
            return this;
        }

        public HttpRequest build() {
            HttpRequest request = new HttpRequest(host, port, scheme, method, path, unmodifiableMap(params), unmodifiableMap(headers),
                    auth, body, connectionTimeout, readTimeout, proxy);
            params = null;
            headers = null;
            return request;
        }

        public Builder fromUrl(String supposedUrl) {
            if (Strings.hasLength(supposedUrl) == false) {
                throw new ElasticsearchParseException("Configured URL is empty, please configure a valid URL");
            }

            try {
                URI uri = new URI(supposedUrl);
                if (Strings.hasLength(uri.getScheme()) == false) {
                    throw new ElasticsearchParseException("URL [{}] does not contain a scheme", uri);
                }
                scheme = Scheme.parse(uri.getScheme());
                port = uri.getPort() > 0 ? uri.getPort() : scheme.defaultPort();
                host = uri.getHost();
                if (Strings.hasLength(uri.getRawPath())) {
                    path = uri.getRawPath();
                }
                String rawQuery = uri.getRawQuery();
                if (Strings.hasLength(rawQuery)) {
                    RestUtils.decodeQueryString(rawQuery, 0, params);
                }
            } catch (URISyntaxException e) {
                throw new ElasticsearchParseException("Malformed URL [{}]", supposedUrl);
            }
            return this;
        }
    }

    public interface Field {
        ParseField SCHEME = new ParseField("scheme");
        ParseField HOST = new ParseField("host");
        ParseField PORT = new ParseField("port");
        ParseField METHOD = new ParseField("method");
        ParseField PATH = new ParseField("path");
        ParseField PARAMS = new ParseField("params");
        ParseField HEADERS = new ParseField("headers");
        ParseField AUTH = new ParseField("auth");
        ParseField BODY = new ParseField("body");
        ParseField CONNECTION_TIMEOUT = new ParseField("connection_timeout_in_millis");
        ParseField CONNECTION_TIMEOUT_HUMAN = new ParseField("connection_timeout");
        ParseField READ_TIMEOUT = new ParseField("read_timeout_millis");
        ParseField READ_TIMEOUT_HUMAN = new ParseField("read_timeout");
        ParseField PROXY = new ParseField("proxy");
        ParseField URL = new ParseField("url");
    }
}
