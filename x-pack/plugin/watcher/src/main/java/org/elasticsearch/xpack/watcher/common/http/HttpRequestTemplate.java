/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.common.http;

import io.netty.handler.codec.http.HttpHeaders;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.rest.RestUtils;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.watcher.support.WatcherDateTimeUtils;
import org.elasticsearch.xpack.core.watcher.support.xcontent.WatcherParams;
import org.elasticsearch.xpack.core.watcher.support.xcontent.WatcherXContentParser;
import org.elasticsearch.xpack.watcher.common.text.TextTemplate;
import org.elasticsearch.xpack.watcher.common.text.TextTemplateEngine;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;

public class HttpRequestTemplate implements ToXContentObject {

    private final Scheme scheme;
    private final String host;
    private final int port;
    private final HttpMethod method;
    private final TextTemplate path;
    private final Map<String, TextTemplate> params;
    private final Map<String, TextTemplate> headers;
    private final BasicAuth auth;
    private final TextTemplate body;
    @Nullable
    private final TimeValue connectionTimeout;
    @Nullable
    private final TimeValue readTimeout;
    @Nullable
    private final HttpProxy proxy;

    public HttpRequestTemplate(
        String host,
        int port,
        @Nullable Scheme scheme,
        @Nullable HttpMethod method,
        @Nullable TextTemplate path,
        Map<String, TextTemplate> params,
        Map<String, TextTemplate> headers,
        BasicAuth auth,
        TextTemplate body,
        @Nullable TimeValue connectionTimeout,
        @Nullable TimeValue readTimeout,
        @Nullable HttpProxy proxy
    ) {
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

    public TextTemplate path() {
        return path;
    }

    public Map<String, TextTemplate> params() {
        return params;
    }

    public Map<String, TextTemplate> headers() {
        return headers;
    }

    public BasicAuth auth() {
        return auth;
    }

    public TextTemplate body() {
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

    public HttpRequest render(TextTemplateEngine engine, Map<String, Object> model) {
        HttpRequest.Builder request = HttpRequest.builder(host, port);
        request.method(method);
        request.scheme(scheme);
        if (path != null) {
            request.path(engine.render(path, model));
        }
        if (params != null && params.isEmpty() == false) {
            MapBuilder<String, String> mapBuilder = MapBuilder.newMapBuilder();
            for (Map.Entry<String, TextTemplate> entry : params.entrySet()) {
                mapBuilder.put(entry.getKey(), engine.render(entry.getValue(), model));
            }
            request.setParams(mapBuilder.map());
        }
        if ((headers == null || headers.isEmpty()) && body != null && body.getContentType() != null) {
            request.setHeaders(singletonMap(HttpHeaders.Names.CONTENT_TYPE, body.getContentType().mediaType()));
        } else if (headers != null && headers.isEmpty() == false) {
            MapBuilder<String, String> mapBuilder = MapBuilder.newMapBuilder();
            if (body != null && body.getContentType() != null) {
                // putting the content type first, so it can be overridden by custom headers
                mapBuilder.put(HttpHeaders.Names.CONTENT_TYPE, body.getContentType().mediaType());
            }
            for (Map.Entry<String, TextTemplate> entry : headers.entrySet()) {
                mapBuilder.put(entry.getKey(), engine.render(entry.getValue(), model));
            }
            request.setHeaders(mapBuilder.map());
        }
        if (auth != null) {
            request.auth(auth);
        }
        if (body != null) {
            request.body(engine.render(body, model));
        }
        if (connectionTimeout != null) {
            request.connectionTimeout(connectionTimeout);
        }
        if (readTimeout != null) {
            request.readTimeout(readTimeout);
        }
        if (proxy != null) {
            request.proxy(proxy);
        }
        return request.build();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject();
        builder.field(HttpRequest.Field.SCHEME.getPreferredName(), scheme.value());
        builder.field(HttpRequest.Field.HOST.getPreferredName(), host);
        builder.field(HttpRequest.Field.PORT.getPreferredName(), port);
        builder.field(HttpRequest.Field.METHOD.getPreferredName(), method.value());
        if (path != null) {
            builder.field(HttpRequest.Field.PATH.getPreferredName(), path, params);
        }
        if (this.params != null) {
            builder.startObject(HttpRequest.Field.PARAMS.getPreferredName());
            for (Map.Entry<String, TextTemplate> entry : this.params.entrySet()) {
                builder.field(entry.getKey(), entry.getValue(), params);
            }
            builder.endObject();
        }
        if (headers != null) {
            builder.startObject(HttpRequest.Field.HEADERS.getPreferredName());
            for (Map.Entry<String, TextTemplate> entry : headers.entrySet()) {
                String key = entry.getKey();
                if (WatcherParams.hideSecrets(params) && "Authorization".equalsIgnoreCase(key)) {
                    builder.field(key, WatcherXContentParser.REDACTED_PASSWORD);
                } else {
                    builder.field(key, entry.getValue(), params);
                }
            }
            builder.endObject();
        }
        if (auth != null) {
            builder.startObject(HttpRequest.Field.AUTH.getPreferredName()).field(BasicAuth.TYPE, auth, params).endObject();
        }
        if (body != null) {
            builder.field(HttpRequest.Field.BODY.getPreferredName(), body, params);
        }
        if (connectionTimeout != null) {
            builder.humanReadableField(
                HttpRequest.Field.CONNECTION_TIMEOUT.getPreferredName(),
                HttpRequest.Field.CONNECTION_TIMEOUT_HUMAN.getPreferredName(),
                connectionTimeout
            );
        }
        if (readTimeout != null) {
            builder.humanReadableField(
                HttpRequest.Field.READ_TIMEOUT.getPreferredName(),
                HttpRequest.Field.READ_TIMEOUT_HUMAN.getPreferredName(),
                readTimeout
            );
        }
        if (proxy != null) {
            proxy.toXContent(builder, params);
        }
        return builder.endObject();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        HttpRequestTemplate that = (HttpRequestTemplate) o;
        return port == that.port
            && scheme == that.scheme
            && Objects.equals(host, that.host)
            && method == that.method
            && Objects.equals(path, that.path)
            && Objects.equals(params, that.params)
            && Objects.equals(headers, that.headers)
            && Objects.equals(auth, that.auth)
            && Objects.equals(connectionTimeout, that.connectionTimeout)
            && Objects.equals(readTimeout, that.readTimeout)
            && Objects.equals(proxy, that.proxy)
            && Objects.equals(body, that.body);
    }

    @Override
    public int hashCode() {
        return Objects.hash(scheme, host, port, method, path, params, headers, auth, body, connectionTimeout, readTimeout, proxy);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    public static Builder builder(String host, int port) {
        return new Builder(host, port);
    }

    public static Builder builder(String url) {
        return new Builder(url);
    }

    static Builder builder() {
        return new Builder();
    }

    public static class Parser {
        public static HttpRequestTemplate parse(XContentParser parser) throws IOException {
            assert parser.currentToken() == XContentParser.Token.START_OBJECT;

            Builder builder = new Builder();
            XContentParser.Token token;
            String currentFieldName = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (HttpRequest.Field.PROXY.match(currentFieldName, parser.getDeprecationHandler())) {
                    builder.proxy(HttpProxy.parse(parser));
                } else if (HttpRequest.Field.PATH.match(currentFieldName, parser.getDeprecationHandler())) {
                    builder.path(parseFieldTemplate(currentFieldName, parser));
                } else if (HttpRequest.Field.HEADERS.match(currentFieldName, parser.getDeprecationHandler())) {
                    builder.putHeaders(parseFieldTemplates(currentFieldName, parser));
                } else if (HttpRequest.Field.PARAMS.match(currentFieldName, parser.getDeprecationHandler())) {
                    builder.putParams(parseFieldTemplates(currentFieldName, parser));
                } else if (HttpRequest.Field.BODY.match(currentFieldName, parser.getDeprecationHandler())) {
                    builder.body(parseFieldTemplate(currentFieldName, parser));
                } else if (HttpRequest.Field.URL.match(currentFieldName, parser.getDeprecationHandler())) {
                    builder.fromUrl(parser.text());
                } else if (HttpRequest.Field.CONNECTION_TIMEOUT.match(currentFieldName, parser.getDeprecationHandler())) {
                    builder.connectionTimeout(TimeValue.timeValueMillis(parser.longValue()));
                } else if (HttpRequest.Field.CONNECTION_TIMEOUT_HUMAN.match(currentFieldName, parser.getDeprecationHandler())) {
                    // Users and 2.x specify the timeout this way
                    try {
                        builder.connectionTimeout(
                            WatcherDateTimeUtils.parseTimeValue(parser, HttpRequest.Field.CONNECTION_TIMEOUT.toString())
                        );
                    } catch (ElasticsearchParseException pe) {
                        throw new ElasticsearchParseException(
                            "could not parse http request template. invalid time value for [{}] field",
                            pe,
                            currentFieldName
                        );
                    }
                } else if (HttpRequest.Field.READ_TIMEOUT.match(currentFieldName, parser.getDeprecationHandler())) {
                    builder.readTimeout(TimeValue.timeValueMillis(parser.longValue()));
                } else if (HttpRequest.Field.READ_TIMEOUT_HUMAN.match(currentFieldName, parser.getDeprecationHandler())) {
                    // Users and 2.x specify the timeout this way
                    try {
                        builder.readTimeout(WatcherDateTimeUtils.parseTimeValue(parser, HttpRequest.Field.READ_TIMEOUT.toString()));
                    } catch (ElasticsearchParseException pe) {
                        throw new ElasticsearchParseException(
                            "could not parse http request template. invalid time value for [{}] field",
                            pe,
                            currentFieldName
                        );
                    }
                } else if (token == XContentParser.Token.START_OBJECT) {
                    if (HttpRequest.Field.AUTH.match(currentFieldName, parser.getDeprecationHandler())) {
                        builder.auth(BasicAuth.parse(parser));
                    } else {
                        throw new ElasticsearchParseException(
                            "could not parse http request template. unexpected object field [{}]",
                            currentFieldName
                        );
                    }
                } else if (token == XContentParser.Token.VALUE_STRING) {
                    if (HttpRequest.Field.SCHEME.match(currentFieldName, parser.getDeprecationHandler())) {
                        builder.scheme(Scheme.parse(parser.text()));
                    } else if (HttpRequest.Field.METHOD.match(currentFieldName, parser.getDeprecationHandler())) {
                        builder.method(HttpMethod.parse(parser.text()));
                    } else if (HttpRequest.Field.HOST.match(currentFieldName, parser.getDeprecationHandler())) {
                        builder.host = parser.text();
                    } else {
                        throw new ElasticsearchParseException(
                            "could not parse http request template. unexpected string field [{}]",
                            currentFieldName
                        );
                    }
                } else if (token == XContentParser.Token.VALUE_NUMBER) {
                    if (HttpRequest.Field.PORT.match(currentFieldName, parser.getDeprecationHandler())) {
                        builder.port = parser.intValue();
                    } else {
                        throw new ElasticsearchParseException(
                            "could not parse http request template. unexpected numeric field [{}]",
                            currentFieldName
                        );
                    }
                } else {
                    throw new ElasticsearchParseException(
                        "could not parse http request template. unexpected token [{}] for field [{}]",
                        token,
                        currentFieldName
                    );
                }
            }

            if (builder.host == null) {
                throw new ElasticsearchParseException(
                    "could not parse http request template. missing required [{}] string field",
                    HttpRequest.Field.HOST.getPreferredName()
                );
            }
            if (builder.port <= 0) {
                throw new ElasticsearchParseException(
                    "could not parse http request template. wrong port for [{}]",
                    HttpRequest.Field.PORT.getPreferredName()
                );
            }

            return builder.build();
        }

        private static TextTemplate parseFieldTemplate(String field, XContentParser parser) throws IOException {
            try {
                return TextTemplate.parse(parser);
            } catch (ElasticsearchParseException pe) {
                throw new ElasticsearchParseException(
                    "could not parse http request template. could not parse value for [{}] field",
                    pe,
                    field
                );
            }
        }

        private static Map<String, TextTemplate> parseFieldTemplates(String field, XContentParser parser) throws IOException {
            Map<String, TextTemplate> templates = new HashMap<>();

            String currentFieldName = null;
            XContentParser.Token token;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else {
                    templates.put(currentFieldName, parseFieldTemplate(field, parser));
                }
            }
            return templates;
        }
    }

    public static class Builder {

        private String host;
        private int port;
        private Scheme scheme;
        private HttpMethod method;
        private TextTemplate path;
        private final Map<String, TextTemplate> params = new HashMap<>();
        private final Map<String, TextTemplate> headers = new HashMap<>();
        private BasicAuth auth;
        private TextTemplate body;
        private TimeValue connectionTimeout;
        private TimeValue readTimeout;
        private HttpProxy proxy;

        private Builder() {}

        private Builder(String url) {
            fromUrl(url);
        }

        private Builder(String host, int port) {
            this.host = host;
            this.port = port;
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
            return path(new TextTemplate(path));
        }

        public Builder path(TextTemplate path) {
            this.path = path;
            return this;
        }

        public Builder putParams(Map<String, TextTemplate> params) {
            this.params.putAll(params);
            return this;
        }

        public Builder putParam(String key, TextTemplate value) {
            this.params.put(key, value);
            return this;
        }

        public Builder putHeaders(Map<String, TextTemplate> headers) {
            this.headers.putAll(headers);
            return this;
        }

        public Builder putHeader(String key, TextTemplate value) {
            this.headers.put(key, value);
            return this;
        }

        public Builder auth(BasicAuth auth) {
            this.auth = auth;
            return this;
        }

        public Builder body(String body) {
            return body(new TextTemplate(body));
        }

        public Builder body(TextTemplate body) {
            this.body = body;
            return this;
        }

        public Builder body(XContentBuilder content) throws IOException {
            return body(new TextTemplate(Strings.toString(content), content.contentType(), ScriptType.INLINE, null));
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

        public HttpRequestTemplate build() {
            return new HttpRequestTemplate(
                host,
                port,
                scheme,
                method,
                path,
                Map.copyOf(params),
                Map.copyOf(headers),
                auth,
                body,
                connectionTimeout,
                readTimeout,
                proxy
            );
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
                if (Strings.hasLength(uri.getPath())) {
                    path = new TextTemplate(uri.getPath());
                }

                String rawQuery = uri.getRawQuery();
                if (Strings.hasLength(rawQuery)) {
                    Map<String, String> stringParams = new HashMap<>();
                    RestUtils.decodeQueryString(rawQuery, 0, stringParams);
                    for (Map.Entry<String, String> entry : stringParams.entrySet()) {
                        params.put(entry.getKey(), new TextTemplate(entry.getValue()));
                    }
                }
            } catch (URISyntaxException e) {
                throw new ElasticsearchParseException("Malformed URL [{}]", supposedUrl);
            }
            return this;
        }
    }

}
