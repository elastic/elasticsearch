/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.support.http;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.watcher.WatcherException;
import org.elasticsearch.watcher.support.http.auth.HttpAuth;
import org.elasticsearch.watcher.support.http.auth.HttpAuthRegistry;
import org.elasticsearch.watcher.support.template.Template;
import org.elasticsearch.watcher.support.template.TemplateEngine;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 */
public class HttpRequestTemplate implements ToXContent {

    private final Scheme scheme;
    private final String host;
    private final int port;
    private final HttpMethod method;
    private final Template path;
    private final ImmutableMap<String, Template> params;
    private final ImmutableMap<String, Template> headers;
    private final HttpAuth auth;
    private final Template body;

    public HttpRequestTemplate(String host, int port, @Nullable Scheme scheme, @Nullable HttpMethod method, @Nullable Template path,
                               Map<String, Template> params, Map<String, Template> headers, HttpAuth auth,
                               Template body) {
        this.host = host;
        this.port = port;
        this.scheme = scheme != null ? scheme :Scheme.HTTP;
        this.method = method != null ? method : HttpMethod.GET;
        this.path = path;
        this.params = params != null ? ImmutableMap.copyOf(params) : ImmutableMap.<String, Template>of();
        this.headers = headers != null ? ImmutableMap.copyOf(headers) : ImmutableMap.<String, Template>of();
        this.auth = auth;
        this.body = body;
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

    public Template path() {
        return path;
    }

    public Map<String, Template> params() {
        return params;
    }

    public Map<String, Template> headers() {
        return headers;
    }

    public HttpAuth auth() {
        return auth;
    }

    public Template body() {
        return body;
    }

    public HttpRequest render(TemplateEngine engine, Map<String, Object> model) {
        HttpRequest.Builder request = HttpRequest.builder(host, port);
        request.method(method);
        request.scheme(scheme);
        if (path != null) {
            request.path(engine.render(path, model));
        }
        if (params != null && !params.isEmpty()) {
            MapBuilder<String, String> mapBuilder = MapBuilder.newMapBuilder();
            for (Map.Entry<String, Template> entry : params.entrySet()) {
                mapBuilder.put(entry.getKey(), engine.render(entry.getValue(), model));
            }
            request.setParams(mapBuilder.map());
        }
        if (headers != null && !headers.isEmpty()) {
            MapBuilder<String, String> mapBuilder = MapBuilder.newMapBuilder();
            for (Map.Entry<String, Template> entry : headers.entrySet()) {
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
        return request.build();
    }

    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject();
        builder.field(Parser.SCHEME_FIELD.getPreferredName(), scheme, params);
        builder.field(Parser.HOST_FIELD.getPreferredName(), host);
        builder.field(Parser.PORT_FIELD.getPreferredName(), port);
        builder.field(Parser.METHOD_FIELD.getPreferredName(), method, params);
        if (path != null) {
            builder.field(Parser.PATH_FIELD.getPreferredName(), path, params);
        }
        if (this.params != null) {
            builder.startObject(Parser.PARAMS_FIELD.getPreferredName());
            for (Map.Entry<String, Template> entry : this.params.entrySet()) {
                builder.field(entry.getKey(), entry.getValue(), params);
            }
            builder.endObject();
        }
        if (headers != null) {
            builder.startObject(Parser.HEADERS_FIELD.getPreferredName());
            for (Map.Entry<String, Template> entry : headers.entrySet()) {
                builder.field(entry.getKey(), entry.getValue(), params);
            }
            builder.endObject();
        }
        if (auth != null) {
            builder.startObject(Parser.AUTH_FIELD.getPreferredName())
                    .field(auth.type(), auth, params)
                    .endObject();
        }
        if (body != null) {
            builder.field(Parser.BODY_FIELD.getPreferredName(), body, params);
        }
        return builder.endObject();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        HttpRequestTemplate that = (HttpRequestTemplate) o;

        if (port != that.port) return false;
        if (auth != null ? !auth.equals(that.auth) : that.auth != null) return false;
        if (body != null ? !body.equals(that.body) : that.body != null) return false;
        if (headers != null ? !headers.equals(that.headers) : that.headers != null) return false;
        if (host != null ? !host.equals(that.host) : that.host != null) return false;
        if (method != that.method) return false;
        if (params != null ? !params.equals(that.params) : that.params != null) return false;
        if (path != null ? !path.equals(that.path) : that.path != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = host != null ? host.hashCode() : 0;
        result = 31 * result + port;
        result = 31 * result + (method != null ? method.hashCode() : 0);
        result = 31 * result + (path != null ? path.hashCode() : 0);
        result = 31 * result + (params != null ? params.hashCode() : 0);
        result = 31 * result + (headers != null ? headers.hashCode() : 0);
        result = 31 * result + (auth != null ? auth.hashCode() : 0);
        result = 31 * result + (body != null ? body.hashCode() : 0);
        return result;
    }

    public static Builder builder(String host, int port) {
        return new Builder(host, port);
    }

    public static class Parser {

        public static final ParseField SCHEME_FIELD = new ParseField("scheme");
        public static final ParseField HOST_FIELD = new ParseField("host");
        public static final ParseField PORT_FIELD = new ParseField("port");
        public static final ParseField METHOD_FIELD = new ParseField("method");
        public static final ParseField PATH_FIELD = new ParseField("path");
        public static final ParseField PARAMS_FIELD = new ParseField("params");
        public static final ParseField HEADERS_FIELD = new ParseField("headers");
        public static final ParseField AUTH_FIELD = new ParseField("auth");
        public static final ParseField BODY_FIELD = new ParseField("body");

        private final HttpAuthRegistry httpAuthRegistry;

        @Inject
        public Parser(HttpAuthRegistry httpAuthRegistry) {
            this.httpAuthRegistry = httpAuthRegistry;
        }

        public HttpRequestTemplate parse(XContentParser parser) throws IOException {
            assert parser.currentToken() == XContentParser.Token.START_OBJECT;

            Builder builder = new Builder();
            XContentParser.Token token;
            String currentFieldName = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (PATH_FIELD.match(currentFieldName)) {
                    builder.path(parseFieldTemplate(currentFieldName, parser));
                } else if (HEADERS_FIELD.match(currentFieldName)) {
                    builder.putHeaders(parseFieldTemplates(currentFieldName, parser));
                } else if (PARAMS_FIELD.match(currentFieldName)) {
                    builder.putParams(parseFieldTemplates(currentFieldName, parser));
                } else if (BODY_FIELD.match(currentFieldName)) {
                    builder.body(parseFieldTemplate(currentFieldName, parser));
                } else if (token == XContentParser.Token.START_OBJECT) {
                    if (AUTH_FIELD.match(currentFieldName)) {
                        builder.auth(httpAuthRegistry.parse(parser));
                    }  else {
                        throw new ParseException("could not parse http request template. unexpected object field [" + currentFieldName + "]");
                    }
                } else if (token == XContentParser.Token.VALUE_STRING) {
                    if (SCHEME_FIELD.match(currentFieldName)) {
                        builder.scheme(Scheme.parse(parser.text()));
                    } else if (METHOD_FIELD.match(currentFieldName)) {
                        builder.method(HttpMethod.parse(parser.text()));
                    } else if (HOST_FIELD.match(currentFieldName)) {
                        builder.host = parser.text();
                    } else {
                        throw new ParseException("could not parse http request template. unexpected string field [" + currentFieldName + "]");
                    }
                } else if (token == XContentParser.Token.VALUE_NUMBER) {
                    if (PORT_FIELD.match(currentFieldName)) {
                        builder.port = parser.intValue();
                    } else {
                        throw new ParseException("could not parse http request template. unexpected numeric field [" + currentFieldName + "]");
                    }
                } else {
                    throw new ParseException("could not parse http request template. unexpected token [" + token + "] for field [" + currentFieldName + "]");
                }
            }

            if (builder.host == null) {
                throw new ParseException("could not parse http request template. missing required [host] string field");
            }
            if (builder.port <= 0) {
                throw new ParseException("could not parse http request template. missing required [port] numeric field");
            }

            return builder.build();
        }

        private static Template parseFieldTemplate(String field, XContentParser parser) throws IOException {
            try {
                return Template.parse(parser);
            } catch (Template.ParseException pe) {
                throw new ParseException("could not parse http request template. could not parse value for  [" + field + "] field", pe);
            }
        }

        private static Map<String, Template> parseFieldTemplates(String field, XContentParser parser) throws IOException {
            Map<String, Template> templates = new HashMap<>();

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

    public static class ParseException extends WatcherException {

        public ParseException(String msg) {
            super(msg);
        }

        public ParseException(String msg, Throwable cause) {
            super(msg, cause);
        }
    }

    public static class Builder {

        private String host;
        private int port;
        private Scheme scheme;
        private HttpMethod method;
        private Template path;
        private final ImmutableMap.Builder<String, Template> params = ImmutableMap.builder();
        private final ImmutableMap.Builder<String, Template> headers = ImmutableMap.builder();
        private HttpAuth auth;
        private Template body;

        private Builder() {
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
            return path(new Template(path));
        }

        public Builder path(Template path) {
            this.path = path;
            return this;
        }

        public Builder putParams(Map<String, Template> params) {
            this.params.putAll(params);
            return this;
        }

        public Builder putParam(String key, Template value) {
            this.params.put(key, value);
            return this;
        }

        public Builder putHeaders(Map<String, Template> headers) {
            this.headers.putAll(headers);
            return this;
        }

        public Builder putHeader(String key, Template value) {
            this.headers.put(key, value);
            return this;
        }

        public Builder auth(HttpAuth auth) {
            this.auth = auth;
            return this;
        }

        public Builder body(String body) {
            return body(new Template(body));
        }

        public Builder body(Template body) {
            this.body = body;
            return this;
        }

        public Builder body(ToXContent content) {
            try {
                return body(jsonBuilder().value(content));
            } catch (IOException ioe) {
                throw new WatcherException("could not set http input body to given xcontent", ioe);
            }
        }

        public Builder body(XContentBuilder content) {
            return body(content.bytes().toUtf8());
        }

        public HttpRequestTemplate build() {
            return new HttpRequestTemplate(host, port, scheme, method, path, params.build(), headers.build(), auth, body);
        }
    }

}
