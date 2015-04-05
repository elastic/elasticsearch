/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.support.http;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.watcher.support.WatcherUtils;
import org.elasticsearch.watcher.support.http.auth.HttpAuth;
import org.elasticsearch.watcher.support.http.auth.HttpAuthRegistry;
import org.elasticsearch.watcher.support.template.Template;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class HttpRequest implements ToXContent {

    private Scheme scheme;
    private String host;
    private int port;
    private HttpMethod method;
    private String path;
    private Map<String, String> params;
    private Map<String, String> headers;
    private HttpAuth auth;
    private String body;

    public HttpRequest() {
        scheme = Scheme.HTTP;
        method = HttpMethod.GET;
    }

    public Scheme scheme() {
        return scheme;
    }

    public void scheme(Scheme scheme) {
        this.scheme = scheme;
    }

    public String host() {
        return host;
    }

    public void host(String host) {
        this.host = host;
    }

    public int port() {
        return port;
    }

    public void port(int port) {
        this.port = port;
    }

    public HttpMethod method() {
        return method;
    }

    public void method(HttpMethod method) {
        this.method = method;
    }

    public String path() {
        return path;
    }

    public void path(String path) {
        this.path = path;
    }

    public Map<String, String> params() {
        return params;
    }

    public void params(Map<String, String> params) {
        this.params = params;
    }

    public Map<String, String> headers() {
        return headers;
    }

    public void headers(Map<String, String> headers) {
        this.headers = headers;
    }

    public HttpAuth auth() {
        return auth;
    }

    public void auth(HttpAuth auth) {
        this.auth = auth;
    }

    public String body() {
        return body;
    }

    public void body(String body) {
        this.body = body;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject();
        builder.field(Parser.HOST_FIELD.getPreferredName(), host);
        builder.field(Parser.PORT_FIELD.getPreferredName(), port);
        builder.field(Parser.METHOD_FIELD.getPreferredName(), method);
        if (path != null) {
            builder.field(Parser.PATH_FIELD.getPreferredName(), path);
        }
        if (this.params != null) {
            builder.field(Parser.PARAMS_FIELD.getPreferredName(), this.params);
        }
        if (headers != null) {
            builder.field(Parser.HEADERS_FIELD.getPreferredName(), headers);
        }
        if (auth != null) {
            builder.field(Parser.AUTH_FIELD.getPreferredName(), auth);
        }
        if (body != null) {
            builder.field(Parser.BODY_FIELD.getPreferredName(), body);
        }
        return builder.endObject();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        HttpRequest request = (HttpRequest) o;

        if (scheme != request.scheme) return false;
        if (port != request.port) return false;
        if (auth != null ? !auth.equals(request.auth) : request.auth != null) return false;
        if (body != null ? !body.equals(request.body) : request.body != null) return false;
        if (headers != null ? !headers.equals(request.headers) : request.headers != null) return false;
        if (!host.equals(request.host)) return false;
        if (method != request.method) return false;
        if (params != null ? !params.equals(request.params) : request.params != null) return false;
        if (path != null ? !path.equals(request.path) : request.path != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = host.hashCode();
        result = 31 * result + port;
        result = 31 * result + method.hashCode();
        result = 31 * result + scheme.hashCode();
        result = 31 * result + (path != null ? path.hashCode() : 0);
        result = 31 * result + (params != null ? params.hashCode() : 0);
        result = 31 * result + (headers != null ? headers.hashCode() : 0);
        result = 31 * result + (auth != null ? auth.hashCode() : 0);
        result = 31 * result + (body != null ? body.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "HttpRequest{" +
                "auth=[" + (auth != null ? "******" : null) +
                "], body=[" + body + '\'' +
                "], path=[" + path + '\'' +
                "], method=[" + method +
                "], port=[" + port +
                "], host=[" + host + '\'' +
                "]}";
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

        public HttpRequest parse(XContentParser parser) throws IOException {
            HttpRequest request = new HttpRequest();
            XContentParser.Token token;
            String currentFieldName = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token == XContentParser.Token.START_OBJECT) {
                    if (HEADERS_FIELD.match(currentFieldName)) {
                        request.headers((Map) WatcherUtils.flattenModel(parser.map()));
                    } else if (PARAMS_FIELD.match(currentFieldName)) {
                        request.params((Map) WatcherUtils.flattenModel(parser.map()));
                    }  else if (AUTH_FIELD.match(currentFieldName)) {
                        request.auth(httpAuthRegistry.parse(parser));
                    } else if (BODY_FIELD.match(currentFieldName)) {
                        request.body(parser.text());
                    } else {
                        throw new ElasticsearchParseException("could not parse http request. unexpected field [" + currentFieldName + "]");
                    }
                } else if (token == XContentParser.Token.VALUE_STRING) {
                    if (SCHEME_FIELD.match(currentFieldName)) {
                        request.scheme(Scheme.parse(parser.text()));
                    } else if (METHOD_FIELD.match(currentFieldName)) {
                        request.method(HttpMethod.parse(parser.text()));
                    } else if (HOST_FIELD.match(currentFieldName)) {
                        request.host(parser.text());
                    } else if (PATH_FIELD.match(currentFieldName)) {
                        request.path(parser.text());
                    } else if (BODY_FIELD.match(currentFieldName)) {
                        request.body(parser.text());
                    } else {
                        throw new ElasticsearchParseException("could not parse http request. unexpected field [" + currentFieldName + "]");
                    }
                } else if (token == XContentParser.Token.VALUE_NUMBER) {
                    if (PORT_FIELD.match(currentFieldName)) {
                        request.port(parser.intValue());
                    } else {
                        throw new ElasticsearchParseException("could not parse http request. unexpected field [" + currentFieldName + "]");
                    }
                } else {
                    throw new ElasticsearchParseException("could not parse http request. unexpected token [" + token + "] for field [" + currentFieldName + "]");
                }
            }
            return request;
        }

    }

    public final static class SourceBuilder implements ToXContent {

        private String scheme;
        private String host;
        private int port;
        private String method;
        private Template path;
        private Map<String, Template.SourceBuilder> params = new HashMap<>();
        private Map<String, Template.SourceBuilder> headers = new HashMap<>();
        private HttpAuth auth;
        private Template body;

        public SourceBuilder setScheme(String scheme) {
            this.scheme = scheme;
            return this;
        }

        public SourceBuilder setHost(String host) {
            this.host = host;
            return this;
        }

        public SourceBuilder setPort(int port) {
            this.port = port;
            return this;
        }

        public SourceBuilder setMethod(String method) {
            this.method = method;
            return this;
        }

        public SourceBuilder setPath(Template path) {
            this.path = path;
            return this;
        }

        public SourceBuilder setParams(Map<String, Template.SourceBuilder> params) {
            this.params = params;
            return this;
        }

        public SourceBuilder putParams(Map<String, Template.SourceBuilder> params) {
            this.params.putAll(params);
            return this;
        }

        public SourceBuilder putParam(String key, Template.SourceBuilder value) {
            this.params.put(key, value);
            return this;
        }

        public SourceBuilder setHeaders(Map<String, Template.SourceBuilder> headers) {
            this.headers = headers;
            return this;
        }

        public SourceBuilder putHeaders(Map<String, Template.SourceBuilder> headers) {
            this.headers.putAll(headers);
            return this;
        }

        public SourceBuilder putHeader(String key, Template.SourceBuilder value) {
            this.headers.put(key, value);
            return this;
        }

        public SourceBuilder setAuth(HttpAuth auth) {
            this.auth = auth;
            return this;
        }

        public SourceBuilder setBody(Template body) {
            this.body = body;
            return this;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params p) throws IOException {
            builder.startObject();
            if (scheme != null) {
                builder.field(Parser.SCHEME_FIELD.getPreferredName(), scheme);
            }
            builder.field(HttpRequest.Parser.HOST_FIELD.getPreferredName(), host);
            builder.field(HttpRequest.Parser.PORT_FIELD.getPreferredName(), port);
            if (method != null) {
                builder.field(HttpRequest.Parser.METHOD_FIELD.getPreferredName(), method);
            }
            if (path != null) {
                builder.field(HttpRequest.Parser.PATH_FIELD.getPreferredName(), path);
            }
            if (params != null) {
                builder.field(HttpRequest.Parser.PARAMS_FIELD.getPreferredName(), params);
            }
            if (headers != null) {
                builder.field(HttpRequest.Parser.HEADERS_FIELD.getPreferredName(), headers);
            }
            if (auth != null) {
                builder.field(HttpRequest.Parser.AUTH_FIELD.getPreferredName(), auth);
            }
            if (body != null) {
                builder.field(HttpRequest.Parser.BODY_FIELD.getPreferredName(), body);
            }
            return builder.endObject();
        }
    }

}
