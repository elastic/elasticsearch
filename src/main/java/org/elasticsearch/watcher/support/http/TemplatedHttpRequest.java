/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.support.http;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.watcher.support.http.auth.HttpAuth;
import org.elasticsearch.watcher.support.http.auth.HttpAuthRegistry;
import org.elasticsearch.watcher.support.template.Template;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 */
public class TemplatedHttpRequest implements ToXContent {

    private Scheme scheme;
    private String host;
    private int port;
    private HttpMethod method;
    private Template path;
    private Map<String, Template> params;
    private Map<String, Template> headers;
    private HttpAuth auth;

    private Template body;

    public TemplatedHttpRequest() {
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

    public Template path() {
        return path;
    }

    public void path(Template path) {
        this.path = path;
    }

    public Map<String, Template> params() {
        return params;
    }

    public void params(Map<String, Template> params) {
        this.params = params;
    }

    public Map<String, Template> headers() {
        return headers;
    }

    public void headers(Map<String, Template> headers) {
        this.headers = headers;
    }

    public HttpAuth auth() {
        return auth;
    }

    public void auth(HttpAuth auth) {
        this.auth = auth;
    }

    public Template body() {
        return body;
    }

    public void body(Template body) {
        this.body = body;
    }

    public HttpRequest render(Map<String, Object> model) {
        HttpRequest copy = new HttpRequest();
        copy.host(host);
        copy.port(port);
        copy.method(method);
        if (path != null) {
            copy.path(path.render(model));
        }
        if (params != null) {
            MapBuilder<String, String> mapBuilder = MapBuilder.newMapBuilder();
            for (Map.Entry<String, Template> entry : params.entrySet()) {
                mapBuilder.put(entry.getKey(), entry.getValue().render(model));
            }
            copy.params(mapBuilder.map());
        }
        if (headers != null) {
            MapBuilder<String, String> mapBuilder = MapBuilder.newMapBuilder();
            for (Map.Entry<String, Template> entry : headers.entrySet()) {
                mapBuilder.put(entry.getKey(), entry.getValue().render(model));
            }
            copy.headers(mapBuilder.map());
        }
        copy.auth(auth);
        if (body != null) {
            copy.body(body.render(model));
        }
        return copy;
    }

    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject();
        builder.field(Parser.SCHEME_FIELD.getPreferredName(), scheme);
        builder.field(Parser.HOST_FIELD.getPreferredName(), host);
        builder.field(Parser.PORT_FIELD.getPreferredName(), port);
        builder.field(Parser.METHOD_FIELD.getPreferredName(), method);
        if (path != null) {
            builder.field(Parser.PATH_FIELD.getPreferredName(), path);
        }
        if (this.params != null) {
            builder.startObject(Parser.PARAMS_FIELD.getPreferredName()).value(this.params).endObject();
        }
        if (headers != null) {
            builder.startObject(Parser.HEADERS_FIELD.getPreferredName()).value(headers).endObject();
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

        TemplatedHttpRequest that = (TemplatedHttpRequest) o;

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

        private final Template.Parser templateParser;
        private final HttpAuthRegistry httpAuthRegistry;

        @Inject
        public Parser(Template.Parser templateParser, HttpAuthRegistry httpAuthRegistry) {
            this.templateParser = templateParser;
            this.httpAuthRegistry = httpAuthRegistry;
        }

        public TemplatedHttpRequest parse(XContentParser parser) throws IOException {
            TemplatedHttpRequest request = new TemplatedHttpRequest();
            XContentParser.Token token;
            String currentFieldName = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token == XContentParser.Token.START_OBJECT) {
                    if (PATH_FIELD.match(currentFieldName)) {
                        request.path(templateParser.parse(parser));
                    } else if (HEADERS_FIELD.match(currentFieldName)) {
                        request.headers(parseTemplates(parser));
                    } else if (PARAMS_FIELD.match(currentFieldName)) {
                        request.params(parseTemplates(parser));
                    }  else if (AUTH_FIELD.match(currentFieldName)) {
                        request.auth(httpAuthRegistry.parse(parser));
                    } else if (BODY_FIELD.match(currentFieldName)) {
                        request.body(templateParser.parse(parser));
                    } else {
                        throw new ElasticsearchParseException("could not parse templated http request. unexpected field [" + currentFieldName + "]");
                    }
                } else if (token == XContentParser.Token.VALUE_STRING) {
                    if (SCHEME_FIELD.match(currentFieldName)) {
                        request.scheme(Scheme.parse(parser.text()));
                    } else if (METHOD_FIELD.match(currentFieldName)) {
                        request.method(HttpMethod.parse(parser.text()));
                    } else if (HOST_FIELD.match(currentFieldName)) {
                        request.host(parser.text());
                    } else if (PATH_FIELD.match(currentFieldName)) {
                        request.path(templateParser.parse(parser));
                    } else if (BODY_FIELD.match(currentFieldName)) {
                        request.body(templateParser.parse(parser));
                    } else {
                        throw new ElasticsearchParseException("could not parse templated http request. unexpected field [" + currentFieldName + "]");
                    }
                } else if (token == XContentParser.Token.VALUE_NUMBER) {
                    if (PORT_FIELD.match(currentFieldName)) {
                        request.port(parser.intValue());
                    } else {
                        throw new ElasticsearchParseException("could not parse templated http request. unexpected field [" + currentFieldName + "]");
                    }
                } else {
                    throw new ElasticsearchParseException("could not parse templated http request. unexpected token [" + token + "] for field [" + currentFieldName + "]");
                }
            }
            return request;
        }

        private Map<String, Template> parseTemplates(XContentParser parser) throws IOException {
            Map<String, Template> templates = new HashMap<>();
            String currentFieldName = null;
            for (XContentParser.Token token = parser.nextToken(); token != XContentParser.Token.END_OBJECT; token = parser.nextToken()) {
                switch (token) {
                    case FIELD_NAME:
                        currentFieldName = parser.currentName();
                        break;
                    case VALUE_STRING:
                    case START_OBJECT:
                        templates.put(currentFieldName, templateParser.parse(parser));
                        break;
                    default:
                        throw new ElasticsearchParseException("could not parse templated http request. unexpected token [" + token + "]");
                }
            }
            return templates;
        }

    }

    public final static class SourceBuilder implements ToXContent {

        private String scheme;
        private String host;
        private int port;
        private String method;
        private Template path;
        private Map<String, Template> params;
        private Map<String, Template> headers;
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

        public SourceBuilder setParams(Map<String, Template> params) {
            this.params = params;
            return this;
        }

        public SourceBuilder setHeaders(Map<String, Template> headers) {
            this.headers = headers;
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
            builder.field(Parser.HOST_FIELD.getPreferredName(), host);
            builder.field(Parser.PORT_FIELD.getPreferredName(), port);
            if (method != null) {
                builder.field(Parser.METHOD_FIELD.getPreferredName(), method);
            }
            if (path != null) {
                builder.field(Parser.PATH_FIELD.getPreferredName(), path);
            }
            if (params != null) {
                builder.field(Parser.PARAMS_FIELD.getPreferredName(), params);
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
            builder.endObject();
            return builder;
        }
    }

}
