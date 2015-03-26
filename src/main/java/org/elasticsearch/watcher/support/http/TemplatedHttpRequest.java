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

    private String host;
    private int port;
    private HttpMethod method;
    private Template path;
    private Map<String, Template> params;
    private Map<String, Template> headers;
    private HttpAuth auth;
    private Template body;

    public TemplatedHttpRequest() {
        method = HttpMethod.GET;
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

    public static class Parser {

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
                    if (METHOD_FIELD.match(currentFieldName)) {
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
                    throw new ElasticsearchParseException("could not parse templated http request. unexpected token [" + token + "]");
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

}
