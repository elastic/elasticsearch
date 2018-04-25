/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.common.http.auth;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.xpack.core.watcher.support.Exceptions.illegalArgument;

public class HttpAuthRegistry {

    private final Map<String, HttpAuthFactory> factories;

    public HttpAuthRegistry(Map<String, HttpAuthFactory> factories) {
        this.factories = factories;
    }

    public HttpAuth parse(XContentParser parser) throws IOException {
        String type = null;
        XContentParser.Token token;
        HttpAuth auth = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                type = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT && type != null) {
                HttpAuthFactory factory = factories.get(type);
                if (factory == null) {
                    throw new ElasticsearchParseException("unknown http auth type [{}]", type);
                }
                auth = factory.parse(parser);
            }
        }
        return auth;
    }

    public <A extends HttpAuth, AA extends ApplicableHttpAuth<A>> AA createApplicable(A auth) {
        HttpAuthFactory factory = factories.get(auth.type());
        if (factory == null) {
            throw illegalArgument("unknown http auth type [{}]", auth.type());
        }
        return (AA) factory.createApplicable(auth);
    }

}
