/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.support.http.auth;

import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Map;

/**
 *
 */
public class HttpAuthRegistry {

    private final ImmutableMap<String, HttpAuth.Parser> parsers;

    @Inject
    public HttpAuthRegistry(Map<String, HttpAuth.Parser> parsers) {
        this.parsers = ImmutableMap.copyOf(parsers);
    }

    public HttpAuth parse(XContentParser parser) throws IOException {
        String type = null;
        XContentParser.Token token;
        HttpAuth auth = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                type = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT && type != null) {
                HttpAuth.Parser inputParser = parsers.get(type);
                if (inputParser == null) {
                    throw new HttpAuthException("unknown http auth type [" + type + "]");
                }
                auth = inputParser.parse(parser);
            }
        }
        return auth;
    }

}
