/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.xcontent;

import java.io.IOException;

/**
 * A parser that parses a document preserving fields that were parsed so far in a {@link XContentBuilder}.
 * Use with {@link XContentSubParser} to preserve the entire object.
 */
public class MemorizingXContentParser extends FilterXContentParserWrapper {
    private final XContentBuilder builder;

    public MemorizingXContentParser(XContentParser delegate) throws IOException {
        super(delegate);
        this.builder = XContentBuilder.builder(delegate.contentType().xContent());
        switch (delegate.currentToken()) {
            case START_OBJECT -> builder.startObject();
            case START_ARRAY -> builder.startArray();
            default -> throw new IllegalArgumentException(
                "can only copy parsers pointed to START_OBJECT or START_ARRAY but found: " + delegate.currentToken()
            );
        }
    }

    @Override
    public Token nextToken() throws IOException {
        XContentParser.Token next = delegate().nextToken();
        builder.copyCurrentEvent(delegate());
        return next;
    }

    public XContentBuilder getBuilder() {
        return builder;
    }
}
