/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.xcontent;

import java.io.IOException;

/**
 * A parser that copies data that was parsed into a {@link XContentBuilder}.
 * This parser naturally has some memory and runtime overhead to perform said copying.
 * Use with {@link XContentSubParser} to preserve the entire object.
 */
public class CopyingXContentParser extends FilterXContentParserWrapper {
    private final XContentBuilder builder;

    public CopyingXContentParser(XContentParser delegate) throws IOException {
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
