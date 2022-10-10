/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.xcontent;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * Copies its values into a {@code byte[]} while reading them. Use
 * with {@link XContentSubParser} to read the entire object.
 */
public class CopyingXContentParser extends FilterXContentParserWrapper {
    private final ByteArrayOutputStream out;
    private final XContentGenerator generator;

    public CopyingXContentParser(XContentParser delegate) throws IOException {
        super(delegate);
        out = new ByteArrayOutputStream();
        generator = delegate.contentType().xContent().createGenerator(out);
        switch (delegate.currentToken()) {
            case START_OBJECT -> generator.writeStartObject();
            case START_ARRAY -> generator.writeStartArray();
            default -> throw new IllegalArgumentException(
                "can only copy parsers pointed to START_OBJECT or START_ARRAY but found: " + delegate.currentToken()
            );
        }
    }

    @Override
    public Token nextToken() throws IOException {
        Token next = delegate().nextToken();
        generator.copyCurrentEvent(delegate());
        return next;
    }

    public byte[] bytes() throws IOException {
        generator.close();
        return out.toByteArray();
    }
}
