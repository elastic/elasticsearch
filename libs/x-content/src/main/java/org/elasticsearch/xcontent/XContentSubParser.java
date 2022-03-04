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
 * Wrapper for a XContentParser that makes a single object/array look like a complete document.
 *
 * The wrapper prevents the parsing logic to consume tokens outside of the wrapped object as well
 * as skipping to the end of the object in case of a parsing error. The wrapper is intended to be
 * used for parsing objects that should be ignored if they are malformed.
 */
public class XContentSubParser extends FilterXContentParserWrapper {

    private int level;
    private boolean closed;

    public XContentSubParser(XContentParser parser) {
        super(parser);
        if (parser.currentToken() != Token.START_OBJECT && parser.currentToken() != Token.START_ARRAY) {
            throw new IllegalStateException("The sub parser has to be created on the start of an object or array");
        }
        level = 1;
    }

    @Override
    public Token nextToken() throws IOException {
        if (level > 0) {
            Token token = super.nextToken();
            if (token == Token.START_OBJECT || token == Token.START_ARRAY) {
                level++;
            } else if (token == Token.END_OBJECT || token == Token.END_ARRAY) {
                level--;
            }
            return token;
        } else {
            return null; // we have reached the end of the wrapped object
        }
    }

    @Override
    public void skipChildren() throws IOException {
        Token token = currentToken();
        if (token != Token.START_OBJECT && token != Token.START_ARRAY) {
            // skip if not starting on an object or an array
            return;
        }
        int backToLevel = level - 1;
        while (nextToken() != null) {
            if (level <= backToLevel) {
                return;
            }
        }
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    public void close() throws IOException {
        if (closed == false) {
            closed = true;
            while (true) {
                if (nextToken() == null) {
                    return;
                }
            }
        }
    }
}
