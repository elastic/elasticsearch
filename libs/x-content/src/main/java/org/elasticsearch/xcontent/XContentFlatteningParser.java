/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.xcontent;

import java.io.IOException;

public class XContentFlatteningParser extends XContentSubParser {

    private int level;
    private boolean closed;

    private final String parentName;

    private static final char DELIMITER = '.';

    public XContentFlatteningParser(XContentParser parser, String parentName) {
        super(parser);
        final Token token = parser.currentToken();
        if (token != Token.START_OBJECT && token != Token.START_ARRAY) {
            throw new IllegalStateException("The sub parser has to be created on the start of an object or array");
        }
        level = 1;
        this.parentName = parentName;
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
    public String currentName() throws IOException {
        if (level == 1) {
            return String.format("%s%s%s", parentName, DELIMITER, delegate().currentName());
        }
        return super.currentName();// TODO-MP what to do here?
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
