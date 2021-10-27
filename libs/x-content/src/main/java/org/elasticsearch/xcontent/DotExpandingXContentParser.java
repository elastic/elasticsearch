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
 * An XContentParser that reinterprets a field name containing dots as an object
 * structure.
 *
 * A fieldname named 'foo.bar.baz':... will be parsed instead as 'foo':{'bar':{'baz':...}}
 */
public class DotExpandingXContentParser extends FilterXContentParser {

    /**
     * Wraps an XContentParser positioned on a field name such that it re-interprets
     * any dots in the field name as an object structure
     * @param in    the parser to wrap
     * @return  the wrapped XContentParser, which may be the initial parser if there are no dots
     *          in the current field name
     */
    public static XContentParser expandDots(XContentParser in) throws IOException {
        if (in.currentToken() != Token.FIELD_NAME) {
            throw new IllegalStateException("The sub parser must be positioned on a field name");
        }
        String field = in.currentName();
        String[] subpaths = field.split("\\.");
        if (subpaths.length == 1) {
            return in;
        }
        Token token = in.nextToken();
        // We wrap the incoming parser so that it returns null when we have finished iterating
        // through the current object or array, and we can emit closing END_OBJECTs
        if (token == Token.START_OBJECT || token == Token.START_ARRAY) {
            return new DotExpandingXContentParser(new XContentSubParser(in), in, subpaths);
        }
        if (token == Token.END_OBJECT || token == Token.END_ARRAY) {
            throw new IllegalStateException("Expecting START_OBJECT or START_ARRAY or VALUE but got [" + token + "]");
        }
        return new DotExpandingXContentParser(new SingletonValueXContentParser(in), in, subpaths);
    }

    private enum State { PRE, DURING, POST }

    final String[] subPaths;
    final XContentParser subparser;

    int level = 0;
    private State state = State.PRE;

    private DotExpandingXContentParser(XContentParser subparser, XContentParser root, String[] subPaths) {
        super(root);
        this.subPaths = subPaths;
        this.subparser = subparser;
    }

    @Override
    public Token nextToken() throws IOException {
        if (state == State.PRE) {
            level++;
            if (level == subPaths.length * 2 - 1) {
                state = State.DURING;
                return in.currentToken();
            }
            if (level % 2 == 0) {
                return Token.FIELD_NAME;
            }
            return Token.START_OBJECT;
        }
        if (state == State.DURING) {
            Token token = subparser.nextToken();
            if (token != null) {
                return token;
            }
            state = State.POST;
        }
        assert state == State.POST;
        if (level > 1) {
            level -= 2;
            return Token.END_OBJECT;
        }
        return in.nextToken();
    }

    @Override
    public Token currentToken() {
        if (state == State.PRE) {
            return level % 2 == 1 ? Token.START_OBJECT : Token.FIELD_NAME;
        }
        if (state == State.POST) {
            if (level > 1) {
                return Token.END_OBJECT;
            }
        }
        return in.currentToken();
    }

    @Override
    public String currentName() throws IOException {
        if (state == State.DURING) {
            return in.currentName();
        }
        if (state == State.POST) {
            if (level <= 1) {
                return in.currentName();
            }
            throw new IllegalStateException("Can't get current name during END_OBJECT");
        }
        return subPaths[level / 2];
    }

    @Override
    public void skipChildren() throws IOException {
        if (state == State.PRE) {
            in.skipChildren();
            level++;
            state = State.POST;
        }
        if (state == State.DURING) {
            subparser.skipChildren();
        }
    }

    @Override
    public String textOrNull() throws IOException {
        if (state == State.PRE) {
            throw new IllegalStateException("Can't get text on a " + currentToken() + " at " + getTokenLocation());
        }
        return super.textOrNull();
    }

    @Override
    public Number numberValue() throws IOException {
        if (state == State.PRE) {
            throw new IllegalStateException("Can't get numeric value on a " + currentToken() + " at " + getTokenLocation());
        }
        return super.numberValue();
    }

    @Override
    public boolean booleanValue() throws IOException {
        if (state == State.PRE) {
            throw new IllegalStateException("Can't get boolean value on a " + currentToken() + " at " + getTokenLocation());
        }
        return super.booleanValue();
    }

    private static class SingletonValueXContentParser extends FilterXContentParser {

        protected SingletonValueXContentParser(XContentParser in) {
            super(in);
        }

        @Override
        public Token nextToken() throws IOException {
            return null;
        }
    }
}
