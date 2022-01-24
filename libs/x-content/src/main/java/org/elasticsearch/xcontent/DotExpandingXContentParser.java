/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.xcontent;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;

/**
 * An XContentParser that reinterprets field names containing dots as an object structure.
 *
 * A fieldname named {@code "foo.bar.baz":...} will be parsed instead as {@code 'foo':{'bar':{'baz':...}}}
 */
public class DotExpandingXContentParser extends FilterXContentParser {

    private static class WrappingParser extends DelegatingXContentParser {

        final Deque<XContentParser> parsers = new ArrayDeque<>();

        WrappingParser(XContentParser in) throws IOException {
            parsers.push(in);
            if (in.currentToken() == Token.FIELD_NAME) {
                expandDots();
            }
        }

        @Override
        public Token nextToken() throws IOException {
            Token token;
            while ((token = delegate().nextToken()) == null) {
                parsers.pop();
                if (parsers.isEmpty()) {
                    return null;
                }
            }
            if (token != Token.FIELD_NAME) {
                return token;
            }
            expandDots();
            return Token.FIELD_NAME;
        }

        private void expandDots() throws IOException {
            String field = delegate().currentName();
            String[] subpaths = splitAndValidatePath(field);
            if (subpaths.length == 0) {
                throw new IllegalArgumentException("field name cannot contain only dots: [" + field + "]");
            }
            if (subpaths.length == 1) {
                return;
            }
            Token token = delegate().nextToken();
            if (token == Token.START_OBJECT || token == Token.START_ARRAY) {
                parsers.push(new DotExpandingXContentParser(new XContentSubParser(delegate()), delegate(), subpaths));
            } else if (token == Token.END_OBJECT || token == Token.END_ARRAY) {
                throw new IllegalStateException("Expecting START_OBJECT or START_ARRAY or VALUE but got [" + token + "]");
            } else {
                parsers.push(new DotExpandingXContentParser(new SingletonValueXContentParser(delegate()), delegate(), subpaths));
            }
        }

        @Override
        protected XContentParser delegate() {
            return parsers.peek();
        }
    }

    private static String[] splitAndValidatePath(String fullFieldPath) {
        if (fullFieldPath.isEmpty()) {
            throw new IllegalArgumentException("field name cannot be an empty string");
        }
        if (fullFieldPath.contains(".") == false) {
            return new String[] { fullFieldPath };
        }
        String[] parts = fullFieldPath.split("\\.");
        if (parts.length == 0) {
            throw new IllegalArgumentException("field name cannot contain only dots");
        }
        for (String part : parts) {
            // check if the field name contains only whitespace
            if (part.isEmpty()) {
                throw new IllegalArgumentException("object field cannot contain only whitespace: ['" + fullFieldPath + "']");
            }
            if (part.isBlank()) {
                throw new IllegalArgumentException(
                    "object field starting or ending with a [.] makes object resolution ambiguous: [" + fullFieldPath + "]"
                );
            }
        }
        return parts;
    }

    /**
     * Wraps an XContentParser such that it re-interprets dots in field names as an object structure
     * @param in    the parser to wrap
     * @return  the wrapped XContentParser
     */
    public static XContentParser expandDots(XContentParser in) throws IOException {
        return new WrappingParser(in);
    }

    private enum State {
        PRE,
        DURING,
        POST
    }

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
        if (level >= 1) {
            level -= 2;
        }
        return level < 0 ? null : Token.END_OBJECT;
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
