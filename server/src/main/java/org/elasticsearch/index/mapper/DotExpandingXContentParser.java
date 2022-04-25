/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.xcontent.FilterXContentParser;
import org.elasticsearch.xcontent.FilterXContentParserWrapper;
import org.elasticsearch.xcontent.XContentLocation;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentSubParser;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * An XContentParser that reinterprets field names containing dots as an object structure.
 *
 * A field name named {@code "foo.bar.baz":...} will be parsed instead as {@code 'foo':{'bar':{'baz':...}}}.
 * The token location is preserved so that error messages refer to the original content being parsed.
 * This parser can output duplicate keys, but that is fine given that it's used for document parsing. The mapping
 * lookups will return the same mapper/field type, and we never load incoming documents in a map where duplicate
 * keys would end up overriding each other.
 */
class DotExpandingXContentParser extends FilterXContentParserWrapper {

    private static final class WrappingParser extends FilterXContentParser {

        private final Function<String, String[]> pathSplitter;
        final Deque<XContentParser> parsers = new ArrayDeque<>();

        WrappingParser(XContentParser in, Function<String, String[]> pathSplitter) throws IOException {
            this.pathSplitter = pathSplitter;
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
            String[] subpaths = pathSplitter.apply(field);
            if (subpaths.length == 0) {
                throw new IllegalArgumentException("field name cannot contain only dots: [" + field + "]");
            }
            // Corner case: if the input has a single trailing '.', eg 'field.', then we will get a single
            // subpath due to the way String.split() works. We can only return fast here if this is not
            // the case
            // TODO make this case throw an error instead? https://github.com/elastic/elasticsearch/issues/28948
            if (subpaths.length == 1 && field.endsWith(".") == false) {
                return;
            }
            XContentLocation location = delegate().getTokenLocation();
            Token token = delegate().nextToken();
            if (token == Token.START_OBJECT || token == Token.START_ARRAY) {
                parsers.push(new DotExpandingXContentParser(new XContentSubParser(delegate()), subpaths, location));
            } else if (token == Token.END_OBJECT || token == Token.END_ARRAY) {
                throw new IllegalStateException("Expecting START_OBJECT or START_ARRAY or VALUE but got [" + token + "]");
            } else {
                parsers.push(new DotExpandingXContentParser(new SingletonValueXContentParser(delegate()), subpaths, location));
            }
        }

        @Override
        protected XContentParser delegate() {
            return parsers.peek();
        }

        @Override
        public Map<String, Object> map() throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public Map<String, Object> mapOrdered() throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public Map<String, String> mapStrings() throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T> Map<String, T> map(Supplier<Map<String, T>> mapFactory, CheckedFunction<XContentParser, T, IOException> mapValueParser)
            throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<Object> list() throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<Object> listOrderedMap() throws IOException {
            throw new UnsupportedOperationException();
        }
    }

    //TODO we may want to move this back to DocumentParser given that it now depends on DocumentParserContext
    static String[] splitAndValidatePath(String fieldName, DocumentParserContext context) {
        if (fieldName.isEmpty()) {
            throw new IllegalArgumentException("field name cannot be an empty string");
        }
        if (fieldName.contains(".") == false) {
            return new String[] { fieldName };
        }
        String[] parts = fieldName.split("\\.");
        if (parts.length == 0) {
            throw new IllegalArgumentException("field name cannot contain only dots");
        }

        //TODO do we want to optimize for the case where there are no collapsed paths in the mapping?

        //check if any of the parent objects has collapsed set to true
        if (context.isWithinCollapsedPath()) {
            return new String[] { fieldName };
        }

        ContentPath currentPath = new ContentPath();
        int indexCollapsed = -1;
        for (int i = 0; i < parts.length; i++) {
            String part = parts[i];
            // check if the field name contains only whitespace
            if (part.isEmpty()) {
                throw new IllegalArgumentException("field name cannot contain only whitespace: ['" + fieldName + "']");
            }
            if (part.isBlank()) {
                throw new IllegalArgumentException(
                    "field name starting or ending with a [.] makes object resolution ambiguous: [" + fieldName + "]"
                );
            }

            //e.g. metrics.service.time.max: if 'time' is collapsed, we still need to expand dots fully. 'service' is the last
            //path part that affects dots expansion for this path if it is collapsed.
            if (indexCollapsed < 0 && i < parts.length - 2) {
                //concatenate the full path of the current element that we are parsing with the previously examined parts of the
                //dotted name that we are splitting, to check if there's a collapsed object in between
                String fullPath = context.path().pathAsText(currentPath.pathAsText(part));
                ObjectMapper objectMapper = context.mappingLookup().objectMappers().get(fullPath);
                if (objectMapper != null && objectMapper.isCollapsed()) {
                    indexCollapsed = i;
                }
                currentPath.add(part);
            }
        }
        if (indexCollapsed >= 0) {
            ContentPath collapsedPath = new ContentPath();
            String[] finalParts = new String[indexCollapsed + 2];
            for (int i = 0; i < parts.length - 1; i++) {
                String part = parts[i];
                if (i <= indexCollapsed) {
                    finalParts[i] = part;
                } else {
                    collapsedPath.add(part);
                }
            }
            finalParts[indexCollapsed + 1] = collapsedPath.pathAsText(parts[parts.length - 1]);
            return finalParts;
        }
        return parts;
    }

    /**
     * Wraps an XContentParser such that it re-interprets dots in field names as an object structure
     * @param in    the parser to wrap
     * @return  the wrapped XContentParser
     */
    static XContentParser expandDots(XContentParser in, DocumentParserContext context) throws IOException {
        return expandDots(in, field -> splitAndValidatePath(field, context));
    }

    static XContentParser expandDots(XContentParser in, Function<String, String[]> pathSplitter) throws IOException {
        return new WrappingParser(in, pathSplitter);
    }

    private enum State {
        EXPANDING_START_OBJECT,
        PARSING_ORIGINAL_CONTENT,
        ENDING_EXPANDED_OBJECT
    }

    final String[] subPaths;

    private XContentLocation currentLocation;
    private int expandedTokens = 0;
    private int innerLevel = -1;
    private State state = State.EXPANDING_START_OBJECT;

    private DotExpandingXContentParser(XContentParser subparser, String[] subPaths, XContentLocation startLocation) {
        super(subparser);
        this.subPaths = subPaths;
        this.currentLocation = startLocation;
    }

    @Override
    public Token nextToken() throws IOException {
        if (state == State.EXPANDING_START_OBJECT) {
            expandedTokens++;
            assert expandedTokens < subPaths.length * 2;
            if (expandedTokens == subPaths.length * 2 - 1) {
                state = State.PARSING_ORIGINAL_CONTENT;
                Token token = delegate().currentToken();
                if (token == Token.START_OBJECT || token == Token.START_ARRAY) {
                    innerLevel++;
                }
                return token;
            }
            // The expansion consists of adding pairs of START_OBJECT and FIELD_NAME tokens
            if (expandedTokens % 2 == 0) {
                return Token.FIELD_NAME;
            }
            return Token.START_OBJECT;
        }
        if (state == State.PARSING_ORIGINAL_CONTENT) {
            Token token = delegate().nextToken();
            if (token == Token.START_OBJECT || token == Token.START_ARRAY) {
                innerLevel++;
            }
            if (token == Token.END_OBJECT || token == Token.END_ARRAY) {
                innerLevel--;
            }
            if (token != null) {
                return token;
            }
            currentLocation = getTokenLocation();
            state = State.ENDING_EXPANDED_OBJECT;
        }
        assert expandedTokens % 2 == 1;
        expandedTokens -= 2;
        return expandedTokens < 0 ? null : Token.END_OBJECT;
    }

    @Override
    public XContentLocation getTokenLocation() {
        if (state == State.PARSING_ORIGINAL_CONTENT) {
            return super.getTokenLocation();
        }
        return currentLocation;
    }

    @Override
    public Token currentToken() {
        return switch (state) {
            case EXPANDING_START_OBJECT -> expandedTokens % 2 == 1 ? Token.START_OBJECT : Token.FIELD_NAME;
            case ENDING_EXPANDED_OBJECT -> Token.END_OBJECT;
            case PARSING_ORIGINAL_CONTENT -> delegate().currentToken();
        };
    }

    @Override
    public String currentName() throws IOException {
        if (state == State.PARSING_ORIGINAL_CONTENT) {
            assert expandedTokens == subPaths.length * 2 - 1;
            // whenever we are parsing some inner object/array we can easily delegate to the inner parser
            // e.g. field.with.dots: { obj:{ parsing here } }
            if (innerLevel > 0) {
                return delegate().currentName();
            }
            Token token = currentToken();
            // if we are parsing the outer object/array, only at the start object/array we need to return
            // e.g. dots instead of field.with.dots otherwise we can simply delegate to the inner parser
            // which will do the right thing
            if (innerLevel == 0 && token != Token.START_OBJECT && token != Token.START_ARRAY) {
                return delegate().currentName();
            }
            // note that innerLevel can be -1 if there are no inner object/array e.g. field.with.dots: value
            // as well as while there is and we are parsing their END_OBJECT or END_ARRAY
        }
        return subPaths[expandedTokens / 2];
    }

    @Override
    public void skipChildren() throws IOException {
        if (state == State.EXPANDING_START_OBJECT) {
            delegate().skipChildren();
            state = State.ENDING_EXPANDED_OBJECT;
        }
        if (state == State.PARSING_ORIGINAL_CONTENT) {
            delegate().skipChildren();
        }
    }

    @Override
    public String textOrNull() throws IOException {
        if (state == State.EXPANDING_START_OBJECT) {
            throw new IllegalStateException("Can't get text on a " + currentToken() + " at " + getTokenLocation());
        }
        return super.textOrNull();
    }

    @Override
    public Number numberValue() throws IOException {
        if (state == State.EXPANDING_START_OBJECT) {
            throw new IllegalStateException("Can't get numeric value on a " + currentToken() + " at " + getTokenLocation());
        }
        return super.numberValue();
    }

    @Override
    public boolean booleanValue() throws IOException {
        if (state == State.EXPANDING_START_OBJECT) {
            throw new IllegalStateException("Can't get boolean value on a " + currentToken() + " at " + getTokenLocation());
        }
        return super.booleanValue();
    }

    private static class SingletonValueXContentParser extends FilterXContentParserWrapper {

        protected SingletonValueXContentParser(XContentParser in) {
            super(in);
        }

        @Override
        public Token nextToken() throws IOException {
            return null;
        }
    }
}
