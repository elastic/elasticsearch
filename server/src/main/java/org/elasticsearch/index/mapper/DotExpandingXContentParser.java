/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.List;
import java.util.Map;
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

        private final ContentPath contentPath;
        final Deque<XContentParser> parsers = new ArrayDeque<>();
        final DocumentParserContext context;
        boolean supportsObjectAutoFlattening;

        WrappingParser(XContentParser in, ContentPath contentPath, DocumentParserContext context) throws IOException {
            this.contentPath = contentPath;
            this.context = context;
            this.supportsObjectAutoFlattening = (context != null && context.supportsObjectAutoFlattening());
            parsers.push(in);
            if (in.currentToken() == Token.FIELD_NAME) {
                expandDots(in);
            }
        }

        @Override
        public Token nextToken() throws IOException {
            Token token;
            XContentParser delegate;
            // cache object field (even when final this is a valid optimization, see https://openjdk.org/jeps/8132243)
            var parsers = this.parsers;
            while ((token = (delegate = parsers.peek()).nextToken()) == null) {
                parsers.pop();
                if (parsers.isEmpty()) {
                    return null;
                }
            }
            if (token != Token.FIELD_NAME) {
                return token;
            }
            expandDots(delegate);
            return Token.FIELD_NAME;
        }

        private void expandDots(XContentParser delegate) throws IOException {
            // this handles fields that belong to objects that can't hold subobjects, where the document specifies
            // the object holding the flat fields
            // e.g. { "metrics.service": { "time.max" : 10 } } with service having subobjects set to false
            if (contentPath.isWithinLeafObject()) {
                return;
            }
            String field = delegate.currentName();
            int length = field.length();
            if (length == 0) {
                throw new IllegalArgumentException("field name cannot be an empty string");
            }
            final int dotCount = FieldTypeLookup.dotCount(field);
            if (dotCount == 0) {
                return;
            }
            doExpandDots(delegate, field, dotCount);
        }

        private void doExpandDots(XContentParser delegate, String field, int dotCount) throws IOException {
            int next;
            int offset = 0;
            String[] list = new String[dotCount + 1];
            int listIndex = 0;
            for (int i = 0; i < dotCount; i++) {
                next = field.indexOf('.', offset);
                list[listIndex++] = field.substring(offset, next);
                offset = next + 1;
            }

            // Add remaining segment
            list[listIndex] = field.substring(offset);

            int resultSize = list.length;
            // Construct result
            while (resultSize > 0 && list[resultSize - 1].isEmpty()) {
                resultSize--;
            }
            if (resultSize == 0) {
                throw new IllegalArgumentException("field name cannot contain only dots");
            }
            String[] subpaths;
            if (resultSize == list.length) {
                for (String part : list) {
                    // check if the field name contains only whitespace
                    if (part.isBlank()) {
                        throwOnBlankOrEmptyPart(field, part);
                    }
                }
                subpaths = list;
            } else {
                // Corner case: if the input has a single trailing '.', eg 'field.', then we will get a single
                // subpath due to the way String.split() works. We can only return fast here if this is not
                // the case
                // TODO make this case throw an error instead? https://github.com/elastic/elasticsearch/issues/28948
                if (resultSize == 1 && field.endsWith(".") == false) {
                    return;
                }
                subpaths = extractAndValidateResults(field, list, resultSize);
            }
            if (supportsObjectAutoFlattening && subpaths.length > 1) {
                subpaths = maybeFlattenPaths(Arrays.asList(subpaths), context, contentPath).toArray(String[]::new);
            }
            pushSubParser(delegate, subpaths);
        }

        private void pushSubParser(XContentParser delegate, String[] subpaths) throws IOException {
            XContentLocation location = delegate.getTokenLocation();
            Token token = delegate.nextToken();
            final XContentParser subParser;
            if (token == Token.START_OBJECT || token == Token.START_ARRAY) {
                subParser = new XContentSubParser(delegate);
            } else {
                if (token == Token.END_OBJECT || token == Token.END_ARRAY) {
                    throwExpectedOpen(token);
                }
                subParser = new SingletonValueXContentParser(delegate);
            }
            parsers.push(new DotExpandingXContentParser(subParser, subpaths, location, contentPath));
        }

        private static void throwExpectedOpen(Token token) {
            throw new IllegalStateException("Expecting START_OBJECT or START_ARRAY or VALUE but got [" + token + "]");
        }

        private static String[] extractAndValidateResults(String field, String[] list, int resultSize) {
            final String[] subpaths = new String[resultSize];
            for (int i = 0; i < resultSize; i++) {
                String part = list[i];
                // check if the field name contains only whitespace
                if (part.isBlank()) {
                    throwOnBlankOrEmptyPart(field, part);
                }
                subpaths[i] = part;
            }
            return subpaths;
        }

        private static void throwOnBlankOrEmptyPart(String field, String part) {
            if (part.isEmpty()) {
                throw new IllegalArgumentException("field name cannot contain only whitespace: ['" + field + "']");
            }
            throw new IllegalArgumentException(
                "field name starting or ending with a [.] makes object resolution ambiguous: [" + field + "]"
            );
        }

        @Override
        protected XContentParser delegate() {
            return parsers.peek();
        }

        /*
        The following methods (map* and list*) are known not be called by DocumentParser when parsing documents, but we support indexing
        percolator queries which are also parsed through DocumentParser, and their parsing code is completely up to each query, which are
        also pluggable. That means that this parser needs to fully support parsing arbitrary content, when dots expansion is turned off.
        We do throw UnsupportedOperationException when dots expansion is enabled as we don't expect such methods to be ever called in
        those circumstances.
         */

        @Override
        public Map<String, Object> map() throws IOException {
            if (contentPath.isWithinLeafObject()) {
                return super.map();
            }
            throw new UnsupportedOperationException();
        }

        @Override
        public Map<String, Object> mapOrdered() throws IOException {
            if (contentPath.isWithinLeafObject()) {
                return super.mapOrdered();
            }
            throw new UnsupportedOperationException();
        }

        @Override
        public Map<String, String> mapStrings() throws IOException {
            if (contentPath.isWithinLeafObject()) {
                return super.mapStrings();
            }
            throw new UnsupportedOperationException();
        }

        @Override
        public <T> Map<String, T> map(Supplier<Map<String, T>> mapFactory, CheckedFunction<XContentParser, T, IOException> mapValueParser)
            throws IOException {
            if (contentPath.isWithinLeafObject()) {
                return super.map(mapFactory, mapValueParser);
            }
            throw new UnsupportedOperationException();
        }

        @Override
        public List<Object> list() throws IOException {
            if (contentPath.isWithinLeafObject()) {
                return super.list();
            }
            throw new UnsupportedOperationException();
        }

        @Override
        public List<Object> listOrderedMap() throws IOException {
            if (contentPath.isWithinLeafObject()) {
                return super.listOrderedMap();
            }
            throw new UnsupportedOperationException();
        }
    }

    /**
     * Wraps an XContentParser such that it re-interprets dots in field names as an object structure
     * @param in            the parser to wrap
     * @param contentPath   the starting path to expand, can be empty
     * @param context       provides mapping context to check for objects supporting sub-object auto-flattening
     * @return              the wrapped XContentParser
     */
    static XContentParser expandDots(XContentParser in, ContentPath contentPath, DocumentParserContext context) throws IOException {
        return new WrappingParser(in, contentPath, context);
    }

    private enum State {
        EXPANDING_START_OBJECT,
        PARSING_ORIGINAL_CONTENT,
        ENDING_EXPANDED_OBJECT
    }

    private final ContentPath contentPath;

    private String[] subPaths;
    private XContentLocation currentLocation;
    private int expandedTokens = 0;
    private int innerLevel = -1;
    private State state = State.EXPANDING_START_OBJECT;

    private DotExpandingXContentParser(
        XContentParser subparser,
        String[] subPaths,
        XContentLocation startLocation,
        ContentPath contentPath
    ) {
        super(subparser);
        this.subPaths = subPaths;
        this.currentLocation = startLocation;
        this.contentPath = contentPath;
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
                int currentIndex = expandedTokens / 2;
                // if there's more than one element left to expand and the parent can't hold subobjects, we replace the array
                // e.g. metrics.service.time.max -> ["metrics", "service", "time.max"]
                if (currentIndex < subPaths.length - 1 && contentPath.isWithinLeafObject()) {
                    String[] newSubPaths = new String[currentIndex + 1];
                    StringBuilder collapsedPath = new StringBuilder();
                    for (int i = 0; i < subPaths.length; i++) {
                        if (i < currentIndex) {
                            newSubPaths[i] = subPaths[i];
                        } else {
                            collapsedPath.append(subPaths[i]);
                            if (i < subPaths.length - 1) {
                                collapsedPath.append(".");
                            }
                        }
                    }
                    newSubPaths[currentIndex] = collapsedPath.toString();
                    subPaths = newSubPaths;
                }
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

    static List<String> maybeFlattenPaths(List<String> subpaths, DocumentParserContext context, ContentPath contentPath) {
        String prefixWithDots = contentPath.pathAsText("");
        ObjectMapper parent = contentPath.length() == 0
            ? context.root()
            : context.findObject(prefixWithDots.substring(0, prefixWithDots.length() - 1));
        List<String> result = new ArrayList<>(subpaths.size());
        for (int i = 0; i < subpaths.size(); i++) {
            String fullPath = prefixWithDots + String.join(".", subpaths.subList(0, i));
            if (i > 0) {
                parent = context.findObject(fullPath);
            }
            boolean match = false;
            StringBuilder path = new StringBuilder(subpaths.get(i));
            if (parent == null) {
                // We get here for dynamic objects, which always get parsed with subobjects and may get flattened later.
                match = true;
            } else if (parent.subobjects() == ObjectMapper.Subobjects.ENABLED) {
                match = true;
            } else if (parent.subobjects() == ObjectMapper.Subobjects.AUTO) {
                // Check if there's any subobject in the remaining path.
                for (int j = i; j < subpaths.size() - 1; j++) {
                    if (j > i) {
                        path.append(".").append(subpaths.get(j));
                    }
                    Mapper mapper = parent.mappers.get(path.toString());
                    if (mapper instanceof ObjectMapper objectMapper
                        && (ObjectMapper.isFlatteningCandidate(objectMapper.subobjects, objectMapper)
                            || objectMapper.checkFlattenable(null).isPresent())) {
                        i = j;
                        match = true;
                        break;
                    }
                }
            }
            if (match) {
                result.add(path.toString());
            } else {
                // We only get here if parent has subobjects set to false, or set to auto with no non-flattenable object in the sub-path.
                result.add(String.join(".", subpaths.subList(i, subpaths.size())));
                return result;
            }
        }
        return result;
    }
}
