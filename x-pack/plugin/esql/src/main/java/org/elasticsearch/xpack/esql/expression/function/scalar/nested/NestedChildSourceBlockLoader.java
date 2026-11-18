/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.expression.function.scalar.nested;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOFunction;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.BlockSourceReader;
import org.elasticsearch.index.mapper.FallbackSyntheticSourceBlockLoader;
import org.elasticsearch.search.fetch.StoredFieldsSpec;
import org.elasticsearch.search.lookup.Source;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Reads a field under a nested object that keeps its values only in synthetic source from the child's object in the parent's
 * {@code _source}. The field's own loader reads the child's {@code _ignored_source}, but where the nested object or an object
 * above it keeps its whole source, as {@code synthetic_source_keep} can, those values are in the parent's instead. The
 * parent's synthetic {@code _source} holds them wherever they are kept, and they are converted as the field's own loader does.
 */
final class NestedChildSourceBlockLoader implements BlockLoader {
    private final FallbackSyntheticSourceBlockLoader fallback;
    private final Set<String> fields;
    private final boolean textScalars;

    /**
     * @param fields the paths in the child's object that hold the field's values: the field's own, and those of the fields that
     *               {@code copy_to} it
     * @param textScalars whether a number or a boolean is read as the text it is written as, as the field's loader reads it
     */
    NestedChildSourceBlockLoader(FallbackSyntheticSourceBlockLoader fallback, Set<String> fields, boolean textScalars) {
        this.fallback = fallback;
        this.fields = Set.copyOf(fields);
        this.textScalars = textScalars;
    }

    @Override
    public Builder builder(BlockFactory factory, int expectedCount) {
        return fallback.builder(factory, expectedCount);
    }

    @Override
    public IOFunction<CircuitBreaker, ColumnAtATimeReader> columnAtATimeReader(LeafReaderContext context) {
        return null;
    }

    @Override
    public RowStrideReader rowStrideReader(CircuitBreaker breaker, LeafReaderContext context) {
        breaker.addEstimateBytesAndMaybeBreak(BlockSourceReader.ESTIMATED_SIZE, "load blocks");
        return new Reader<>(breaker, fallback.reader());
    }

    @Override
    public StoredFieldsSpec rowStrideStoredFieldSpec() {
        return StoredFieldsSpec.NEEDS_SOURCE;
    }

    @Override
    public boolean supportsOrdinals() {
        return false;
    }

    @Override
    public SortedSetDocValues ordinals(LeafReaderContext context) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String toString() {
        return "NestedChildSource[" + fallback + "]";
    }

    private class Reader<T> implements RowStrideReader {
        private final CircuitBreaker breaker;
        private final FallbackSyntheticSourceBlockLoader.Reader<T> reader;

        Reader(CircuitBreaker breaker, FallbackSyntheticSourceBlockLoader.Reader<T> reader) {
            this.breaker = breaker;
            this.reader = reader;
        }

        @Override
        public void read(int docId, StoredFields storedFields, Builder builder) throws IOException {
            List<T> values = new ArrayList<>();
            // The child's source holds its object at the nested object's path, as the parent's holds all of them
            Source source = storedFields.source();
            BytesReference bytes = source.internalSourceRef();
            if (bytes != null) {
                try (
                    XContentParser parser = source.sourceContentType()
                        .xContent()
                        .createParser(XContentParserConfiguration.EMPTY, bytes.streamInput())
                ) {
                    if (parser.nextToken() != null) {
                        parseAt(parser, null, values);
                    }
                }
            }
            if (values.isEmpty()) {
                builder.appendNull();
                return;
            }
            if (values.size() > 1) {
                builder.beginPositionEntry();
            }
            reader.writeToBlock(values, builder);
            if (values.size() > 1) {
                builder.endPositionEntry();
            }
        }

        private boolean isAncestorOfAField(String path) {
            for (String field : fields) {
                if (field.length() > path.length() && field.startsWith(path) && field.charAt(path.length()) == '.') {
                    return true;
                }
            }
            return false;
        }

        /**
         * Adds the values of {@link #fields} under the value the parser is on, which is at {@code path}, or the root if it is
         * {@code null}, as the field's own loader does.
         */
        private void parseAt(XContentParser parser, String path, List<T> values) throws IOException {
            XContentParser.Token token = parser.currentToken();
            if (token == XContentParser.Token.START_OBJECT) {
                while (parser.nextToken() == XContentParser.Token.FIELD_NAME) {
                    String childPath = path == null ? parser.currentName() : path + "." + parser.currentName();
                    parser.nextToken();
                    if (fields.contains(childPath)) {
                        parseValue(parser, values);
                    } else if (isAncestorOfAField(childPath)) {
                        parseAt(parser, childPath, values);
                    } else {
                        parser.skipChildren();
                    }
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                for (token = parser.nextToken(); token != null && token != XContentParser.Token.END_ARRAY; token = parser.nextToken()) {
                    parseAt(parser, path, values);
                }
            }
        }

        /**
         * Adds the values of a field in {@link #fields}, which the parser is on, as the field's own loader adds those it keeps whole: for a
         * field that reads one value at a time, the reader parses a string, a null and any structure, and a number, a boolean or a
         * binary value is converted from the type the source holds it in, a {@code keyword} that holds a number or a boolean
         * included.
         */
        private void parseValue(XContentParser parser, List<T> values) throws IOException {
            if (reader instanceof FallbackSyntheticSourceBlockLoader.SingleValueReader<?> == false) {
                reader.parse(parser, values);
                return;
            }
            switch (parser.currentToken()) {
                case START_ARRAY -> {
                    for (var token = parser.nextToken(); token != null && token != XContentParser.Token.END_ARRAY; token = parser
                        .nextToken()) {
                        parseValue(parser, values);
                    }
                }
                case VALUE_STRING -> reader.parse(parser, values);
                case VALUE_NUMBER -> reader.convertValue(
                    textScalars ? new BytesRef(String.valueOf(parser.numberValue())) : parser.numberValue(),
                    values
                );
                case VALUE_BOOLEAN -> reader.convertValue(
                    textScalars ? new BytesRef(String.valueOf(parser.booleanValue())) : parser.booleanValue(),
                    values
                );
                case VALUE_EMBEDDED_OBJECT -> reader.convertValue(parser.binaryValue(), values);
                default -> reader.parse(parser, values);
            }
        }

        @Override
        public boolean canReuse(int startingDocID) {
            return true;
        }

        @Override
        public void close() {
            breaker.addWithoutBreaking(-BlockSourceReader.ESTIMATED_SIZE);
        }

        @Override
        public String toString() {
            return "NestedChildSource[" + reader + "]";
        }
    }
}
