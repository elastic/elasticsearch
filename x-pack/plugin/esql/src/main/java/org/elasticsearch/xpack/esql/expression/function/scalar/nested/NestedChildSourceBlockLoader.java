/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.expression.function.scalar.nested;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedSetDocValues;
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

/**
 * Reads a field under a nested object that keeps its values only in synthetic source from the child's object in the parent's
 * {@code _source}. The field's own loader reads the child's {@code _ignored_source}, but where the nested object or an object
 * above it keeps its whole source, as {@code synthetic_source_keep} can, those values are in the parent's instead. The
 * parent's synthetic {@code _source} holds them wherever they are kept, and they are converted as the field's own loader does.
 */
final class NestedChildSourceBlockLoader implements BlockLoader {
    private final FallbackSyntheticSourceBlockLoader fallback;
    private final String field;

    NestedChildSourceBlockLoader(FallbackSyntheticSourceBlockLoader fallback, String field) {
        this.fallback = fallback;
        this.field = field;
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

        /**
         * Adds the values of {@link #field} under the value the parser is on, which is at {@code path}, or the root if it is
         * {@code null}, as the field's own loader does.
         */
        private void parseAt(XContentParser parser, String path, List<T> values) throws IOException {
            XContentParser.Token token = parser.currentToken();
            if (token == XContentParser.Token.START_OBJECT) {
                while (parser.nextToken() == XContentParser.Token.FIELD_NAME) {
                    String childPath = path == null ? parser.currentName() : path + "." + parser.currentName();
                    parser.nextToken();
                    if (childPath.equals(field)) {
                        reader.parse(parser, values);
                    } else if (field.startsWith(childPath) && field.charAt(childPath.length()) == '.') {
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
