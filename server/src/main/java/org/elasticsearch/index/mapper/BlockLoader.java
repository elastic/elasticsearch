/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.search.fetch.StoredFieldsSpec;
import org.elasticsearch.search.lookup.Source;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Interface for loading data in a block shape. Instances of this class
 * must be immutable and thread safe.
 */
public interface BlockLoader {
    /**
     * The {@link BlockLoader.Builder} for data of this type. Called when
     * loading from a multi-segment or unsorted block.
     */
    Builder builder(BlockFactory factory, int expectedCount);

    interface Reader {
        /**
         * Checks if the reader can be used to read a range documents starting with the given docID by the current thread.
         */
        boolean canReuse(int startingDocID);
    }

    interface ColumnAtATimeReader extends Reader {
        /**
         * Reads the values of all documents in {@code docs}.
         *
         * @param nullsFiltered if {@code true}, then target docs are guaranteed to have a value for the field;
         *                      otherwise, the guarantee is unknown. This enables optimizations for block loaders,
         *                      treating the field as dense (every document has value) even if it is sparse in
         *                      the index. For example, "FROM index | WHERE x != null | STATS sum(x)", after filtering out
         *                      documents without value for field x, all target documents returned from the source operator
         *                      will have a value for field x whether x is dense or sparse in the index.
         */
        BlockLoader.Block read(BlockFactory factory, Docs docs, int offset, boolean nullsFiltered) throws IOException;
    }

    /**
     * An interface for readers that attempt to load all document values in a column-at-a-time fashion.
     * <p>
     * Unlike {@link ColumnAtATimeReader}, implementations may return {@code null} if they are unable
     * to load the requested values, for example due to unsupported underlying data.
     * This allows callers to optimistically try optimized loading strategies first, and fall back if necessary.
     */
    interface OptionalColumnAtATimeReader {
        /**
         * Attempts to read the values of all documents in {@code docs}
         * Returns {@code null} if unable to load the values.
         */
        @Nullable
        BlockLoader.Block tryRead(BlockFactory factory, Docs docs, int offset) throws IOException;
    }

    interface RowStrideReader extends Reader {
        /**
         * Reads the values of the given document into the builder.
         */
        void read(int docId, StoredFields storedFields, Builder builder) throws IOException;
    }

    interface AllReader extends ColumnAtATimeReader, RowStrideReader {}

    interface StoredFields {
        /**
         * The {@code _source} of the document.
         */
        Source source() throws IOException;

        /**
         * @return the ID for the current document
         */
        String id() throws IOException;

        /**
         * @return the routing path for the current document
         */
        String routing() throws IOException;

        /**
         * @return stored fields for the current document
         */
        Map<String, List<Object>> storedFields() throws IOException;
    }

    ColumnAtATimeReader columnAtATimeReader(LeafReaderContext context) throws IOException;

    RowStrideReader rowStrideReader(LeafReaderContext context) throws IOException;

    StoredFieldsSpec rowStrideStoredFieldSpec();

    /**
     * Does this loader support loading bytes via calling {@link #ordinals}.
     */
    boolean supportsOrdinals();

    /**
     * Load ordinals for the provided context.
     */
    SortedSetDocValues ordinals(LeafReaderContext context) throws IOException;

    /**
     * In support of 'Union Types', we sometimes desire that Blocks loaded from source are immediately
     * converted in some way. Typically, this would be a type conversion, or an encoding conversion.
     * @param block original block loaded from source
     * @return converted block (or original if no conversion required)
     */
    default Block convert(Block block) {
        return block;
    }

    /**
     * Load blocks with only null.
     */
    BlockLoader CONSTANT_NULLS = new BlockLoader() {
        @Override
        public Builder builder(BlockFactory factory, int expectedCount) {
            return factory.nulls(expectedCount);
        }

        @Override
        public ColumnAtATimeReader columnAtATimeReader(LeafReaderContext context) {
            return new ConstantNullsReader();
        }

        @Override
        public RowStrideReader rowStrideReader(LeafReaderContext context) {
            return new ConstantNullsReader();
        }

        @Override
        public StoredFieldsSpec rowStrideStoredFieldSpec() {
            return StoredFieldsSpec.NO_REQUIREMENTS;
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
            return "ConstantNull";
        }
    };

    /**
     * Implementation of {@link ColumnAtATimeReader} and {@link RowStrideReader} that always
     * loads {@code null}.
     */
    class ConstantNullsReader implements AllReader {
        @Override
        public Block read(BlockFactory factory, Docs docs, int offset, boolean nullsFiltered) throws IOException {
            return factory.constantNulls(docs.count() - offset);
        }

        @Override
        public void read(int docId, StoredFields storedFields, Builder builder) throws IOException {
            builder.appendNull();
        }

        @Override
        public boolean canReuse(int startingDocID) {
            return true;
        }

        @Override
        public String toString() {
            return "constant_nulls";
        }
    }

    /**
     * Load blocks with only {@code value}.
     */
    static BlockLoader constantBytes(BytesRef value) {
        return new BlockLoader() {
            @Override
            public Builder builder(BlockFactory factory, int expectedCount) {
                return factory.bytesRefs(expectedCount);
            }

            @Override
            public ColumnAtATimeReader columnAtATimeReader(LeafReaderContext context) {
                return new ColumnAtATimeReader() {
                    @Override
                    public Block read(BlockFactory factory, Docs docs, int offset, boolean nullsFiltered) {
                        return factory.constantBytes(value, docs.count() - offset);
                    }

                    @Override
                    public boolean canReuse(int startingDocID) {
                        return true;
                    }

                    @Override
                    public String toString() {
                        return "constant[" + value + "]";
                    }
                };
            }

            @Override
            public RowStrideReader rowStrideReader(LeafReaderContext context) {
                return new RowStrideReader() {
                    @Override
                    public void read(int docId, StoredFields storedFields, Builder builder) {
                        ((BlockLoader.BytesRefBuilder) builder).appendBytesRef(value);
                    }

                    @Override
                    public boolean canReuse(int startingDocID) {
                        return true;
                    }

                    @Override
                    public String toString() {
                        return "constant[" + value + "]";
                    }
                };
            }

            @Override
            public StoredFieldsSpec rowStrideStoredFieldSpec() {
                return StoredFieldsSpec.NO_REQUIREMENTS;
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
                return "ConstantBytes[" + value + "]";
            }
        };
    }

    abstract class Delegating implements BlockLoader {
        protected final BlockLoader delegate;

        protected Delegating(BlockLoader delegate) {
            this.delegate = delegate;
        }

        @Override
        public Builder builder(BlockFactory factory, int expectedCount) {
            return delegate.builder(factory, expectedCount);
        }

        @Override
        public ColumnAtATimeReader columnAtATimeReader(LeafReaderContext context) throws IOException {
            ColumnAtATimeReader reader = delegate.columnAtATimeReader(context);
            if (reader == null) {
                return null;
            }
            return new ColumnAtATimeReader() {
                @Override
                public Block read(BlockFactory factory, Docs docs, int offset, boolean nullsFiltered) throws IOException {
                    return reader.read(factory, docs, offset, nullsFiltered);
                }

                @Override
                public boolean canReuse(int startingDocID) {
                    return reader.canReuse(startingDocID);
                }

                @Override
                public String toString() {
                    return "Delegating[to=" + delegatingTo() + ", impl=" + reader + "]";
                }
            };
        }

        @Override
        public RowStrideReader rowStrideReader(LeafReaderContext context) throws IOException {
            RowStrideReader reader = delegate.rowStrideReader(context);
            if (reader == null) {
                return null;
            }
            return new RowStrideReader() {
                @Override
                public void read(int docId, StoredFields storedFields, Builder builder) throws IOException {
                    reader.read(docId, storedFields, builder);
                }

                @Override
                public boolean canReuse(int startingDocID) {
                    return reader.canReuse(startingDocID);
                }

                @Override
                public String toString() {
                    return "Delegating[to=" + delegatingTo() + ", impl=" + reader + "]";
                }
            };
        }

        @Override
        public StoredFieldsSpec rowStrideStoredFieldSpec() {
            return delegate.rowStrideStoredFieldSpec();
        }

        @Override
        public boolean supportsOrdinals() {
            return delegate.supportsOrdinals();
        }

        @Override
        public SortedSetDocValues ordinals(LeafReaderContext context) throws IOException {
            return delegate.ordinals(context);
        }

        protected abstract String delegatingTo();

        @Override
        public final String toString() {
            return "Delegating[to=" + delegatingTo() + ", impl=" + delegate + "]";
        }
    }

    /**
     * A list of documents to load. Documents are always in non-decreasing order.
     */
    interface Docs {
        int count();

        int get(int i);
    }

    /**
     * Builds block "builders" for loading data into blocks for the compute engine.
     * It's important for performance that this only have one implementation in
     * production code. That implementation sits in the "compute" project. The is
     * also a test implementation, but there may be no more other implementations.
     */
    interface BlockFactory {
        /**
         * Build a builder to load booleans as loaded from doc values. Doc values
         * load booleans in sorted order.
         */
        BooleanBuilder booleansFromDocValues(int expectedCount);

        /**
         * Build a builder to load booleans without any loading constraints.
         */
        BooleanBuilder booleans(int expectedCount);

        /**
         * Build a builder to load {@link BytesRef}s as loaded from doc values.
         * Doc values load {@linkplain BytesRef}s deduplicated and in sorted order.
         */
        BytesRefBuilder bytesRefsFromDocValues(int expectedCount);

        /**
         * Build a builder to load {@link BytesRef}s without any loading constraints.
         */
        BytesRefBuilder bytesRefs(int expectedCount);

        /**
         * Build a builder to load doubles as loaded from doc values.
         * Doc values load doubles in sorted order.
         */
        DoubleBuilder doublesFromDocValues(int expectedCount);

        /**
         * Build a builder to load doubles without any loading constraints.
         */
        DoubleBuilder doubles(int expectedCount);

        /**
         * Build a builder to load dense vectors without any loading constraints.
         */
        FloatBuilder denseVectors(int expectedVectorsCount, int dimensions);

        /**
         * Build a builder to load ints as loaded from doc values.
         * Doc values load ints in sorted order.
         */
        IntBuilder intsFromDocValues(int expectedCount);

        /**
         * Build a builder to load ints without any loading constraints.
         */
        IntBuilder ints(int expectedCount);

        /**
         * Build a builder to load longs as loaded from doc values.
         * Doc values load longs in sorted order.
         */
        LongBuilder longsFromDocValues(int expectedCount);

        /**
         * Build a builder to load longs without any loading constraints.
         */
        LongBuilder longs(int expectedCount);

        /**
         * Build a specialized builder for singleton dense long based fields with the following constraints:
         * <ul>
         *     <li>Only one value per document can be collected</li>
         *     <li>No more than expectedCount values can be collected</li>
         * </ul>
         *
         * @param expectedCount The maximum number of values to be collected.
         */
        SingletonLongBuilder singletonLongs(int expectedCount);

        /**
         * Build a builder to load only {@code null}s.
         */
        Builder nulls(int expectedCount);

        /**
         * Build a block that contains only {@code null}.
         */
        Block constantNulls(int count);

        /**
         * Build a block that contains {@code value} repeated
         * {@code size} times.
         */
        Block constantBytes(BytesRef value, int count);

        /**
         * Build a reader for reading {@link SortedDocValues}
         */
        SingletonOrdinalsBuilder singletonOrdinalsBuilder(SortedDocValues ordinals, int count, boolean isDense);

        /**
         * Build a reader for reading {@link SortedSetDocValues}
         */
        SortedSetOrdinalsBuilder sortedSetOrdinalsBuilder(SortedSetDocValues ordinals, int count);

        AggregateMetricDoubleBuilder aggregateMetricDoubleBuilder(int count);
    }

    /**
     * Marker interface for block results. The compute engine has a fleshed
     * out implementation.
     */
    interface Block extends Releasable {}

    /**
     * A builder for typed values. For each document you may either call
     * {@link #appendNull}, {@code append<Type>}, or
     * {@link #beginPositionEntry} followed by two or more {@code append<Type>}
     * calls, and then {@link #endPositionEntry}.
     */
    interface Builder extends Releasable {
        /**
         * Build the actual block.
         */
        Block build();

        /**
         * Insert a null value.
         */
        Builder appendNull();

        /**
         * Start a multivalued field.
         */
        Builder beginPositionEntry();

        /**
         * End a multivalued field.
         */
        Builder endPositionEntry();
    }

    interface BooleanBuilder extends Builder {
        /**
         * Appends a boolean to the current entry.
         */
        BooleanBuilder appendBoolean(boolean value);
    }

    interface BytesRefBuilder extends Builder {
        /**
         * Appends a BytesRef to the current entry.
         */
        BytesRefBuilder appendBytesRef(BytesRef value);
    }

    interface FloatBuilder extends Builder {
        /**
         * Appends a float to the current entry.
         */
        FloatBuilder appendFloat(float value);
    }

    interface DoubleBuilder extends Builder {
        /**
         * Appends a double to the current entry.
         */
        DoubleBuilder appendDouble(double value);
    }

    interface IntBuilder extends Builder {
        /**
         * Appends an int to the current entry.
         */
        IntBuilder appendInt(int value);
    }

    /**
     * Specialized builder for collecting dense arrays of long values.
     */
    interface SingletonLongBuilder extends Builder {

        SingletonLongBuilder appendLong(long value);

        SingletonLongBuilder appendLongs(long[] values, int from, int length);
    }

    interface LongBuilder extends Builder {
        /**
         * Appends a long to the current entry.
         */
        LongBuilder appendLong(long value);
    }

    interface SingletonOrdinalsBuilder extends Builder {
        /**
         * Appends an ordinal to the builder.
         */
        SingletonOrdinalsBuilder appendOrd(int value);

        SingletonOrdinalsBuilder appendOrds(int[] values, int from, int length, int minOrd, int maxOrd);
    }

    interface SortedSetOrdinalsBuilder extends Builder {
        /**
         * Appends an ordinal to the builder.
         */
        SortedSetOrdinalsBuilder appendOrd(int value);
    }

    interface AggregateMetricDoubleBuilder extends Builder {

        DoubleBuilder min();

        DoubleBuilder max();

        DoubleBuilder sum();

        IntBuilder count();
    }
}
