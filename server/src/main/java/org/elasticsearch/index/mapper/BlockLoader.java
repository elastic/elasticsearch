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
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOSupplier;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.index.mapper.blockloader.ConstantBytes;
import org.elasticsearch.index.mapper.blockloader.ConstantNull;
import org.elasticsearch.index.mapper.blockloader.docvalues.BlockDocValuesReader;
import org.elasticsearch.index.mapper.blockloader.docvalues.fn.MvMaxLongsFromDocValuesBlockLoader;
import org.elasticsearch.index.mapper.blockloader.docvalues.fn.MvMinLongsFromDocValuesBlockLoader;
import org.elasticsearch.search.fetch.StoredFieldsSpec;
import org.elasticsearch.search.lookup.Source;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Loads values from a chunk of lucene documents into a "Block" for the compute engine.
 * <p>
 *     Think of a Block as an array of values for a sequence of lucene documents. That's
 *     almost true! For the purposes of implementing {@link BlockLoader}, it's close enough.
 *     The compute engine operates on arrays because the good folks that build CPUs have
 *     spent the past 40 years making them really really good at running tight loops over
 *     arrays of data. So we play along with the CPU and make arrays.
 * </p>
 * <h2>How to implement</h2>
 * <p>
 *     There are a lot of interesting choices hiding in here to make getting those arrays
 *     out of lucene work well:
 * </p>
 * <ul>
 *     <li>
 *         {@code doc_values} are already on disk in array-like structures so we prefer
 *         to just copy them into an array in one loop inside {@link ColumnAtATimeReader}.
 *         Well, not entirely array-like. {@code doc_values} are designed to be read in
 *         non-descending order (think {@code 0, 1, 1, 4, 9}) and will fail if they are
 *         read truly randomly. This lets the doc values implementations have some
 *         chunking/compression/magic on top of the array-like on disk structure. The
 *         caller manages this, always putting {@link Docs} in non-descending order.
 *         Extend {@link BlockDocValuesReader} to implement all this.
 *     </li>
 *     <li>
 *         All stored {@code stored} fields for each document are stored on disk together,
 *         compressed with a general purpose compression algorithm like
 *         <a href="https://en.wikipedia.org/wiki/Zstd">Zstd</a>. Blocks of documents are
 *         compressed together to get a better compression ratio. Just like doc values,
 *         we read them in non-descending order. Unlike doc values, we read all fields for a
 *         document at once. Because reading one requires decompressing them all. We do
 *         this by returning {@code null} from {@link BlockLoader#columnAtATimeReader}
 *         to signal that we can't load the whole column at once. Instead, we implement a
 *         {@link RowStrideReader} which the caller will call once for each doc. Extend
 *         {@link BlockStoredFieldsReader} to implement all this.
 *     </li>
 *     <li>
 *         Fields loaded from {@code _source} are an extra special case of {@code stored}
 *         fields. {@code _source} itself is just another stored field, compressed in chunks
 *         with all the other stored fields. It's the original bytes sent when indexing the
 *         document. Think {@code json} or {@code yaml}. When we need fields from
 *         {@code _source} we get it from the stored fields reader infrastructure and then
 *         explode it into a {@link Map} representing the original {@code json} and
 *         the {@link RowStrideReader} implementation grabs the parts of the {@code json}
 *         it needs. Extend {@link BlockSourceReader} to implement all this.
 *     </li>
 *     <li>
 *         Synthetic {@code _source} complicates this further by storing fields in somewhat
 *         unexpected places, but is otherwise like a {@code stored} field reader. Use
 *         {@link FallbackSyntheticSourceBlockLoader} to implement all this.
 *     </li>
 * </ul>
 * <h2>How many to implement</h2>
 * <p>
 *     Generally reads are faster from {@code doc_values}, slower from {@code stored} fields,
 *     and even slower from {@code _source}. If we get to chose, we pick {@code doc_values}.
 *     But we work with what's on disk and that's a product of the field type and what the user's
 *     configured. Picking the optimal choice given what's on disk is the responsibility of each
 *     field's {@link MappedFieldType#blockLoader} method. The more configurable the field's
 *     storage strategies the more {@link BlockLoader}s you have to implement to integrate it
 *     with ESQL. It can get to be a lot. Sorry.
 * </p>
 * <p>
 *     For a field to be supported by ESQL fully it has to be loadable if it was configured to be
 *     stored in any way. It's possible to turn off storage entirely by turning off
 *     {@code doc_values} and {@code _source} and {@code stored} fields. In that case, it's
 *     acceptable to return {@link ConstantNull}. User turned the field off, best we can do
 *     is {@code null}.
 * </p>
 * <p>
 *     We also sometimes want to "push" executing some ESQL functions into the block loader itself.
 *     Usually we do this when it's a ton faster. See the docs for {@code BlockLoaderExpression}
 *     for why and how we do this.
 * </p>
 * <p>
 *     For example, {@code long} fields implement these block loaders:
 * </p>
 * <ul>
 *     <li>
 *         {@link org.elasticsearch.index.mapper.blockloader.docvalues.LongsBlockLoader} to read
 *         from {@code doc_values}.
 *     </li>
 *     <li>
 *         {@link org.elasticsearch.index.mapper.BlockSourceReader.LongsBlockLoader} to read from
 *         {@code _source}.
 *     </li>
 *     <li>
 *         A specially configured {@link FallbackSyntheticSourceBlockLoader} to read synthetic
 *         {@code _source}.
 *     </li>
 *     <li>
 *         {@link MvMinLongsFromDocValuesBlockLoader} to read {@code MV_MIN(long_field)} from
 *         {@code doc_values}.
 *     </li>
 *     <li>
 *         {@link MvMaxLongsFromDocValuesBlockLoader} to read {@code MV_MAX(long_field)} from
 *         {@code doc_values}.
 *     </li>
 * </ul>
 * <p>
 *     NOTE: We can't read from {@code long}s from {@code stored} fields which is a
 *     <a href="https://github.com/elastic/elasticsearch/issues/138019">bug</a>, but maybe not
 *     a terrible one because it's very uncommon to configure {@code long} to be {@code stored}
 *     but to disable {@code _source} and {@code doc_values}. Nothing's perfect. Especially
 *     code.
 * </p>
 * <h2>Why is {@link AllReader}?</h2>
 * <p>
 *     When we described how to read from {@code doc_values} we said we <strong>prefer</strong>
 *     to use {@link ColumnAtATimeReader}. But some callers don't support reading column-at-a-time
 *     and need to read row-by-row. So we also need an implementation of {@link RowStrideReader}
 *     that reads from {@code doc_values}. Usually it's most convenient to implement both of those
 *     in the same {@code class}. {@link AllReader} is an interface for those sorts of classes, and
 *     you'll see it in the {@code doc_values} code frequently.
 * </p>
 * <h2>Why is {@link #rowStrideStoredFieldSpec}?</h2>
 * <p>
 *     When decompressing {@code stored} fields lucene can skip stored field that aren't used. They
 *     still have to be decompressed, but they aren't turned into java objects which saves a fair bit
 *     of work. If you don't need any stored fields return {@link StoredFieldsSpec#NO_REQUIREMENTS}.
 *     Otherwise, return what you need.
 * </p>
 * <h2>Thread safety</h2>
 * <p>
 *     Instances of this class must be immutable and thread safe. Instances of
 *     {@link ColumnAtATimeReader} and {@link RowStrideReader} are all mutable and can only
 *     be accessed by one thread at a time but <strong>may</strong> be passed between threads.
 *     See implementations {@link Reader#canReuse} for how that's handled. "Normal" java objects
 *     don't need to do anything special to be kicked from thread to thread - the transfer itself
 *     establishes a {@code happens-before} relationship that makes everything you need visible.
 *     But Lucene's readers aren't "normal" java objects and sometimes need to be rebuilt if we
 *     shift threads.
 * </p>
 */
public interface BlockLoader {
    /**
     * @deprecated remove me once serverless migrates
     */
    @Deprecated
    static BlockLoader constantBytes(BytesRef value) {
        return new ConstantBytes(value);
    }

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
         *
         * @param nullsFiltered  if {@code true}, then target docs are guaranteed to have a value for the field.
         *                       see {@link ColumnAtATimeReader#read(BlockFactory, Docs, int, boolean)}
         * @param toDouble       a function to convert long values to double, or null if no conversion is needed/supported
         * @param toInt          whether to convert to int in case int block / vector is needed
         * @param binaryMultiValuedFormat whether the multi-valued binary format is used (CustomBinaryDocValuesField).
         */
        @Nullable
        BlockLoader.Block tryRead(
            BlockFactory factory,
            Docs docs,
            int offset,
            boolean nullsFiltered,
            BlockDocValuesReader.ToDouble toDouble,
            boolean toInt,
            boolean binaryMultiValuedFormat
        ) throws IOException;
    }

    /**
     * An interface for readers that attempt to load BytesRef length values directly without loading BytesRefs.
     * <p>
     * Implementations may return {@code null} if they are unable to load the requested values,
     * for example due to unsupported underlying data.
     * This allows callers to optimistically try optimized loading strategies first, and fall back if necessary.
     */
    interface OptionalLengthReader {
        /**
         * Attempts to read the values of all documents in {@code docs}
         * Returns {@code null} if unable to load the values.
         *
         * @param nullsFiltered  if {@code true}, then target docs are guaranteed to have a value for the field.
         *                       see {@link ColumnAtATimeReader#read(BlockFactory, Docs, int, boolean)}
         */
        @Nullable
        BlockLoader.Block tryReadLength(BlockFactory factory, Docs docs, int offset, boolean nullsFiltered) throws IOException;

        NumericDocValues toLengthValues();
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

        /**
         * Whether stored fields have already been loaded for the current document.
         * If the stored fields are not loaded yet, the block loader might avoid loading them when not needed.
         */
        boolean loaded();
    }

    /**
     * The {@link BlockLoader.Builder} for data of this type. Called when
     * loading from a multi-segment or unsorted block.
     */
    Builder builder(BlockFactory factory, int expectedCount);

    /**
     * Build a column-at-a-time reader. <strong>May</strong> return {@code null}
     * if the underlying storage needs to be loaded row-by-row. Callers should try
     * this first, only falling back to {@link #rowStrideReader} if this returns
     * {@code null} or if they can't load column-at-a-time themselves.
     */
    @Nullable
    IOSupplier<ColumnAtATimeReader> columnAtATimeReader(LeafReaderContext context) throws IOException;

    /**
     * Build a row-by-row reader. Must <strong>never</strong> return {@code null},
     * evan if the underlying storage prefers to be loaded column-at-a-time. Some
     * callers simply can't load column-at-a-time so all implementations must support
     * this method.
     */
    RowStrideReader rowStrideReader(LeafReaderContext context) throws IOException;

    /**
     * What {@code stored} fields are needed by this reader.
     */
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
     * A {@link BlockLoader} that conditionally selects one of two underlying loaders based on the underlying data.
     * It prefers the {@code preferLoader} when possible, falling back to the {@code fallbackLoader} otherwise.
     * <p>
     * For example, a text field with a synthetic source can be loaded from its child keyword field when possible,
     * which is faster than loading from stored fields:
     * <pre>
     * {
     *     "parent_text_field": {
     *         "type": "text",
     *         "fields": {
     *             "child_keyword_field": {
     *                 "type": "keyword",
     *                 "ignore_above": 256
     *             }
     *         }
     *     }
     * }
     * </pre>
     * If no values in a segment exceed the 256-character limit, then we can safely use the block loader from the
     * keyword field for the entire segment. Alternatively, on a per-document basis, if doc-1 has the value "a"
     * (under the limit) and doc-2 has the value "bcd..." (exceeds the limit), we can load doc-1 from the doc_values
     * of keyword field and doc-2 from the slower stored fields.
     */
    abstract class ConditionalBlockLoader implements BlockLoader {
        private final BlockLoader preferLoader;
        private final BlockLoader fallbackLoader;

        protected ConditionalBlockLoader(BlockLoader preferLoader, BlockLoader fallbackLoader) {
            this.preferLoader = preferLoader;
            this.fallbackLoader = fallbackLoader;
        }

        @Override
        public Builder builder(BlockFactory factory, int expectedCount) {
            return fallbackLoader.builder(factory, expectedCount);
        }

        /**
         * Determines whether the preferred loader can be used for all documents in the given leaf context.
         */
        protected abstract boolean canUsePreferLoaderForLeaf(LeafReaderContext context) throws IOException;

        /**
         * Whether we can use the prefer loader for the given doc ID. If {@code true}, then the preferred loader is used
         * to avoid loading stored fields or source.
         */
        protected abstract boolean canUsePreferLoaderForDoc(int docId) throws IOException;

        @Override
        public IOSupplier<ColumnAtATimeReader> columnAtATimeReader(LeafReaderContext context) throws IOException {
            if (canUsePreferLoaderForLeaf(context)) {
                return preferLoader.columnAtATimeReader(context);
            } else {
                return fallbackLoader.columnAtATimeReader(context);
            }
        }

        @Override
        public RowStrideReader rowStrideReader(LeafReaderContext context) throws IOException {
            if (preferLoader.rowStrideStoredFieldSpec().noRequirements() == false) {
                return fallbackLoader.rowStrideReader(context);
            }
            RowStrideReader preferReader = preferLoader.rowStrideReader(context);
            if (canUsePreferLoaderForLeaf(context)) {
                return preferReader;
            }
            RowStrideReader fallbackReader = fallbackLoader.rowStrideReader(context);
            return new RowStrideReader() {
                @Override
                public void read(int docId, StoredFields storedFields, Builder builder) throws IOException {
                    if (storedFields.loaded() == false && canUsePreferLoaderForDoc(docId)) {
                        preferReader.read(docId, storedFields, builder);
                    } else {
                        fallbackReader.read(docId, storedFields, builder);
                    }
                }

                @Override
                public boolean canReuse(int startingDocID) {
                    return fallbackReader.canReuse(startingDocID) && preferReader.canReuse(startingDocID);
                }
            };
        }

        @Override
        public StoredFieldsSpec rowStrideStoredFieldSpec() {
            return fallbackLoader.rowStrideStoredFieldSpec();
        }

        @Override
        public boolean supportsOrdinals() {
            return false;
        }

        @Override
        public SortedSetDocValues ordinals(LeafReaderContext context) throws IOException {
            return null;
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
         * Adjust the circuit breaker with the given delta, if the delta is negative, the breaker will
         * be adjusted without tripping.
         * @throws CircuitBreakingException if the breaker was put above its limit
         */
        void adjustBreaker(long delta) throws CircuitBreakingException;

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
         * Build a specialized builder for singleton dense {@link BytesRef} fields with the following constraints:
         * <ul>
         *     <li>Only one value per document can be collected</li>
         *     <li>No more than expectedCount values can be collected</li>
         * </ul>
         *
         * @param expectedCount The maximum number of values to be collected.
         */
        SingletonBytesRefBuilder singletonBytesRefs(int expectedCount);

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
         * Build a specialized builder for singleton dense int based fields with the following constraints:
         * <ul>
         *     <li>Only one value per document can be collected</li>
         *     <li>No more than expectedCount values can be collected</li>
         * </ul>
         *
         * @param expectedCount The maximum number of values to be collected.
         */
        SingletonIntBuilder singletonInts(int expectedCount);

        /**
         * Build a specialized builder for singleton dense double based fields with the following constraints:
         * <ul>
         *     <li>Only one value per document can be collected</li>
         *     <li>No more than expectedCount values can be collected</li>
         * </ul>
         *
         * @param expectedCount The maximum number of values to be collected.
         */
        SingletonDoubleBuilder singletonDoubles(int expectedCount);

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
         * Build a block that contains {@code value} repeated
         * {@code count} times.
         */
        Block constantInt(int value, int count);

        /**
         * Build a reader for reading {@link SortedDocValues}
         */
        SingletonOrdinalsBuilder singletonOrdinalsBuilder(SortedDocValues ordinals, int count, boolean isDense);

        /**
         * Build a reader for reading {@link SortedSetDocValues}
         */
        SortedSetOrdinalsBuilder sortedSetOrdinalsBuilder(SortedSetDocValues ordinals, int count);

        AggregateMetricDoubleBuilder aggregateMetricDoubleBuilder(int count);

        LongRangeBuilder longRangeBuilder(int count);

        Block buildAggregateMetricDoubleDirect(Block minBlock, Block maxBlock, Block sumBlock, Block countBlock);

        ExponentialHistogramBuilder exponentialHistogramBlockBuilder(int count);

        Block buildExponentialHistogramBlockDirect(
            Block minima,
            Block maxima,
            Block sums,
            Block valueCounts,
            Block zeroThresholds,
            Block encodedHistograms
        );

        Block buildTDigestBlockDirect(Block encodedDigests, Block minima, Block maxima, Block sums, Block valueCounts);

        TDigestBuilder tdigestBlockBuilder(int count);
    }

    /**
     * A columnar representation of homogenous data. It has a position (row) count, and
     * various data retrieval methods for accessing the underlying data that is stored at a given
     * position. In other words, a fancy wrapper over an array.
     * <p>
     *     <strong>This</strong> is just a marker interface for these results. The compute engine
     *     has fleshed out implementations.
     * </p>
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

    /**
     * Specialized builder for collecting dense arrays of BytesRef values.
     */
    interface SingletonBytesRefBuilder extends Builder {
        /**
         * Append multiple BytesRef. Offsets contains offsets of each BytesRef in the byte array.
         * The length of the offsets array is one more than the number of BytesRefs.
         */
        SingletonBytesRefBuilder appendBytesRefs(byte[] bytes, long[] offsets) throws IOException;

        /**
         * Append multiple BytesRefs, all with the same length.
         */
        SingletonBytesRefBuilder appendBytesRefs(byte[] bytes, long bytesRefLengths) throws IOException;
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

    /**
     * Specialized builder for collecting dense arrays of double values.
     */
    interface SingletonDoubleBuilder extends Builder {
        SingletonDoubleBuilder appendLongs(BlockDocValuesReader.ToDouble toDouble, long[] values, int from, int length);
    }

    /**
     * Specialized builder for collecting dense arrays of int values.
     */
    interface SingletonIntBuilder extends Builder {
        SingletonIntBuilder appendLongs(long[] values, int from, int length);

        SingletonIntBuilder appendInts(int[] values, int from, int length);
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

        /**
         * Appends a single ord for the next N positions
         */
        SingletonOrdinalsBuilder appendOrds(int ord, int length);

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

    interface LongRangeBuilder extends Builder {
        LongBuilder from();

        LongBuilder to();
    }

    interface ExponentialHistogramBuilder extends Builder {
        DoubleBuilder minima();

        DoubleBuilder maxima();

        DoubleBuilder sums();

        DoubleBuilder valueCounts();

        DoubleBuilder zeroThresholds();

        BytesRefBuilder encodedHistograms();
    }

    interface TDigestBuilder extends Builder {
        DoubleBuilder minima();

        DoubleBuilder maxima();

        DoubleBuilder sums();

        LongBuilder valueCounts();

        BytesRefBuilder encodedDigests();
    }
}
