/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene.read;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.IOFunction;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.DocBlock;
import org.elasticsearch.compute.data.DocVector;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.IndexedByShardId;
import org.elasticsearch.compute.lucene.query.LuceneSourceOperator;
import org.elasticsearch.compute.operator.AbstractPageMappingToIteratorOperator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.BlockLoaderStoredFieldsFromLeafLoader;
import org.elasticsearch.index.mapper.SourceLoader;
import org.elasticsearch.index.mapper.blockloader.ConstantNull;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.search.fetch.StoredFieldsSpec;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Function;

/**
 * Loads values from Lucene.
 * <p>
 *     Input pages must contain a {@link DocVector} which describes the location of lucene document.
 *     We represent documents as a triple of integers that we all "shard", "segment", and "doc".
 *     "Shard" point to the lucene index. "Segment" points to the segment inside the index. And
 *     "doc" is the offset within that segment. Input look like
 * </p>
 * {@snippet lang="txt" :
 * ┌───────────────────────┐
 * │          doc          │
 * ├───────┬─────────┬─────┤
 * │ shard │ segment │ doc │
 * ├───────┼─────────┼─────┤
 * │     0 │       0 │   0 │
 * │     0 │       0 │   1 │
 * │     0 │       0 │   2 │
 * │     0 │       0 │   3 │
 * │     0 │       0 │  12 │
 * └───────┴─────────┴─────┘
 * }
 * <p>
 *     And output pages have the loaded fields appended:
 * </p>
 * {@snippet lang="txt" :
 * ┌───────────────────────┬──────┐
 * │          doc          │      │
 * ├───────┬─────────┬─────┤ name │
 * │ shard │ segment │ doc │      │
 * ├───────┼─────────┼─────┼──────┤
 * │     0 │       0 │   0 │ foo  │
 * │     0 │       0 │   1 │ bar  │
 * │     0 │       0 │   2 │ baz  │
 * │     0 │       0 │   3 │ foo  │
 * │     0 │       0 │  12 │ bar  │
 * └───────┴─────────┴─────┴──────┘
 * }
 * <h2>Are we loading from one segment?</h2>
 * <p>
 *     If we load from one segment then we can load more efficiently using
 *     {@link ValuesFromSingleReader}. Otherwise, we use {@link ValuesFromManyReader}.
 *     Loading from one shard looks like the example above. And it's super
 *     common. {@link LuceneSourceOperator} always loads fields this way and
 *     that's "high performance" way of reading documents. But after a sort
 *     then you are likely to be loading from many segments. Which looks like:
 * </p>
 * {@snippet lang="txt" :
 * ┌───────────────────────┐
 * │          doc          │
 * ├───────┬─────────┬─────┤
 * │ shard │ segment │ doc │
 * ├───────┼─────────┼─────┤
 * │     0 │       0 │   0 │
 * │     0 │       1 │   0 │
 * │     0 │       1 │   1 │
 * │     1 │       0 │   1 │
 * │     1 │       1 │  12 │
 * └───────┴─────────┴─────┘
 * }
 * <h2>{@link BlockLoader.RowStrideReader row-by-row} vs {@link BlockLoader.ColumnAtATimeReader column-at-a-time}</h2>
 * <p>
 *     Lucene can be configured to score data in two kinds of ways: rows and columns.
 *     All fields configured for "row" style storage and concatenated together and compressed
 *     with something like <a href="https://en.wikipedia.org/wiki/Zstd">zstd</a>. "Column"
 *     fields are stored as a dense array of values on disk and compressed using math tricks.
 * </p>
 * <p>The "row" style fields need to be loaded together, one row at a time.</p>
 * {@snippet lang="txt" :
 * ┌─────┬────────┐   ┌─────┬────────┐   ┌─────┬────────┐   ┌─────┬────────┐   ┌─────┬────────┐   ┌─────┬────────┐
 * │ ref │ class  │   │ ref │ class  │   │ ref │ class  │   │ ref │ class  │   │ ref │ class  │   │ ref │ class  │
 * ├─────┼────────┤   ├─────┼────────┤   ├─────┼────────┤   ├─────┼────────┤   ├─────┼────────┤   ├─────┼────────┤
 * │     │        │   │ 173 │ Euclid │   │ 173 │ Euclid │   │ 173 │ Euclid │   │ 173 │ Euclid │   │ 173 │ Euclid │
 * │     │        │ ⟶ │     │        │ ⟶ │ 049 │ Euclid │ ⟶ │ 049 │ Euclid │ ⟶ │ 049 │ Euclid │ ⟶ │ 049 │ Euclid │
 * │     │        │   │     │        │   │     │        │   │ 096 │ Euclid │   │ 096 │ Euclid │   │ 096 │ Euclid │
 * │     │        │   │     │        │   │     │        │   │     │        │   │ 682 │ Keter  │   │ 682 │ Keter  │
 * │     │        │   │     │        │   │     │        │   │     │        │   │     │        │   │ 055 │ Keter  │
 * └─────┴────────┘   └─────┴────────┘   └─────┴────────┘   └─────┴────────┘   └─────┴────────┘   └─────┴────────┘
 * }
 * <p>The "column" style fields need to be loaded one at a time:</p>
 * {@snippet lang="txt" :
 * ┌─────┐   ┌─────┬────────┐   ┌─────┬────────┬──────┐   ┌─────┬────────┬──────┬────────────┐
 * │ ref │   │ ref │ class  │   │ ref │ class  │ site │   │ ref │ class  │ site │ casualties │
 * ├─────┤   ├─────┼────────┤   ├─────┼────────┼──────┤   ├─────┼────────┼──────┼────────────┤
 * │ 173 │   │ 173 │ Euclid │   │ 173 │ Euclid │ 19   │   │ 173 │ Euclid │ 19   │ 0          │
 * │ 049 │   │ 049 │ Euclid │   │ 049 │ Euclid │ 19   │   │ 049 │ Euclid │ 19   │ 1          │
 * │ 096 │ ⟶ │ 096 │ Euclid │ ⟶ │ 096 │ Euclid │ ██   │ ⟶ │ 096 │ Euclid │ ██   │ ██         │
 * │ 682 │   │ 682 │ Keter  │   │ 682 │ Keter  │ ██   │   │ 682 │ Keter  │ ██   │ 34         │
 * │ 055 │   │ 055 │ Keter  │   │ 055 │ Keter  │ 19   │   │ 055 │ Keter  │ 19   │ null       │
 * └─────┘   └─────┴────────┘   └─────┴────────┴──────┘   └─────┴────────┴──────┴────────────┘
 * }
 * <h2>Oh, no! Giant strings</h2>
 * <p>
 *     It's important to keep the {@link Block}s we build from being giant. A couple of mb
 *     is ok, but 100mb is not usually great for the query. The most surefire way to do this
 *     is to load fields in the order they appear in the input page and then stop if you load
 *     too much. {@link ValuesFromSingleReader} and {@link ValuesFromManyReader} don't do that
 *     because they are trying to be more efficient when loading small things. But when we load
 *     big things we use {@link ValuesFromDocSequence} to load them in the order they appear
 *     so we can always bail early.
 * </p>
 */
public class ValuesSourceReaderOperator extends AbstractPageMappingToIteratorOperator {
    private static final Logger log = LogManager.getLogger(ValuesSourceReaderOperator.class);

    /**
     * Creates a factory for {@link ValuesSourceReaderOperator}.
     * @param fields fields to load
     * @param shardContexts per-shard loading information
     * @param docChannel the channel containing the shard, leaf/segment and doc id
     */
    public record Factory(
        ByteSizeValue jumboSize,
        List<FieldInfo> fields,
        IndexedByShardId<ShardContext> shardContexts,
        boolean reuseColumnLoaders,
        int docChannel,
        double sourceReservationFactor,
        int docSequenceBytesRefFieldThreshold
    ) implements OperatorFactory {
        public Factory

        {
            if (fields.isEmpty()) {
                throw new IllegalStateException("ValuesSourceReaderOperator doesn't support empty fields");
            }
        }

        /**
         * Convenience constructor that uses the default doc-sequence threshold of 500.
         */
        public Factory(
            ByteSizeValue jumboSize,
            List<FieldInfo> fields,
            IndexedByShardId<ShardContext> shardContexts,
            boolean reuseColumnLoaders,
            int docChannel,
            double sourceReservationFactor
        ) {
            this(jumboSize, fields, shardContexts, reuseColumnLoaders, docChannel, sourceReservationFactor, 500);
        }

        @Override
        public Operator get(DriverContext driverContext) {
            return new ValuesSourceReaderOperator(
                driverContext,
                jumboSize.getBytes(),
                fields,
                shardContexts,
                reuseColumnLoaders,
                docChannel,
                sourceReservationFactor,
                docSequenceBytesRefFieldThreshold
            );
        }

        @Override
        public String describe() {
            StringBuilder sb = new StringBuilder();
            sb.append("ValuesSourceReaderOperator[fields = [");
            if (fields.size() < 10) {
                boolean first = true;
                for (FieldInfo f : fields) {
                    if (first) {
                        first = false;
                    } else {
                        sb.append(", ");
                    }
                    sb.append(f.name);
                }
            } else {
                sb.append(fields.size()).append(" fields");
            }
            return sb.append("]]").toString();
        }
    }

    /**
     * Configuration for a field to load.
     *
     * @param nullsFiltered if {@code true}, then target docs passed from the source operator are guaranteed to have a value
     *                      for the field; otherwise, the guarantee is unknown. This enables optimizations for block loaders,
     *                      treating the field as dense (every document has value) even if it is sparse in the index.
     *                      For example, "FROM index | WHERE x != null | STATS sum(x)", after filtering out documents
     *                      without value for field x, all target documents returned from the source operator
     *                      will have a value for field x whether x is dense or sparse in the index.
     * @param buildLoader builds the {@link BlockLoader} which loads the actual blocks and an
     *                    optional type converter for a given shard.
     */
    public record FieldInfo(String name, ElementType type, boolean nullsFiltered, BuildLoader buildLoader) {}

    /**
     * Builds a {@link LoaderAndConverter} for a given shard.
     */
    public interface BuildLoader {
        LoaderAndConverter build(DriverContext.WarningsMode warningsMode, int shard);
    }

    /**
     * Singleton to load constant {@code null}s.
     */
    public static final LoaderAndConverter LOAD_CONSTANT_NULLS = new LoaderAndConverter(ConstantNull.INSTANCE, null);

    /**
     * Loads directly from the {@code loader}.
     */
    public static LoaderAndConverter load(BlockLoader loader) {
        return new LoaderAndConverter(loader, null);
    }

    /**
     * Loads from the {@code loader} and then converts the values using the {@code converter}.
     *
     */
    public static LoaderAndConverter loadAndConvert(BlockLoader loader, ConverterFactory converter) {
        return new LoaderAndConverter(loader, converter);
    }

    public record ShardContext(
        IndexReader reader,
        Function<Set<String>, SourceLoader> newSourceLoader,
        double storedFieldsSequentialProportion
    ) {}

    final DriverContext driverContext;

    /**
     * Owns the "evaluators" of type conversions that be performed on load.
     * Converters are built on first need and kept until the {@link ValuesSourceReaderOperator}
     * is {@link #close closed}.
     */
    private final ConverterEvaluators converterEvaluators = new ConverterEvaluators();

    /**
     * When the loaded fields {@link Block}s' estimated size grows larger than this,
     * we finish loading the {@linkplain Page} and return it, even if
     * the {@linkplain Page} is shorter than the incoming {@linkplain Page}.
     * <p>
     *     NOTE: This only applies when loading single segment non-descending
     *     row stride bytes. This is the most common way to get giant fields,
     *     but it isn't all the ways.
     * </p>
     */
    final long jumboBytes;
    final int docSequenceBytesRefFieldThreshold;
    final FieldWork[] fields;
    final IndexedByShardId<? extends ShardContext> shardContexts;
    private final boolean reuseColumnLoaders;
    private final int docChannel;

    /**
     * Multiplier applied to {@link #lastKnownSourceSize} to pre-reserve memory on the circuit
     * breaker before loading {@code _source}. A factor of 3.0 covers the large untracked
     * allocations from source parsing such as the scratch buffer, SourceFilter.filterBytes() and
     * JSON parsing overhead. This is a heuristic and can be adjusted based on observed
     * memory usage patterns.
     */
    final double sourceReservationFactor;

    private final Map<String, Integer> readersBuilt = new TreeMap<>();
    long valuesLoaded;

    private int lastShard = -1;
    private int lastSegment = -1;

    /**
     * The maximum raw byte size of _source observed so far. This persists across pages so
     * the pre-reservation for source parsing overhead can protect even the first (and only)
     * document in a page. On a 512MB JVM, jumboBytes is ~512KB, so each 5MB text field
     * creates a 1-doc page. Without persisting this, every page starts with 0 reservation.
     */
    long lastKnownSourceSize;

    /**
     * Persistent reservation on the circuit breaker for the expected overhead of _source
     * parsing. Held while this operator is active and loading large documents. When multiple
     * operators load large _source concurrently, their persistent reservations accumulate
     * on the shared circuit breaker, causing it to trip before the aggregate untracked
     * temporaries from concurrent loads can overwhelm the heap.
     */
    private long sourceLoadingReservation;

    /**
     * Creates a new extractor
     * @param fields fields to load
     * @param docChannel the channel containing the shard, leaf/segment and doc id
     */
    public ValuesSourceReaderOperator(
        DriverContext driverContext,
        long jumboBytes,
        List<FieldInfo> fields,
        IndexedByShardId<? extends ShardContext> shardContexts,
        boolean reuseColumnLoaders,
        int docChannel,
        double sourceReservationFactor,
        int docSequenceBytesRefFieldThreshold
    ) {
        if (fields.isEmpty()) {
            throw new IllegalStateException("ValuesSourceReaderOperator doesn't support empty fields");
        }
        this.driverContext = driverContext;
        this.jumboBytes = jumboBytes;
        this.docSequenceBytesRefFieldThreshold = docSequenceBytesRefFieldThreshold;
        this.fields = new FieldWork[fields.size()];
        for (int i = 0; i < this.fields.length; i++) {
            this.fields[i] = new FieldWork(fields.get(i), i);
        }
        this.shardContexts = shardContexts;
        this.reuseColumnLoaders = reuseColumnLoaders;
        this.docChannel = docChannel;
        this.sourceReservationFactor = sourceReservationFactor;
    }

    @Override
    protected ReleasableIterator<Page> receive(Page page) {
        acquireSourceLoadingReservation();
        DocVector docVector = page.<DocBlock>getBlock(docChannel).asVector();
        return appendBlockArrays(page, valuesReader(docVector));
    }

    private ValuesReader valuesReader(DocVector docVector) {
        if (docVector.singleSegment()) {
            return new ValuesFromSingleReader(this, docVector);
        }
        if (bytesRefFieldCount() >= docSequenceBytesRefFieldThreshold) {
            return new ValuesFromDocSequence(this, docVector);
        }
        return new ValuesFromManyReader(this, docVector);
    }

    private int bytesRefFieldCount() {
        int count = 0;
        for (FieldWork field : fields) {
            if (field.info.type() == ElementType.BYTES_REF) {
                count++;
            }
        }
        return count;
    }

    /**
     * Acquires or increases the persistent source loading reservation on the circuit breaker.
     * Called at the start of each page and after each row load that updates
     * {@link #lastKnownSourceSize}. If the breaker trips, the exception propagates and
     * prevents further loading, limiting concurrent large _source operations.
     */
    void acquireSourceLoadingReservation() {
        long needed = (long) (lastKnownSourceSize * sourceReservationFactor);
        long additional = needed - sourceLoadingReservation;
        if (additional > 0) {
            driverContext.blockFactory().adjustBreaker(additional);
            sourceLoadingReservation = needed;
            if (log.isDebugEnabled()) {
                log.debug("reserve {}/{} bytes on circuit breaker for source loading", additional, sourceLoadingReservation);
            }
        }
    }

    /**
     * Updates the source loading reservation if the source bytes from the current document
     * exceed what we've seen before, then releases the parsed source to free memory.
     */
    void trackSourceBytesAndRelease(BlockLoaderStoredFieldsFromLeafLoader storedFields) {
        long sourceBytes = storedFields.lastSourceBytesSize();
        if (sourceBytes > lastKnownSourceSize) {
            lastKnownSourceSize = sourceBytes;
            acquireSourceLoadingReservation();
        }
        storedFields.releaseParsedSource();
    }

    void positionFieldWork(int shard, int segment, int firstDoc) {
        if (lastShard == shard) {
            if (lastSegment == segment) {
                for (FieldWork w : fields) {
                    w.sameSegment(firstDoc);
                }
                return;
            }
            lastSegment = segment;
            for (FieldWork w : fields) {
                w.sameShardNewSegment();
            }
            return;
        }
        lastShard = shard;
        lastSegment = segment;
        for (FieldWork w : fields) {
            w.newShard(shard);
        }
    }

    boolean positionFieldWorkDocGuaranteedAscending(int shard, int segment) {
        if (lastShard == shard) {
            if (lastSegment == segment) {
                return false;
            }
            lastSegment = segment;
            for (FieldWork w : fields) {
                w.sameShardNewSegment();
            }
            return true;
        }
        lastShard = shard;
        lastSegment = segment;
        for (FieldWork w : fields) {
            w.newShard(shard);
        }
        return true;
    }

    void trackStoredFields(StoredFieldsSpec spec, boolean sequential) {
        readersBuilt.merge(
            "stored_fields["
                + "requires_source:"
                + spec.requiresSource()
                + ", fields:"
                + spec.requiredStoredFields().size()
                + ", sequential: "
                + sequential
                + "]",
            1,
            (prev, one) -> prev + one
        );
    }

    @Override
    public void close() {
        if (sourceLoadingReservation > 0) {
            driverContext.blockFactory().adjustBreaker(-sourceLoadingReservation);
            sourceLoadingReservation = 0;
            if (log.isDebugEnabled()) {
                log.debug("release {} bytes from circuit breaker after source loading", sourceLoadingReservation);
            }
        }
        Releasables.close(super::close, converterEvaluators, Releasables.wrap(fields));
    }

    protected class FieldWork implements Releasable {
        final FieldInfo info;
        private final int fieldIdx;

        BlockLoader loader;
        // TODO rework this bit of mutable state into something harder to forget
        // Seriously, I've tripped over this twice.
        @Nullable
        ConverterEvaluator converter;
        @Nullable
        BlockLoader.ColumnAtATimeReader columnAtATime;
        @Nullable
        BlockLoader.RowStrideReader rowStride;

        FieldWork(FieldInfo info, int fieldIdx) {
            this.info = info;
            this.fieldIdx = fieldIdx;
        }

        void sameSegment(int firstDoc) {
            if (columnAtATime != null && columnAtATime.canReuse(firstDoc) == false) {
                // TODO count the number of times we can't reuse?
                columnAtATime.close();
                columnAtATime = null;
            }
            if (rowStride != null && rowStride.canReuse(firstDoc) == false) {
                rowStride.close();
                rowStride = null;
            }
        }

        void sameShardNewSegment() {
            closeReaders();
        }

        void newShard(int shard) {
            LoaderAndConverter l = info.buildLoader.build(driverContext.warningsMode(), shard);
            loader = l.loader;
            converter = l.converter == null ? null : converterEvaluators.get(shard, fieldIdx, info.name, l.converter);
            log.debug("moved to shard {} {} {}", shard, loader, converter);
            sameShardNewSegment();
        }

        BlockLoader.ColumnAtATimeReader columnAtATime(LeafReaderContext ctx) throws IOException {
            if (columnAtATime == null) {
                IOFunction<CircuitBreaker, BlockLoader.ColumnAtATimeReader> fn = loader.columnAtATimeReader(ctx);
                if (fn == null) {
                    trackReader("column_at_a_time", null);
                    return null;
                }
                if (reuseColumnLoaders) {
                    columnAtATime = fn.apply(driverContext.breaker());
                    trackReader("column_at_a_time", columnAtATime);
                } else {
                    columnAtATime = new ColumnAtATimeReaderWithoutReuse(
                        driverContext.breaker(),
                        fn,
                        r -> trackReader("column_at_a_time", r)
                    );
                }
            }
            return columnAtATime;
        }

        BlockLoader.RowStrideReader rowStride(LeafReaderContext ctx) throws IOException {
            if (rowStride == null) {
                rowStride = loader.rowStrideReader(driverContext.breaker(), ctx);
                trackReader("row_stride", this.rowStride);
            }
            return rowStride;
        }

        private void trackReader(String type, BlockLoader.Reader reader) {
            readersBuilt.merge(info.name + ":" + type + ":" + reader, 1, (prev, one) -> prev + one);
        }

        @Override
        public void close() {
            closeReaders();
        }

        private void closeReaders() {
            Releasables.close(columnAtATime, rowStride);
            columnAtATime = null;
            rowStride = null;
        }
    }

    LeafReaderContext ctx(int shard, int segment) {
        return shardContexts.get(shard).reader().leaves().get(segment);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("ValuesSourceReaderOperator[fields = [");
        if (fields.length < 10) {
            boolean first = true;
            for (FieldWork f : fields) {
                if (first) {
                    first = false;
                } else {
                    sb.append(", ");
                }
                sb.append(f.info.name);
            }
        } else {
            sb.append(fields.length).append(" fields");
        }
        return sb.append("]]").toString();
    }

    @Override
    protected ValuesSourceReaderOperatorStatus status(
        long processNanos,
        int pagesReceived,
        int pagesEmitted,
        long rowsReceived,
        long rowsEmitted
    ) {
        return new ValuesSourceReaderOperatorStatus(
            new TreeMap<>(readersBuilt),
            converterEvaluators.used(),
            processNanos,
            pagesReceived,
            pagesEmitted,
            rowsReceived,
            rowsEmitted,
            valuesLoaded
        );
    }

    /**
     * Quick checks for on the loaded block to make sure it looks reasonable.
     * @param loader the object that did the loading - we use it to make error messages if the block is busted
     * @param expectedPositions how many positions the block should have - it's as many as the incoming {@link Page} has
     * @param block the block to sanity check
     * @param field offset into the {@link #fields} array for the block being loaded
     */
    void sanityCheckBlock(Object loader, int expectedPositions, Block block, int field) {
        if (block.getPositionCount() != expectedPositions) {
            throw new IllegalStateException(
                sanityCheckBlockErrorPrefix(loader, block, field)
                    + " has ["
                    + block.getPositionCount()
                    + "] positions instead of ["
                    + expectedPositions
                    + "]"
            );
        }
        if (block.elementType() != ElementType.NULL && block.elementType() != fields[field].info.type) {
            throw new IllegalStateException(
                sanityCheckBlockErrorPrefix(loader, block, field)
                    + "'s element_type ["
                    + block.elementType()
                    + "] NOT IN (NULL, "
                    + fields[field].info.type
                    + ")"
            );
        }
    }

    private String sanityCheckBlockErrorPrefix(Object loader, Block block, int field) {
        return fields[field].info.name + "[" + loader + "]: " + block;
    }

    class ConverterEvaluators implements Releasable {
        private final Map<Key, ConverterAndCount> built = new HashMap<>();

        public ConverterEvaluator get(int shard, int fieldIdx, String field, ConverterFactory converter) {
            ConverterAndCount evaluator = built.computeIfAbsent(
                new Key(shard, fieldIdx, field),
                unused -> new ConverterAndCount(converter.build(driverContext))
            );
            evaluator.used++;
            return evaluator.evaluator;
        }

        public Map<String, Integer> used() {
            Map<String, Integer> used = new TreeMap<>();
            for (var e : built.entrySet()) {
                used.merge(e.getKey().field + ":" + e.getValue().evaluator, e.getValue().used, Integer::sum);
            }
            return used;
        }

        @Override
        public void close() {
            Releasables.close(built.values());
        }
    }

    record Key(int shard, int fieldIdx, String field) {}

    private static class ConverterAndCount implements Releasable {
        private final ConverterEvaluator evaluator;
        private int used;

        private ConverterAndCount(ConverterEvaluator evaluator) {
            this.evaluator = evaluator;
        }

        @Override
        public void close() {
            Releasables.close(evaluator);
        }
    }

    public static class LoaderAndConverter {
        private final BlockLoader loader;
        /**
         * An optional conversion function to apply after loading
         */
        @Nullable
        private final ConverterFactory converter;

        private LoaderAndConverter(BlockLoader loader, @Nullable ConverterFactory converter) {
            this.loader = loader;
            this.converter = converter;
        }

        public BlockLoader loader() {
            return loader;
        }
    }

    public interface ConverterFactory {
        ConverterEvaluator build(DriverContext context);
    }

    /**
     * Evaluator for any type conversions that must be performed on load. These are
     * built lazily on first need and kept until the {@link ValuesSourceReaderOperator}
     * is {@link #close closed}.
     */
    public interface ConverterEvaluator extends Releasable {
        Block convert(Block block);
    }
}
