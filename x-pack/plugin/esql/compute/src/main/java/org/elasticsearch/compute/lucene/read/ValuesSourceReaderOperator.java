/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene.read;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.IOSupplier;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.DocBlock;
import org.elasticsearch.compute.data.DocVector;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.IndexedByShardId;
import org.elasticsearch.compute.lucene.LuceneSourceOperator;
import org.elasticsearch.compute.operator.AbstractPageMappingToIteratorOperator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.mapper.BlockLoader;
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
import java.util.function.IntFunction;

/**
 * Operator that extracts doc_values from a Lucene index out of pages that have been produced by {@link LuceneSourceOperator}
 * and outputs them to a new column.
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
        int docChannel
    ) implements OperatorFactory {
        public Factory

        {
            if (fields.isEmpty()) {
                throw new IllegalStateException("ValuesSourceReaderOperator doesn't support empty fields");
            }
        }

        @Override
        public Operator get(DriverContext driverContext) {
            return new ValuesSourceReaderOperator(
                driverContext,
                jumboSize.getBytes(),
                fields,
                shardContexts,
                reuseColumnLoaders,
                docChannel
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
     * @param loaderAndConverter   maps shard index to the {@link BlockLoader} which load the actual blocks and an
     *                             optional type converter.
     */
    public record FieldInfo(String name, ElementType type, boolean nullsFiltered, IntFunction<LoaderAndConverter> loaderAndConverter) {}

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
    final FieldWork[] fields;
    final IndexedByShardId<? extends ShardContext> shardContexts;
    private final boolean reuseColumnLoaders;
    private final int docChannel;

    private final Map<String, Integer> readersBuilt = new TreeMap<>();
    long valuesLoaded;

    private int lastShard = -1;
    private int lastSegment = -1;

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
        int docChannel
    ) {
        if (fields.isEmpty()) {
            throw new IllegalStateException("ValuesSourceReaderOperator doesn't support empty fields");
        }
        this.driverContext = driverContext;
        this.jumboBytes = jumboBytes;
        this.fields = new FieldWork[fields.size()];
        for (int i = 0; i < this.fields.length; i++) {
            this.fields[i] = new FieldWork(fields.get(i), i);
        }
        this.shardContexts = shardContexts;
        this.reuseColumnLoaders = reuseColumnLoaders;
        this.docChannel = docChannel;
    }

    @Override
    protected ReleasableIterator<Page> receive(Page page) {
        DocVector docVector = page.<DocBlock>getBlock(docChannel).asVector();
        return appendBlockArrays(
            page,
            docVector.singleSegment() ? new ValuesFromSingleReader(this, docVector) : new ValuesFromManyReader(this, docVector)
        );
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
        Releasables.close(super::close, converterEvaluators);
    }

    protected class FieldWork {
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
                columnAtATime = null;
            }
            if (rowStride != null && rowStride.canReuse(firstDoc) == false) {
                rowStride = null;
            }
        }

        void sameShardNewSegment() {
            columnAtATime = null;
            rowStride = null;
        }

        void newShard(int shard) {
            LoaderAndConverter l = info.loaderAndConverter.apply(shard);
            loader = l.loader;
            converter = l.converter == null ? null : converterEvaluators.get(shard, fieldIdx, info.name, l.converter);
            log.debug("moved to shard {} {} {}", shard, loader, converter);
            columnAtATime = null;
            rowStride = null;
        }

        BlockLoader.ColumnAtATimeReader columnAtATime(LeafReaderContext ctx) throws IOException {
            if (columnAtATime == null) {
                IOSupplier<BlockLoader.ColumnAtATimeReader> supplier = loader.columnAtATimeReader(ctx);
                if (supplier == null) {
                    trackReader("column_at_a_time", null);
                    return null;
                }
                if (reuseColumnLoaders) {
                    columnAtATime = supplier.get();
                    trackReader("column_at_a_time", columnAtATime);
                } else {
                    columnAtATime = new ColumnAtATimeReaderWithoutReuse(supplier, r -> trackReader("column_at_a_time", r));
                }
            }
            return columnAtATime;
        }

        BlockLoader.RowStrideReader rowStride(LeafReaderContext ctx) throws IOException {
            if (rowStride == null) {
                rowStride = loader.rowStrideReader(ctx);
                trackReader("row_stride", this.rowStride);
            }
            return rowStride;
        }

        private void trackReader(String type, BlockLoader.Reader reader) {
            readersBuilt.merge(info.name + ":" + type + ":" + reader, 1, (prev, one) -> prev + one);
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
