/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.matview.fold.compute;

import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.compute.aggregation.GroupingAggregator;
import org.elasticsearch.compute.aggregation.SumDoubleAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.SumIntAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.SumLongAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.blockhash.BlockHash;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.IndexedByShardIdFromSingleton;
import org.elasticsearch.compute.lucene.read.ValuesSourceReaderOperator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.HashAggregationOperator;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.WarningSourceLocation;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.SourceLoader;
import org.elasticsearch.index.mapper.blockloader.docvalues.BytesRefsFromOrdsBlockLoader;
import org.elasticsearch.index.mapper.blockloader.docvalues.DoublesBlockLoader;
import org.elasticsearch.index.mapper.blockloader.docvalues.IntsBlockLoader;
import org.elasticsearch.index.mapper.blockloader.docvalues.LongsBlockLoader;
import org.elasticsearch.xpack.matview.fold.FoldSpec;
import org.elasticsearch.xpack.matview.fold.KeyEncoder;
import org.elasticsearch.xpack.matview.fold.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Reduces the live documents of a merge with the ES|QL compute engine: a tiny source operator walks the leaves,
 * {@link ValuesSourceReaderOperator} loads the key and the reduced fields through the ordinary doc values block
 * loaders, and a {@link HashAggregationOperator} groups by the key fields. Because a stored sum folds by summing,
 * the reduction of {@code sum} and {@code count} style state is the plain {@link AggregatorMode#SINGLE} aggregator
 * over the stored parts; aggregates that keep opaque state (digests, sketches) would run the same chain in
 * {@link AggregatorMode#INTERMEDIATE} instead.
 * <p>
 * The operators are pumped by hand on the calling (merge) thread; no driver or executor is involved.
 */
public final class ComputeReducer implements Reducer {
    private static final WarningSourceLocation NO_WARNINGS = new WarningSourceLocation() {
        @Override
        public int lineNumber() {
            return -1;
        }

        @Override
        public int columnNumber() {
            return -1;
        }

        @Override
        public String viewName() {
            return null;
        }

        @Override
        public String text() {
            return "";
        }
    };

    private final BlockFactory blockFactory;
    private final FoldSpec spec;
    private final int maxPageSize;

    public ComputeReducer(BlockFactory blockFactory, FoldSpec spec, int maxPageSize) {
        this.blockFactory = blockFactory;
        this.spec = spec;
        this.maxPageSize = maxPageSize;
    }

    @Override
    public Result reduce(List<? extends CodecReader> readers, KeyIndex keys) throws IOException {
        if (readers.isEmpty()) {
            return new ArrayResult(spec, List.of(), keys);
        }
        // MultiReader only to hand the leaves to the values reader; it must not close the merge's readers.
        try (MultiReader multi = new MultiReader(readers.toArray(IndexReader[]::new), false)) {
            DriverContext driverContext = new DriverContext(blockFactory.bigArrays(), blockFactory, null);
            List<Page> output = new ArrayList<>();
            try (
                SegmentDocSourceOperator source = new SegmentDocSourceOperator(blockFactory, multi.leaves(), maxPageSize);
                Operator values = valuesReader(multi).get(driverContext);
                Operator aggregate = aggregator().get(driverContext)
            ) {
                while (source.isFinished() == false) {
                    Page page = source.getOutput();
                    if (page == null) {
                        continue;
                    }
                    values.addInput(page);
                    drain(values, aggregate);
                }
                values.finish();
                drain(values, aggregate);
                aggregate.finish();
                for (Page page = aggregate.getOutput(); page != null; page = aggregate.getOutput()) {
                    output.add(page);
                }
                return new ArrayResult(spec, output, keys);
            } finally {
                Releasables.close(() -> output.forEach(Page::releaseBlocks));
            }
        }
    }

    private static void drain(Operator from, Operator to) {
        for (Page page = from.getOutput(); page != null; page = from.getOutput()) {
            to.addInput(page);
        }
    }

    /** Loads the key fields and then the reduced fields; channel 0 is the doc block. */
    private ValuesSourceReaderOperator.Factory valuesReader(IndexReader reader) {
        List<ValuesSourceReaderOperator.FieldInfo> fields = new ArrayList<>();
        for (FoldSpec.KeyField k : spec.keys()) {
            fields.add(field(k.name(), k.type(), switch (k.type()) {
                case LONG -> new LongsBlockLoader(k.name());
                case BYTES_REF -> new BytesRefsFromOrdsBlockLoader(k.name(), ByteSizeValue.ZERO);
                default -> throw new IllegalArgumentException("unsupported key type [" + k.type() + "]");
            }));
        }
        for (FoldSpec.ReducedField f : spec.fields()) {
            fields.add(field(f.name(), f.type(), switch (f.type()) {
                case DOUBLE -> new DoublesBlockLoader(f.name(), NumericUtils::sortableLongToDouble);
                case INT -> new IntsBlockLoader(f.name());
                case LONG -> new LongsBlockLoader(f.name());
                default -> throw new IllegalArgumentException("unsupported type [" + f.type() + "]");
            }));
        }
        var shardContext = new ValuesSourceReaderOperator.ShardContext(reader, ignored -> SourceLoader.FROM_STORED_SOURCE, 0.0);
        return new ValuesSourceReaderOperator.Factory(
            ByteSizeValue.ofMb(1),
            fields,
            new IndexedByShardIdFromSingleton<>(shardContext),
            true,
            0,
            0.0,
            () -> 0L
        );
    }

    private static ValuesSourceReaderOperator.FieldInfo field(String name, ElementType type, BlockLoader loader) {
        return new ValuesSourceReaderOperator.FieldInfo(
            name,
            type,
            false,
            (driverContext, shard) -> ValuesSourceReaderOperator.load(loader)
        );
    }

    private HashAggregationOperator.Factory aggregator() {
        int numKeys = spec.keys().size();
        List<BlockHash.GroupSpec> groups = new ArrayList<>();
        for (int k = 0; k < numKeys; k++) {
            groups.add(new BlockHash.GroupSpec(k + 1, spec.keys().get(k).type()));
        }
        List<GroupingAggregator.Factory> aggregators = new ArrayList<>();
        for (int i = 0; i < spec.fields().size(); i++) {
            FoldSpec.ReducedField f = spec.fields().get(i);
            List<Integer> channel = List.of(numKeys + 1 + i);
            aggregators.add(switch (f.reduction()) {
                case SUM -> switch (f.type()) {
                    case DOUBLE -> new SumDoubleAggregatorFunctionSupplier().groupingAggregatorFactory(AggregatorMode.SINGLE, channel);
                    case INT -> new SumIntAggregatorFunctionSupplier().groupingAggregatorFactory(AggregatorMode.SINGLE, channel);
                    case LONG -> new SumLongAggregatorFunctionSupplier(NO_WARNINGS).groupingAggregatorFactory(
                        AggregatorMode.SINGLE,
                        channel
                    );
                    default -> throw new IllegalArgumentException("unsupported type [" + f.type() + "]");
                };
            });
        }
        return new HashAggregationOperator.Builder().groups(groups).mode(AggregatorMode.SINGLE).aggregators(aggregators).build();
    }

    /**
     * Reduced values copied out of the aggregation output into flat arrays, indexed by the key ids the fold scan
     * assigned: every output row's key is encoded the same way and looked up in the {@link KeyIndex}. Copying lets
     * the pages be released right away and keeps the merge-time lookups cheap.
     */
    static final class ArrayResult implements Result {
        private final int[] rowByKeyId;
        private final long[][] values;
        private final FixedBitSet[] hasValue;

        ArrayResult(FoldSpec spec, List<Page> pages, KeyIndex keys) {
            int total = pages.stream().mapToInt(Page::getPositionCount).sum();
            int numKeys = spec.keys().size();
            int fields = spec.fields().size();
            this.rowByKeyId = new int[Math.toIntExact(keys.size())];
            Arrays.fill(rowByKeyId, -1);
            this.values = new long[fields][total];
            this.hasValue = new FixedBitSet[fields];
            for (int f = 0; f < fields; f++) {
                hasValue[f] = new FixedBitSet(total);
            }
            KeyEncoder encoder = new KeyEncoder();
            BytesRef scratch = new BytesRef();
            int row = 0;
            for (Page page : pages) {
                for (int p = 0; p < page.getPositionCount(); p++, row++) {
                    encoder.reset();
                    boolean complete = true;
                    for (int k = 0; k < numKeys && complete; k++) {
                        Block key = page.getBlock(k);
                        if (key.isNull(p)) {
                            complete = false; // a group with a missing key component has no representative
                        } else if (key instanceof LongBlock longs) {
                            encoder.addLong(longs.getLong(longs.getFirstValueIndex(p)));
                        } else {
                            BytesRefBlock bytes = (BytesRefBlock) key;
                            encoder.addBytes(bytes.getBytesRef(bytes.getFirstValueIndex(p), scratch));
                        }
                    }
                    if (complete) {
                        long keyId = keys.find(encoder.get());
                        assert keyId >= 0 : "aggregation produced a group the fold scan never registered";
                        if (keyId >= 0) {
                            rowByKeyId[Math.toIntExact(keyId)] = row;
                        }
                    }
                    for (int f = 0; f < fields; f++) {
                        Block block = page.getBlock(numKeys + f);
                        if (block.isNull(p)) {
                            continue;
                        }
                        hasValue[f].set(row);
                        values[f][row] = switch (spec.fields().get(f).type()) {
                            case DOUBLE -> NumericUtils.doubleToSortableLong(((DoubleBlock) block).getDouble(p));
                            case INT, LONG -> ((LongBlock) block).getLong(p);
                            default -> throw new IllegalArgumentException("unsupported type");
                        };
                    }
                }
            }
        }

        @Override
        public int rowOf(int keyId) {
            return rowByKeyId[keyId];
        }

        @Override
        public boolean hasValue(int field, int row) {
            return hasValue[field].get(row);
        }

        @Override
        public long value(int field, int row) {
            return values[field][row];
        }

        @Override
        public void close() {}
    }
}
