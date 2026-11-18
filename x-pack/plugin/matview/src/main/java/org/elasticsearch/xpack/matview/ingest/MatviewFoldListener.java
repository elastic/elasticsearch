/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.matview.ingest;

import org.apache.lucene.document.DoubleField;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.Rounding;
import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.compute.aggregation.CountAggregatorFunction;
import org.elasticsearch.compute.aggregation.SumDoubleAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.blockhash.BlockHash;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.DoubleVector;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.HashAggregationOperator;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.IndexOperationBatch;
import org.elasticsearch.index.mapper.LuceneDocument;
import org.elasticsearch.index.shard.IndexingOperationListener;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Folds every batch a primary shard indexes into contributions for the view: {@code SUM(value), COUNT(value)} per
 * time bucket and group field values, computed with the compute engine over the batch's parsed documents and
 * written to the view index as {@code {@timestamp: bucket, group...: value, field: {sum, value_count}}} documents.
 * The live path opens no reader and sees no other batch; merges of the view fold the contributions later.
 * <p>
 * The batch path hands over a whole shard batch at once. The per-operation path, which ordinary indices use, is
 * called once per document, so its rows are buffered and folded either when {@link #FLUSH_ROWS} have accumulated or
 * {@link #FLUSH_DELAY} after the first row arrived; without that every document would become its own bulk to the
 * view, which floods the cluster with in-flight requests.
 * <p>
 * POC limits: the write is fire and forget (no cursor, so delivery is at most once here and at least once on retry
 * elsewhere), no origin stashing for security, and the aggregation is fixed to sum and count.
 */
public final class MatviewFoldListener implements IndexingOperationListener {
    private static final Logger logger = LogManager.getLogger(MatviewFoldListener.class);

    /**
     * @param target         view index receiving the contributions
     * @param timestampField date field of the source documents
     * @param groupFields    keyword fields of the source documents the view groups by, besides the time bucket
     * @param valueField     numeric field of the source documents to average
     * @param rounding       bucket rounding applied to the timestamp
     */
    public record Config(String target, String timestampField, List<String> groupFields, String valueField, Rounding.Prepared rounding) {}

    static final int FLUSH_ROWS = 4096;
    static final TimeValue FLUSH_DELAY = TimeValue.timeValueSeconds(1);

    /** One source document's contribution to the fold. */
    private record Row(long bucket, BytesRef[] groups, double value) {}

    private final Config config;
    private final Client client;
    private final ThreadPool threadPool;
    private final BlockFactory blockFactory;

    /** Rows from the per-operation path waiting to be folded; guarded by itself. */
    private final List<Row> pending = new ArrayList<>();
    private boolean flushScheduled; // guarded by pending
    private boolean closed; // guarded by pending

    public MatviewFoldListener(Config config, Client client, ThreadPool threadPool, BlockFactory blockFactory) {
        this.config = config;
        this.client = client;
        this.threadPool = threadPool;
        this.blockFactory = blockFactory;
    }

    @Override
    public void postIndexBatch(ShardId shardId, IndexOperationBatch batch, List<Engine.IndexResult> results) {
        if (batch.origin() != Engine.Operation.Origin.PRIMARY) {
            return; // replicas index the same documents again; only the primary contributes
        }
        List<Engine.Index> ops = batch.materializeIndexOps();
        List<Row> rows = new ArrayList<>(ops.size());
        for (int i = 0; i < ops.size(); i++) {
            if (results.get(i).getResultType() == Engine.Result.Type.SUCCESS) {
                addRow(rows, ops.get(i));
            }
        }
        fold(rows);
    }

    @Override
    public void postIndex(ShardId shardId, Engine.Index index, Engine.IndexResult result) {
        if (index.origin() != Engine.Operation.Origin.PRIMARY || result.getResultType() != Engine.Result.Type.SUCCESS) {
            return;
        }
        List<Row> rows = null;
        synchronized (pending) {
            if (closed) {
                return;
            }
            addRow(pending, index);
            if (pending.size() >= FLUSH_ROWS) {
                rows = List.copyOf(pending);
                pending.clear();
            } else if (flushScheduled == false && pending.isEmpty() == false) {
                flushScheduled = true;
                threadPool.schedule(this::flushPending, FLUSH_DELAY, threadPool.generic());
            }
        }
        if (rows != null) {
            fold(rows);
        }
    }

    private void flushPending() {
        List<Row> rows;
        synchronized (pending) {
            flushScheduled = false;
            rows = List.copyOf(pending);
            pending.clear();
        }
        fold(rows);
    }

    /**
     * Stops contributing: rows still pending are dropped, so a view index that is recreated under the same name
     * never receives rows of a source index that no longer exists. (With a cursor these rows would be re-read.)
     */
    public void close() {
        synchronized (pending) {
            closed = true;
            pending.clear();
        }
    }

    /** Reads the fields straight off the parsed Lucene document; no source parsing or mapper lookup needed. */
    private void addRow(List<Row> rows, Engine.Index index) {
        LuceneDocument doc = index.parsedDoc().rootDoc();
        Number timestamp = doc.getNumericValue(config.timestampField());
        Double value = doubleValue(doc, config.valueField());
        if (timestamp == null || value == null) {
            return;
        }
        BytesRef[] groups = new BytesRef[config.groupFields().size()];
        for (int g = 0; g < groups.length; g++) {
            groups[g] = keywordValue(doc, config.groupFields().get(g));
            if (groups[g] == null) {
                return; // POC: documents without a group value are left out instead of forming a null group
            }
        }
        rows.add(new Row(config.rounding().round(timestamp.longValue()), groups, value));
    }

    /**
     * The double the mapper indexed. The number mapper writes a {@link DoubleField} (points and doc values) or a bare
     * doc values field, both of which report the sortable long encoding from {@code numericValue()}, a
     * {@link DoublePoint} when there are only points, and a {@link StoredField} when stored.
     */
    private static Double doubleValue(LuceneDocument doc, String name) {
        for (IndexableField field : doc.getFields()) {
            if (field.name().equals(name) == false || field.numericValue() == null) {
                continue;
            }
            if (field instanceof DoubleField || field.fieldType().docValuesType() == DocValuesType.SORTED_NUMERIC) {
                return NumericUtils.sortableLongToDouble(field.numericValue().longValue());
            }
            if (field instanceof DoublePoint || field instanceof StoredField) {
                return field.numericValue().doubleValue();
            }
        }
        return null;
    }

    /** The keyword the mapper indexed: its binary term, or the string of a plain string field. */
    private static BytesRef keywordValue(LuceneDocument doc, String name) {
        for (IndexableField field : doc.getFields()) {
            if (field.name().equals(name) == false) {
                continue;
            }
            if (field.binaryValue() != null) {
                return BytesRef.deepCopyOf(field.binaryValue());
            }
            if (field.stringValue() != null) {
                return new BytesRef(field.stringValue());
            }
        }
        return null;
    }

    private void fold(List<Row> rows) {
        if (rows.isEmpty()) {
            return;
        }
        // off the write thread: the aggregation and the bulk to the view are not part of the source's write latency
        threadPool.generic().execute(() -> {
            try {
                BulkRequest bulk = contributions(rows);
                client.bulk(bulk, ActionListener.wrap(response -> {
                    if (response.hasFailures()) {
                        logger.warn("failed to write view contributions to [{}]: {}", config.target(), response.buildFailureMessage());
                    }
                }, e -> logger.warn(() -> "failed to write view contributions to [" + config.target() + "]", e)));
            } catch (Exception e) {
                logger.warn(() -> "failed to fold batch for view [" + config.target() + "]", e);
            }
        });
    }

    /** {@code SUM(value), COUNT(value) BY bucket, groups...} over the batch, in a single compute operator run. */
    private BulkRequest contributions(List<Row> rows) {
        DriverContext driverContext = new DriverContext(blockFactory.bigArrays(), blockFactory, null);
        int numGroups = config.groupFields().size();
        List<BlockHash.GroupSpec> groups = new ArrayList<>();
        groups.add(new BlockHash.GroupSpec(0, ElementType.LONG));
        for (int g = 0; g < numGroups; g++) {
            groups.add(new BlockHash.GroupSpec(g + 1, ElementType.BYTES_REF));
        }
        List<Integer> valueChannel = List.of(numGroups + 1);
        Page input = inputPage(rows);
        BulkRequest bulk = new BulkRequest();
        BytesRef scratch = new BytesRef();
        try (
            Operator aggregate = new HashAggregationOperator.Builder().groups(groups)
                .mode(AggregatorMode.SINGLE)
                .aggregators(
                    List.of(
                        new SumDoubleAggregatorFunctionSupplier().groupingAggregatorFactory(AggregatorMode.SINGLE, valueChannel),
                        CountAggregatorFunction.supplier().groupingAggregatorFactory(AggregatorMode.SINGLE, valueChannel)
                    )
                )
                .build()
                .get(driverContext)
        ) {
            aggregate.addInput(input);
            aggregate.finish();
            for (Page out = aggregate.getOutput(); out != null; out = aggregate.getOutput()) {
                try {
                    LongBlock buckets = out.getBlock(0);
                    DoubleBlock sums = out.getBlock(numGroups + 1);
                    LongBlock counts = out.getBlock(numGroups + 2);
                    for (int p = 0; p < out.getPositionCount(); p++) {
                        Map<String, Object> source = new HashMap<>();
                        source.put(config.timestampField(), buckets.getLong(p));
                        for (int g = 0; g < numGroups; g++) {
                            BytesRefBlock group = out.getBlock(g + 1);
                            source.put(config.groupFields().get(g), group.getBytesRef(p, scratch).utf8ToString());
                        }
                        source.put(config.valueField(), Map.of("sum", sums.getDouble(p), "value_count", counts.getLong(p)));
                        bulk.add(new IndexRequest(config.target()).source(source));
                    }
                } finally {
                    out.releaseBlocks();
                }
            }
        }
        return bulk;
    }

    private Page inputPage(List<Row> rows) {
        int numGroups = config.groupFields().size();
        Block[] blocks = new Block[numGroups + 2];
        List<Releasable> builders = new ArrayList<>();
        boolean success = false;
        try {
            LongVector.Builder buckets = blockFactory.newLongVectorBuilder(rows.size());
            builders.add(buckets);
            BytesRefVector.Builder[] groups = new BytesRefVector.Builder[numGroups];
            for (int g = 0; g < numGroups; g++) {
                groups[g] = blockFactory.newBytesRefVectorBuilder(rows.size());
                builders.add(groups[g]);
            }
            DoubleVector.FixedBuilder values = blockFactory.newDoubleVectorFixedBuilder(rows.size());
            builders.add(values);
            for (Row row : rows) {
                buckets.appendLong(row.bucket());
                for (int g = 0; g < numGroups; g++) {
                    groups[g].appendBytesRef(row.groups()[g]);
                }
                values.appendDouble(row.value());
            }
            blocks[0] = buckets.build().asBlock();
            for (int g = 0; g < numGroups; g++) {
                blocks[g + 1] = groups[g].build().asBlock();
            }
            blocks[numGroups + 1] = values.build().asBlock();
            Page page = new Page(blocks);
            success = true;
            return page;
        } finally {
            if (success == false) {
                Releasables.close(Releasables.wrap(builders), Releasables.wrap(blocks));
            }
        }
    }
}
