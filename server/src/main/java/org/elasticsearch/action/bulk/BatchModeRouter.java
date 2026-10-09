/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.bulk;

import org.apache.lucene.document.column.LongTupleCursor;
import org.apache.lucene.document.column.ObjectTupleCursor;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.routing.IndexRouting;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.escf.EscfBatch;
import org.elasticsearch.escf.EscfBatchScatterer;
import org.elasticsearch.escf.EscfColumn;
import org.elasticsearch.escf.EscfColumnKind;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.sourcebatch.SourceBatch;
import org.elasticsearch.sourcebatch.SourceSchema;
import org.elasticsearch.sourcebatch.SourceValueType;
import org.elasticsearch.transport.BytesRefRecycler;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

/**
 * Per-abstraction router: decides each item's destination shard and builds the per-shard
 * {@link SourceBatch} by scattering its pre-built {@link EscfBatch} across concrete backing indices.
 *
 * <p>One {@code BatchModeRouter} handles exactly one index-abstraction write target (a data stream,
 * a plain index, or an alias). A single batch may still fan out to multiple concrete backing indices
 * (e.g. a TSDB data stream whose rows straddle a rollover boundary). The router tracks up to N
 * {@link IndexTarget}s with a linear scan — typical TSDB batches span at most 2 generations — and
 * uses globally numbered partitions for the scatter step:
 *
 * <pre>
 *   index1: shards 0,1 → partitions 0,1
 *   index2: shards 0,1,2 → partitions 2,3,4   (totalPartitions = 5)
 * </pre>
 *
 * <p>Items that are dropped during the grouping loop (closed index, validation failure, etc.)
 * leave their corresponding rows unclaimed. At scatter time those orphan rows are routed to a
 * discard partition that is immediately released and never mapped to a {@link ShardId}.
 *
 * <p>Use {@link BatchRouterSet} to coordinate multiple {@code BatchModeRouter} instances when a
 * bulk carries batches for more than one index abstraction.
 */
final class BatchModeRouter {

    /**
     * One concrete backing index plus its routing and its global partition base.
     * {@code partitionBase + localShardId} is the global partition id for a row going to this target.
     */
    private record IndexTarget(Index index, IndexRouting routing, int shardCount, int partitionBase) {}

    /** Key returned by {@link #batchKey} for this router's abstraction. */
    final String key;
    private final EscfBatch source;

    /**
     * Global partition id for each row. Initialized to {@code -1} (unclaimed / discard).
     * {@link #buildGrouping} overwrites entries for successfully routed rows with their real partition
     * ids; any remaining {@code -1} entries identify orphan rows that go to the discard partition.
     */
    private final int[] partitionIds;

    /** The {@link BulkItemRequest} for each row, in row-index order. Null for orphan rows. */
    private final BulkItemRequest[] items;

    /**
     * Resolved concrete targets, grown with {@link Arrays#copyOf} as new ones are encountered.
     * Valid range is {@code [0, targetCount)}.
     */
    @Nullable
    private IndexTarget[] targets;
    private int targetCount;

    /** Sum of all {@link IndexTarget#shardCount()} values; also the next available partitionBase. */
    private int totalPartitions;

    /**
     * Target index (into {@link #targets}) for each row. Allocated lazily on the transition from 1 to
     * 2 targets; stays null while {@code targetCount <= 1}. Zero-fill is correct: every row recorded
     * before the second target belongs to target 0.
     */
    @Nullable
    private int[] rowTargets;

    private int lastRow = -1;
    private int routedCount;
    private boolean scattered;
    private boolean groupingBuilt;

    BatchModeRouter(String key, EscfBatch source) {
        this.key = key;
        this.source = source;
        int n = source.docCount();
        this.partitionIds = new int[n];
        Arrays.fill(this.partitionIds, -1);
        this.items = new BulkItemRequest[n];
    }

    /**
     * Computes the batch key for an index abstraction — the string under which its documents are
     * accumulated. Chosen so that two distinct keys always resolve to distinct concrete write indices,
     * preventing per-shard batch collisions after scatter.
     *
     * <ul>
     *   <li>DATA_STREAM or an alias over one: the data stream's canonical name.</li>
     *   <li>CONCRETE_INDEX (plain): the index's own name (which is its own write index).</li>
     *   <li>ALIAS (non-data-stream): the alias's write index's name.</li>
     *   <li>CONCRETE_INDEX that is a backing index of a data stream: {@code null} — such items
     *       cannot be batched without a batch-merge utility; the whole bulk falls back to the row
     *       path.</li>
     *   <li>ALIAS with no write index: {@code null}.</li>
     * </ul>
     */
    @Nullable
    static String batchKey(IndexAbstraction ia, ProjectMetadata project) {
        if (ia == null) {
            return null;
        }
        if (ia.getType() == IndexAbstraction.Type.CONCRETE_INDEX && ia.getParentDataStream() != null) {
            // Direct write to a backing index. Its rows would belong in the parent data stream's batch,
            // but the batch's items were resolved through a different abstraction. Rare (requires OCC
            // terms or sequence numbers disabled) and not worth a batch merge — take the whole bulk
            // down the row path.
            return null;
        }
        DataStream ds = DataStream.resolveDataStream(ia, project);
        if (ds != null) {
            return ds.getName(); // DATA_STREAM, or an ALIAS that points to one
        }
        Index writeIndex = ia.getWriteIndex();
        return writeIndex == null ? null : writeIndex.getName(); // CONCRETE_INDEX, or a plain ALIAS
    }

    /**
     * Resolves {@code @timestamp} from the ESCF columns and caches it on each item's
     * {@link IndexRequest} via {@link IndexRequest#setTimeSeriesTimestamp}. Must be called before the
     * per-item routing loop (before {@link #route}) so that
     * {@link DataStream#getWriteIndex(IndexRequest, ProjectMetadata)} finds the memoized value and can
     * select the correct backing index.
     *
     * <p>No-op for non-TSDB data streams. All-or-none: if every request already has a timestamp set
     * (producer supplied it out-of-band), returns immediately. A mixed batch (some set, some not)
     * throws.
     *
     * @param requests the bulk request's items; only those whose batch key matches this router are
     *                 considered, so this method is safe to call from a coordinator that passes the
     *                 full request list
     * @throws IllegalArgumentException if {@code @timestamp} is missing, an unsupported kind,
     *                                  or a row has no value for it
     */
    void preResolveTimestamps(ProjectMetadata project, List<DocWriteRequest<?>> requests) {
        // Only TSDB data streams need per-document backing-index selection.
        IndexAbstraction ia = project.getIndicesLookup().get(key);
        if (ia == null) {
            return;
        }
        DataStream dataStream = DataStream.resolveDataStream(ia, project);
        if (dataStream == null || IndexMode.isTsdb(dataStream.getIndexMode()) == false) {
            return;
        }

        // Build a row→IndexRequest map so we can set timestamps by row index. Filter to items that
        // belong to this router (multi-router bulks may have overlapping row-index ranges).
        IndexRequest[] byRow = new IndexRequest[source.docCount()];
        boolean anyHasTimestamp = false;
        boolean anyLacksTimestamp = false;
        for (DocWriteRequest<?> req : requests) {
            if (req instanceof IndexRequest ir && ir.indexSource().hasSourceRow()) {
                IndexAbstraction reqIa = project.getIndicesLookup().get(ir.index());
                String reqKey = batchKey(reqIa, project);
                if (key.equals(reqKey) == false) {
                    continue;
                }
                int rowIndex = ir.indexSource().rowIndex();
                if (rowIndex >= 0 && rowIndex < byRow.length) {
                    byRow[rowIndex] = ir;
                    if (ir.getTimeSeriesTimestamp() != null) {
                        anyHasTimestamp = true;
                    } else {
                        anyLacksTimestamp = true;
                    }
                }
            }
        }

        if (anyHasTimestamp && anyLacksTimestamp) {
            throw new IllegalArgumentException(
                "pre-built batch for ["
                    + key
                    + "] has a mix of requests with and without a pre-set timestamp;"
                    + " either all requests must supply a timestamp (producer-side) or none"
            );
        }
        if (anyHasTimestamp) {
            // All timestamps pre-supplied by the producer — nothing to resolve.
            return;
        }

        // Find the @timestamp column index.
        SourceSchema schema = source.schema();
        int leaf = schema.findLeaf(DataStream.TIMESTAMP_FIELD_NAME, 0);
        if (leaf < 0) {
            throw new IllegalArgumentException(
                "pre-built batch for ["
                    + key
                    + "] targets a TSDB data stream but the batch has no ["
                    + DataStream.TIMESTAMP_FIELD_NAME
                    + "] column; the batch producer must include a timestamp column"
            );
        }

        EscfColumn col = source.column(leaf);
        byte kind = col.kind();

        switch (kind) {
            case EscfColumnKind.LONG -> resolveTimestampsFromLong(col, byRow);
            case EscfColumnKind.STRING -> resolveTimestampsFromString(col, byRow);
            case EscfColumnKind.UNION -> resolveTimestampsFromUnion(col, byRow);
            default -> throw new UnsupportedOperationException(
                "pre-built batch for ["
                    + key
                    + "] has a ["
                    + DataStream.TIMESTAMP_FIELD_NAME
                    + "] column of unsupported kind ["
                    + EscfColumnKind.name(kind)
                    + "]; the batch producer must encode the timestamp as a LONG (epoch millis), STRING (ISO-8601), or UNION column"
            );
        }
    }

    /**
     * Resolves {@code @timestamp} from a LONG (epoch-millis) column and caches on each row's
     * {@link IndexRequest}. An absent row throws because TSDB requires every document to have a
     * timestamp.
     */
    private void resolveTimestampsFromLong(EscfColumn col, IndexRequest[] byRow) {
        int docCount = source.docCount();
        boolean[] seen = new boolean[docCount];
        LongTupleCursor cursor = col.longCursor();
        int r;
        while ((r = cursor.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
            setTimestampOnRequest(byRow, r, Instant.ofEpochMilli(cursor.longValue()));
            seen[r] = true;
        }
        checkAllRowsHaveTimestamp(seen, byRow);
    }

    /**
     * Resolves {@code @timestamp} from a STRING (ISO-8601 or epoch-millis) column and caches on each
     * row's {@link IndexRequest}.
     */
    private void resolveTimestampsFromString(EscfColumn col, IndexRequest[] byRow) {
        int docCount = source.docCount();
        boolean[] seen = new boolean[docCount];
        ObjectTupleCursor<BytesRef> cursor = col.bytesRefCursor(false);
        int r;
        while ((r = cursor.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
            BytesRef bytes = cursor.value();
            String text = bytes.utf8ToString();
            Instant ts = DataStream.getTimestampFromRawValue(text);
            setTimestampOnRequest(byRow, r, ts);
            seen[r] = true;
        }
        checkAllRowsHaveTimestamp(seen, byRow);
    }

    /**
     * Resolves {@code @timestamp} from a UNION column (dispatching on the per-row type byte) and
     * caches on each row's {@link IndexRequest}.
     */
    private void resolveTimestampsFromUnion(EscfColumn col, IndexRequest[] byRow) {
        int docCount = source.docCount();
        boolean[] seen = new boolean[docCount];
        ObjectTupleCursor<BytesRef> cursor = col.bytesRefCursor(false);
        int r;
        while ((r = cursor.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
            byte typeByte = col.getTypeByte(r);
            Instant ts;
            if (typeByte == SourceValueType.LONG || typeByte == SourceValueType.INT) {
                ts = DataStream.getTimestampFromRawValue(col.getLongValue(r));
            } else if (typeByte == SourceValueType.STRING) {
                BytesRef bytes = cursor.value();
                ts = DataStream.getTimestampFromRawValue(bytes.utf8ToString());
            } else {
                throw new UnsupportedOperationException(
                    "pre-built batch for ["
                        + key
                        + "] row "
                        + r
                        + " has a UNION ["
                        + DataStream.TIMESTAMP_FIELD_NAME
                        + "] value of unsupported type ["
                        + SourceValueType.name(typeByte)
                        + "]; expected LONG or STRING"
                );
            }
            setTimestampOnRequest(byRow, r, ts);
            seen[r] = true;
        }
        checkAllRowsHaveTimestamp(seen, byRow);
    }

    private void checkAllRowsHaveTimestamp(boolean[] seen, IndexRequest[] byRow) {
        for (int row = 0; row < seen.length; row++) {
            if (seen[row] == false && byRow[row] != null) {
                throw new IllegalArgumentException(
                    "pre-built batch for ["
                        + key
                        + "] row "
                        + row
                        + " has no ["
                        + DataStream.TIMESTAMP_FIELD_NAME
                        + "] value; every TSDB document must have a timestamp"
                );
            }
        }
    }

    private void setTimestampOnRequest(IndexRequest[] byRow, int row, Instant rawTimestamp) {
        Instant ts = DataStream.getCanonicalTimestampBound(rawTimestamp);
        IndexRequest ir = byRow[row];
        if (ir != null) {
            ir.setTimeSeriesTimestamp(ts);
        }
    }

    /**
     * Records one item for deferred routing. The actual shard assignment is deferred until
     * {@link #buildGrouping}.
     */
    void route(BulkItemRequest bulkItem, IndexRequest request, Index concreteIndex, IndexRouting routing, ProjectMetadata project) {
        int targetIdx = prepareRouting(request, concreteIndex, routing, project);
        recordDeferredItem(bulkItem, request.indexSource().rowIndex(), targetIdx);
    }

    /**
     * Validates the request, resolves (or registers) the concrete-index target, and returns the target
     * index into {@link #targets}.
     */
    private int prepareRouting(IndexRequest request, Index concreteIndex, IndexRouting routing, ProjectMetadata project) {
        // Linear scan over targets (typically 1–2 entries at a TSDB rollover boundary; no Map needed).
        for (int i = 0; i < targetCount; i++) {
            Index t = targets[i].index();
            // == short-circuit: same object is the common case within one backing-index generation.
            if (t == concreteIndex || t.equals(concreteIndex)) {
                return i;
            }
        }

        // New concrete index — validate routing strategy and register the target.
        if (routing instanceof IndexRouting.ExtractFromSource) {
            if (routing instanceof IndexRouting.ExtractFromSource.ForIndexDimensions == false) {
                throw new IllegalArgumentException(
                    "index ["
                        + concreteIndex.getName()
                        + "] routes by extracting fields from _source, but this bulk supplies a pre-built source batch"
                        + " with no inline source; supply a pre-computed _tsid or use an index whose routing depends"
                        + " only on _id/_routing"
                );
            }
        }
        int shardCount = project.getIndexSafe(concreteIndex).getNumberOfShards();
        int partitionBase = totalPartitions;
        totalPartitions += shardCount;

        IndexTarget newTarget = new IndexTarget(concreteIndex, routing, shardCount, partitionBase);
        if (targets == null) {
            targets = new IndexTarget[2];
        } else if (targetCount == targets.length) {
            targets = Arrays.copyOf(targets, targetCount * 2);
        }
        targets[targetCount] = newTarget;
        int newTargetIdx = targetCount;
        targetCount++;

        // Allocate rowTargets exactly once, on the transition from 1→2 targets. == 2 (not >= 2)
        // ensures later targets don't re-allocate and discard already-written assignments. Zero fill
        // is correct: every row recorded before this call belongs to target 0.
        if (targetCount == 2) {
            rowTargets = new int[source.docCount()];
        }

        return newTargetIdx;
    }

    private void recordDeferredItem(BulkItemRequest bulkItem, int rowIndex, int targetIdx) {
        int docCount = source.docCount();
        if (rowIndex < 0 || rowIndex >= docCount) {
            throw new IllegalArgumentException(
                "rowIndex " + rowIndex + " is out of range [0, " + docCount + ") for pre-built batch [" + key + "]"
            );
        }
        if (rowIndex <= lastRow) {
            throw new IllegalArgumentException(
                "rowIndex "
                    + rowIndex
                    + " is not strictly greater than the previous row "
                    + lastRow
                    + " of pre-built batch ["
                    + key
                    + "]; rows must arrive in ascending order"
            );
        }
        if (items[rowIndex] != null) {
            throw new IllegalArgumentException("rowIndex " + rowIndex + " is claimed by two items in pre-built batch [" + key + "]");
        }
        lastRow = rowIndex;
        items[rowIndex] = bulkItem;
        if (rowTargets != null) {
            rowTargets[rowIndex] = targetIdx;
        }
        routedCount++;
    }

    /**
     * Completes routing: computes the shard assignment for each deferred item and adds it to
     * {@code requestsByShard}. Must be called exactly once; {@link #shardBatches()} scatters the
     * source data to match the grouping produced here.
     *
     * <p>Rows with no corresponding item (orphans from
     * {@link org.elasticsearch.escf.EscfBatchBuilder#abortRow} or items dropped during routing) are
     * left with {@code partitionIds == -1}; the scatter step remaps them to the discard partition.
     */
    Map<ShardId, List<BulkItemRequest>> buildGrouping(
        Map<ShardId, List<BulkItemRequest>> requestsByShard,
        BiConsumer<BulkItemRequest, Exception> onItemFailure
    ) {
        assert groupingBuilt == false : "buildGrouping called more than once";
        groupingBuilt = true;
        if (routedCount == 0) {
            return requestsByShard;
        }
        try {
            if (targetCount == 1) {
                routeSingleTarget(requestsByShard);
            } else {
                routeMultipleTargets(requestsByShard);
            }
        } catch (Exception e) {
            // The routing trio does not give us a row index, so we cannot isolate which row(s) caused
            // the problem. Fail every deferred item with the same exception.
            scattered = true; // prevent shardBatches() from attempting a stale scatter
            for (int i = 0; i < source.docCount(); i++) {
                if (items[i] != null) {
                    onItemFailure.accept(items[i], e);
                }
            }
        }
        return requestsByShard;
    }

    /**
     * Fast path for a single concrete index. Collects claimed rows (skipping any orphans), routes
     * them, and fills partitionIds for claimed rows only; orphan rows retain their {@code -1}.
     */
    private void routeSingleTarget(Map<ShardId, List<BulkItemRequest>> requestsByShard) {
        IndexTarget target = targets[0];
        int n = source.docCount();

        // Collect claimed rows to handle orphans (aborted rows or items dropped before route()).
        int[] claimedRows = new int[routedCount];
        IndexRequest[] requests = new IndexRequest[routedCount];
        int k = 0;
        for (int row = 0; row < n; row++) {
            if (items[row] != null) {
                claimedRows[k] = row;
                requests[k] = (IndexRequest) items[row].request();
                k++;
            }
        }

        target.routing().preProcess(requests);
        int[] shards = target.routing().indexShard(requests, source, claimedRows);
        target.routing().postProcess(requests);
        for (int i = 0; i < k; i++) {
            int row = claimedRows[i];
            int shardId = shards[i];
            partitionIds[row] = shardId; // partitionBase is 0 for target 0
            requestsByShard.computeIfAbsent(new ShardId(target.index(), shardId), ignored -> new ArrayList<>()).add(items[row]);
        }
    }

    /**
     * Routes multiple targets. Builds per-target row-index arrays, calls the routing trio once per
     * target (rollover-safe: each target has its own dimension predicate), then fills
     * {@link #partitionIds} and {@code requestsByShard} atomically.
     *
     * <p>All target routing completes before {@code requestsByShard} is modified, so a failure on any
     * target leaves {@code requestsByShard} untouched. Orphan rows (null items) are skipped.
     */
    private void routeMultipleTargets(Map<ShardId, List<BulkItemRequest>> requestsByShard) {
        int n = source.docCount();
        // Count rows per target to size the per-target arrays.
        int[] targetRowCounts = new int[targetCount];
        for (int row = 0; row < n; row++) {
            if (items[row] != null) {
                targetRowCounts[rowTargets[row]]++;
            }
        }

        // Build per-target row-index arrays (in ascending order) and IndexRequest arrays.
        int[][] rowsByTarget = new int[targetCount][];
        IndexRequest[][] requestsByTarget = new IndexRequest[targetCount][];
        for (int t = 0; t < targetCount; t++) {
            rowsByTarget[t] = new int[targetRowCounts[t]];
            requestsByTarget[t] = new IndexRequest[targetRowCounts[t]];
        }
        int[] fill = new int[targetCount];
        for (int row = 0; row < n; row++) {
            if (items[row] != null) {
                int t = rowTargets[row];
                int slot = fill[t]++;
                rowsByTarget[t][slot] = row;
                requestsByTarget[t][slot] = (IndexRequest) items[row].request();
            }
        }

        // Route each target. Collect shard ids before touching requestsByShard so a failure on a
        // later target leaves earlier targets' items out of the map (all-or-none).
        // No try/finally around postProcess: indexShard() clears batchHashes at entry (before it
        // can throw), so the routing object is already clean on failure. Calling postProcess() in
        // a finally would itself throw (batchHashes == null) and mask the real exception.
        int[][] shardsByTarget = new int[targetCount][];
        for (int t = 0; t < targetCount; t++) {
            IndexTarget target = targets[t];
            target.routing().preProcess(requestsByTarget[t]);
            shardsByTarget[t] = target.routing().indexShard(requestsByTarget[t], source, rowsByTarget[t]);
            target.routing().postProcess(requestsByTarget[t]);
        }

        // All routing succeeded — commit to partitionIds and requestsByShard.
        for (int t = 0; t < targetCount; t++) {
            IndexTarget target = targets[t];
            int[] rows = rowsByTarget[t];
            int[] shards = shardsByTarget[t];
            for (int k = 0; k < rows.length; k++) {
                int row = rows[k];
                int globalPartition = target.partitionBase() + shards[k];
                partitionIds[row] = globalPartition;
                requestsByShard.computeIfAbsent(new ShardId(target.index(), shards[k]), ignored -> new ArrayList<>()).add(items[row]);
            }
        }
    }

    /**
     * Returns the per-shard batches. Returns empty on any call after the first — the failure-store
     * redirect pass must not re-scatter batches already in flight.
     */
    Map<ShardId, SourceBatch> shardBatches() {
        if (scattered) {
            return Map.of();
        }
        scattered = true;
        if (routedCount == 0) {
            return Map.of();
        }
        int n = source.docCount();
        if (totalPartitions == 1 && routedCount == n) {
            // Fast path: single target, single shard, no dropped rows — hand the original batch
            // through untouched. Source rows were already set by the encoder or external producer.
            return Map.of(new ShardId(targets[0].index(), 0), source);
        }
        return scatter();
    }

    private Map<ShardId, SourceBatch> scatter() {
        int n = source.docCount();
        boolean hasOrphans = routedCount != n;
        int discardPartition = totalPartitions; // only meaningful when hasOrphans
        int totalPartitionsForScatter = hasOrphans ? totalPartitions + 1 : totalPartitions;

        // Remap unclaimed rows (partitionIds == -1) to the discard partition.
        if (hasOrphans) {
            for (int row = 0; row < n; row++) {
                if (partitionIds[row] < 0) {
                    partitionIds[row] = discardPartition;
                }
            }
        }

        EscfBatch[] parts;
        try (EscfBatchScatterer scatterer = new EscfBatchScatterer(BytesRefRecycler.NON_RECYCLING_INSTANCE)) {
            parts = scatterer.scatter(source, partitionIds, totalPartitionsForScatter);
        }

        // Immediately release the discard partition — it is the only copy of orphan rows and must
        // not be mapped to any shard.
        if (hasOrphans && parts[discardPartition] != null) {
            parts[discardPartition].close();
            parts[discardPartition] = null;
        }

        Map<ShardId, SourceBatch> result = new HashMap<>();
        int[] nextRow = new int[totalPartitions];
        for (int row = 0; row < n; row++) {
            int globalPartition = partitionIds[row];
            EscfBatch part = parts[globalPartition];
            if (part == null) {
                // Discard partition (released above) or a zero-row real partition — skip.
                continue;
            }

            IndexTarget target = targetForPartition(globalPartition);
            int localShardId = globalPartition - target.partitionBase();
            result.putIfAbsent(new ShardId(target.index(), localShardId), part);

            IndexRequest req = (IndexRequest) items[row].request();
            req.indexSource().setSourceRow(part, nextRow[globalPartition]++, req.indexSource().contentType());
        }
        return result;
    }

    /** Returns the target that owns the given global partition id. Linear scan; N is typically 1–2. */
    private IndexTarget targetForPartition(int globalPartition) {
        for (int t = 0; t < targetCount; t++) {
            IndexTarget target = targets[t];
            if (globalPartition >= target.partitionBase() && globalPartition < target.partitionBase() + target.shardCount()) {
                return target;
            }
        }
        throw new AssertionError("no target found for global partition " + globalPartition);
    }
}
