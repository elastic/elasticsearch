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
import org.elasticsearch.core.Releasable;
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
 * Per-bulk router: decides each item's destination shard and builds the per-shard {@link SourceBatch}
 * by scattering one pre-built {@link EscfBatch} per index abstraction.
 *
 * <p>A bulk may carry multiple batches — one per index-abstraction write target. Each batch is
 * tracked as a {@link BatchGroup}. The router routes items to their group's concrete shards
 * (deferring the actual shard assignment until {@link #buildGrouping}), then scatters each batch
 * into per-shard sub-batches via {@link EscfBatchScatterer}.
 *
 * <p>Items that are dropped during the grouping loop (closed index, validation failure, etc.)
 * leave their corresponding rows unclaimed. At scatter time those orphan rows are routed to a
 * discard partition that is immediately released and never mapped to a {@link ShardId}.
 */
final class BatchModeRouter implements Releasable {

    /**
     * One concrete backing index plus its routing and its global partition base.
     * {@code partitionBase + localShardId} is the global partition id for a row going to this target.
     */
    private record IndexTarget(Index index, IndexRouting routing, int shardCount, int partitionBase) {}

    /**
     * All per-batch state for one index-abstraction write target. The router may hold several of
     * these (one per target), but the typical case is exactly one.
     */
    private static final class BatchGroup {
        /** Key returned by {@link #batchKey} for this group's abstraction. */
        final String key;
        final EscfBatch source;
        /**
         * Global partition id for each row. Initialized to {@code -1} (unclaimed / discard).
         * {@link #buildGroupingForGroup} overwrites entries for successfully routed rows with their
         * real partition ids; any remaining {@code -1} entries identify orphan rows that must go to
         * the discard partition during scatter.
         */
        final int[] partitionIds;
        /** The {@link BulkItemRequest} for each row, in row-index order. Null for orphan rows. */
        final BulkItemRequest[] items;
        /**
         * Concrete-index targets encountered so far. Linear scan; N is typically 1–2 (TSDB rollover
         * boundary).
         */
        @Nullable
        IndexTarget[] targets;
        int targetCount;
        /** Sum of all {@link IndexTarget#shardCount()} values; also the next available partitionBase. */
        int totalPartitions;

        /**
         * Target index (into {@link #targets}) for each row. Allocated lazily on the first
         * second-target sighting; stays null while {@code targetCount <= 1}. Zero-fill is correct:
         * every row recorded before the second target belongs to target 0.
         */
        @Nullable
        int[] rowTargets;

        int lastRow = -1;
        int routedCount;

        BatchGroup(String key, EscfBatch source) {
            this.key = key;
            this.source = source;
            int n = source.docCount();
            this.partitionIds = new int[n];
            Arrays.fill(this.partitionIds, -1);
            this.items = new BulkItemRequest[n];
        }
    }

    private final BatchGroup[] groups;
    private final int groupCount;

    private boolean scattered;
    private boolean groupingBuilt;

    private BatchModeRouter(BatchGroup[] groups, int groupCount) {
        this.groups = groups;
        this.groupCount = groupCount;
    }

    /**
     * Returns the router for this bulk when the bulk carries externally pre-built ESCF batches, or
     * {@code null} when batch indexing does not apply.
     *
     * <p>The internally-encoded path (via {@code BulkEscfEncodePass}) creates a router via
     * {@link #forBatches} after the encode pass completes.
     */
    @Nullable
    static BatchModeRouter create(BulkRequest bulkRequest, boolean batchIndexingSupported) {
        Map<String, SourceBatch> provided = bulkRequest.getPreBuiltBatches();
        if (provided == null || provided.isEmpty()) {
            return null;
        }
        if (batchIndexingSupported == false) {
            throw new IllegalStateException(
                "pre-built source batch submitted but batch indexing is not supported"
                    + " (setting disabled, feature flag off, or mixed-version cluster)"
            );
        }

        // Validate that every item is an IndexRequest carrying a source-row reference.
        for (DocWriteRequest<?> request : bulkRequest.requests()) {
            if (request instanceof IndexRequest indexRequest) {
                if (indexRequest.indexSource().hasSourceRow() == false) {
                    throw new IllegalArgumentException(
                        "item targeting index ["
                            + request.index()
                            + "] must carry a source-row reference when a pre-built batch is attached"
                    );
                }
            } else {
                throw new IllegalArgumentException(
                    "["
                        + request.opType()
                        + "] operation on index ["
                        + request.index()
                        + "] cannot be mixed with pre-built source batches; every item of such a bulk must be an index"
                        + " request carrying a source-row reference"
                );
            }
        }

        return forBatches(provided);
    }

    /**
     * Creates a router from a map of pre-built (or internally-encoded) {@link SourceBatch}es. Each
     * key is the string returned by {@link #batchKey} for the corresponding abstraction.
     *
     * @throws IllegalArgumentException if any batch is not an {@link EscfBatch}
     */
    static BatchModeRouter forBatches(Map<String, SourceBatch> batches) {
        BatchGroup[] groups = new BatchGroup[batches.size()];
        int g = 0;
        for (Map.Entry<String, SourceBatch> e : batches.entrySet()) {
            String name = e.getKey();
            SourceBatch batch = e.getValue();
            if (batch instanceof EscfBatch escfBatch) {
                groups[g++] = new BatchGroup(name, escfBatch);
            } else {
                throw new IllegalArgumentException(
                    "pre-built batch for [" + name + "] must be an EscfBatch but was [" + batch.getClass().getName() + "]"
                );
            }
        }
        return new BatchModeRouter(groups, g);
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
     * Resolves {@code @timestamp} from each group's ESCF columns and caches it on each item's
     * {@link IndexRequest} via {@link IndexRequest#setTimeSeriesTimestamp}. Must be called before the
     * per-item routing loop (before {@link #route}) so that
     * {@link DataStream#getWriteIndex(IndexRequest, ProjectMetadata)} finds the memoized value and can
     * select the correct backing index.
     *
     * <p>No-op for non-TSDB data streams. All-or-none per group: if every request in a group
     * already has a timestamp set (producer supplied it out-of-band), that group is skipped. A mixed
     * group (some set, some not) throws.
     *
     * @param requests the bulk request's items, in the same order as the bulk; each row-bearing
     *                 {@link IndexRequest}'s row index is used to map it to the batch column
     * @throws IllegalArgumentException if {@code @timestamp} is missing, an unsupported kind,
     *                                  or a row has no value for it
     */
    void preResolveTimestamps(ProjectMetadata project, List<DocWriteRequest<?>> requests) {
        for (int g = 0; g < groupCount; g++) {
            preResolveTimestampsForGroup(groups[g], project, requests);
        }
    }

    private static void preResolveTimestampsForGroup(BatchGroup group, ProjectMetadata project, List<DocWriteRequest<?>> requests) {
        // Only TSDB data streams need per-document backing-index selection.
        IndexAbstraction ia = project.getIndicesLookup().get(group.key);
        if (ia == null) {
            return;
        }
        DataStream dataStream = DataStream.resolveDataStream(ia, project);
        if (dataStream == null || IndexMode.isTsdb(dataStream.getIndexMode()) == false) {
            return;
        }

        // Build a row→IndexRequest map so we can set timestamps by row index. Filter to items that
        // belong to this group (multi-group bulks may have overlapping row-index ranges across groups).
        IndexRequest[] byRow = new IndexRequest[group.source.docCount()];
        boolean anyHasTimestamp = false;
        boolean anyLacksTimestamp = false;
        for (DocWriteRequest<?> req : requests) {
            if (req instanceof IndexRequest ir && ir.indexSource().hasSourceRow()) {
                // Determine which group this item belongs to via its abstraction.
                IndexAbstraction reqIa = project.getIndicesLookup().get(ir.index());
                String reqKey = batchKey(reqIa, project);
                if (group.key.equals(reqKey) == false) {
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
                    + group.key
                    + "] has a mix of requests with and without a pre-set timestamp;"
                    + " either all requests must supply a timestamp (producer-side) or none"
            );
        }
        if (anyHasTimestamp) {
            return; // All timestamps pre-supplied by the producer — nothing to resolve.
        }

        // Find the @timestamp column index.
        SourceSchema schema = group.source.schema();
        int leaf = schema.findLeaf(DataStream.TIMESTAMP_FIELD_NAME, 0);
        if (leaf < 0) {
            throw new IllegalArgumentException(
                "pre-built batch for ["
                    + group.key
                    + "] targets a TSDB data stream but the batch has no ["
                    + DataStream.TIMESTAMP_FIELD_NAME
                    + "] column; the batch producer must include a timestamp column"
            );
        }

        EscfColumn col = group.source.column(leaf);
        byte kind = col.kind();

        switch (kind) {
            case EscfColumnKind.LONG -> resolveTimestampsFromLong(group.key, group.source, col, byRow);
            case EscfColumnKind.STRING -> resolveTimestampsFromString(group.key, group.source, col, byRow);
            case EscfColumnKind.UNION -> resolveTimestampsFromUnion(group.key, group.source, col, byRow);
            default -> throw new UnsupportedOperationException(
                "pre-built batch for ["
                    + group.key
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
    private static void resolveTimestampsFromLong(String groupKey, EscfBatch source, EscfColumn col, IndexRequest[] byRow) {
        int docCount = source.docCount();
        boolean[] seen = new boolean[docCount];
        LongTupleCursor cursor = col.longCursor();
        int r;
        while ((r = cursor.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
            setTimestampOnRequest(byRow, r, Instant.ofEpochMilli(cursor.longValue()));
            seen[r] = true;
        }
        checkAllRowsHaveTimestamp(seen, byRow, groupKey);
    }

    /**
     * Resolves {@code @timestamp} from a STRING (ISO-8601 or epoch-millis) column and caches on each
     * row's {@link IndexRequest}.
     */
    private static void resolveTimestampsFromString(String groupKey, EscfBatch source, EscfColumn col, IndexRequest[] byRow) {
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
        checkAllRowsHaveTimestamp(seen, byRow, groupKey);
    }

    /**
     * Resolves {@code @timestamp} from a UNION column (dispatching on the per-row type byte) and
     * caches on each row's {@link IndexRequest}.
     */
    private static void resolveTimestampsFromUnion(String groupKey, EscfBatch source, EscfColumn col, IndexRequest[] byRow) {
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
                        + groupKey
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
        checkAllRowsHaveTimestamp(seen, byRow, groupKey);
    }

    private static void checkAllRowsHaveTimestamp(boolean[] seen, IndexRequest[] byRow, String groupKey) {
        for (int row = 0; row < seen.length; row++) {
            if (seen[row] == false && byRow[row] != null) {
                throw new IllegalArgumentException(
                    "pre-built batch for ["
                        + groupKey
                        + "] row "
                        + row
                        + " has no ["
                        + DataStream.TIMESTAMP_FIELD_NAME
                        + "] value; every TSDB document must have a timestamp"
                );
            }
        }
    }

    private static void setTimestampOnRequest(IndexRequest[] byRow, int row, Instant rawTimestamp) {
        Instant ts = DataStream.getCanonicalTimestampBound(rawTimestamp);
        IndexRequest ir = byRow[row];
        if (ir != null) {
            ir.setTimeSeriesTimestamp(ts);
        }
    }

    /**
     * Records one item for deferred routing. The actual shard assignment is deferred until
     * {@link #buildGrouping}; this call registers the item with its batch group via
     * {@link #batchKey}.
     */
    void route(
        BulkItemRequest bulkItem,
        DocWriteRequest<?> request,
        IndexAbstraction abstraction,
        Index concreteIndex,
        IndexRouting routing,
        ProjectMetadata project,
        Map<ShardId, List<BulkItemRequest>> requestsByShard
    ) {
        IndexRequest batchItem = (IndexRequest) request;
        BatchGroup group = findGroup(abstraction, project);
        if (group == null) {
            throw new IllegalArgumentException(
                "item targeting index ["
                    + request.index()
                    + "] carries a source-row reference but no pre-built batch was supplied for it;"
                    + " batches must be keyed by the write-target resolved from the request's target index"
            );
        }
        int targetIdx = prepareRouting(group, batchItem, concreteIndex, routing, project);
        recordDeferredItem(group, bulkItem, batchItem.indexSource().rowIndex(), targetIdx);
    }

    /** Linear scan over groups; N is typically 1. Returns {@code null} if no group key matches. */
    @Nullable
    private BatchGroup findGroup(IndexAbstraction abstraction, ProjectMetadata project) {
        String key = batchKey(abstraction, project);
        if (key == null) {
            return null;
        }
        for (int g = 0; g < groupCount; g++) {
            if (groups[g].key.equals(key)) {
                return groups[g];
            }
        }
        return null;
    }

    /**
     * Validates the request, resolves (or registers) the concrete-index target for the group, and
     * returns the target index into {@link BatchGroup#targets}.
     */
    private static int prepareRouting(
        BatchGroup group,
        IndexRequest request,
        Index concreteIndex,
        IndexRouting routing,
        ProjectMetadata project
    ) {
        // Linear scan over targets (typically 1–2 entries at a TSDB rollover boundary; no Map needed).
        for (int i = 0; i < group.targetCount; i++) {
            Index t = group.targets[i].index();
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
        int partitionBase = group.totalPartitions;
        group.totalPartitions += shardCount;

        IndexTarget newTarget = new IndexTarget(concreteIndex, routing, shardCount, partitionBase);
        if (group.targets == null) {
            group.targets = new IndexTarget[2];
        } else if (group.targetCount == group.targets.length) {
            group.targets = Arrays.copyOf(group.targets, group.targetCount * 2);
        }
        group.targets[group.targetCount] = newTarget;
        int newTargetIdx = group.targetCount;
        group.targetCount++;

        // Allocate rowTargets exactly once, on the transition from 1→2 targets. == 2 (not >= 2)
        // ensures later targets don't re-allocate and discard already-written assignments. Zero fill
        // is correct: every row recorded before this call belongs to target 0.
        if (group.targetCount == 2) {
            group.rowTargets = new int[group.source.docCount()];
        }

        return newTargetIdx;
    }

    private static void recordDeferredItem(BatchGroup group, BulkItemRequest bulkItem, int rowIndex, int targetIdx) {
        int docCount = group.source.docCount();
        if (rowIndex < 0 || rowIndex >= docCount) {
            throw new IllegalArgumentException(
                "rowIndex " + rowIndex + " is out of range [0, " + docCount + ") for pre-built batch [" + group.key + "]"
            );
        }
        if (rowIndex <= group.lastRow) {
            throw new IllegalArgumentException(
                "rowIndex "
                    + rowIndex
                    + " is not strictly greater than the previous row "
                    + group.lastRow
                    + " of pre-built batch ["
                    + group.key
                    + "]; rows must arrive in ascending order"
            );
        }
        if (group.items[rowIndex] != null) {
            throw new IllegalArgumentException("rowIndex " + rowIndex + " is claimed by two items in pre-built batch [" + group.key + "]");
        }
        group.lastRow = rowIndex;
        group.items[rowIndex] = bulkItem;
        if (group.rowTargets != null) {
            group.rowTargets[rowIndex] = targetIdx;
        }
        group.routedCount++;
    }

    /**
     * Completes routing for all groups: computes the shard assignment for each deferred item and
     * adds it to {@code requestsByShard}. Must be called exactly once per bulk;
     * {@link #shardBatches()} scatters the source data to match the grouping produced here.
     *
     * <p>Rows with no corresponding item (orphans from {@link org.elasticsearch.escf.EscfBatchBuilder#abortRow}
     * or items dropped during routing) are left with {@code partitionIds == -1}; the scatter step
     * remaps them to the discard partition.
     */
    Map<ShardId, List<BulkItemRequest>> buildGrouping(
        Map<ShardId, List<BulkItemRequest>> requestsByShard,
        BiConsumer<BulkItemRequest, Exception> onItemFailure
    ) {
        assert groupingBuilt == false : "buildGrouping called more than once";
        groupingBuilt = true;
        for (int g = 0; g < groupCount; g++) {
            buildGroupingForGroup(groups[g], requestsByShard, onItemFailure);
        }
        return requestsByShard;
    }

    private void buildGroupingForGroup(
        BatchGroup group,
        Map<ShardId, List<BulkItemRequest>> requestsByShard,
        BiConsumer<BulkItemRequest, Exception> onItemFailure
    ) {
        if (group.routedCount == 0) {
            return;
        }
        try {
            if (group.targetCount == 1) {
                routeSingleTarget(group, requestsByShard);
            } else {
                routeMultipleTargets(group, requestsByShard);
            }
        } catch (Exception e) {
            // Routing failed for this group. Fail every deferred item; mark scattered so
            // shardBatches() won't try a stale scatter.
            scattered = true;
            for (int i = 0; i < group.source.docCount(); i++) {
                if (group.items[i] != null) {
                    onItemFailure.accept(group.items[i], e);
                }
            }
        }
    }

    /**
     * Fast path for a single concrete index. Collects claimed rows (skipping any orphans), routes
     * them, and fills partitionIds for claimed rows only; orphan rows retain their {@code -1}.
     */
    private static void routeSingleTarget(BatchGroup group, Map<ShardId, List<BulkItemRequest>> requestsByShard) {
        IndexTarget target = group.targets[0];
        int n = group.source.docCount();

        // Collect claimed rows to handle orphans (aborted rows or items dropped before route()).
        int[] claimedRows = new int[group.routedCount];
        IndexRequest[] requests = new IndexRequest[group.routedCount];
        int k = 0;
        for (int row = 0; row < n; row++) {
            if (group.items[row] != null) {
                claimedRows[k] = row;
                requests[k] = (IndexRequest) group.items[row].request();
                k++;
            }
        }

        target.routing().preProcess(requests);
        int[] shards = target.routing().indexShard(requests, group.source, claimedRows);
        target.routing().postProcess(requests);
        for (int i = 0; i < k; i++) {
            int row = claimedRows[i];
            int shardId = shards[i];
            group.partitionIds[row] = shardId; // partitionBase is 0 for target 0
            requestsByShard.computeIfAbsent(new ShardId(target.index(), shardId), key -> new ArrayList<>()).add(group.items[row]);
        }
    }

    /**
     * Routes multiple targets. Builds per-target row-index arrays, calls the routing trio once per
     * target (rollover-safe: each target has its own dimension predicate), then fills
     * {@link BatchGroup#partitionIds} and {@code requestsByShard} atomically.
     *
     * <p>All target routing completes before {@code requestsByShard} is modified, so a failure on any
     * target leaves {@code requestsByShard} untouched. Orphan rows (null items) are skipped.
     */
    private static void routeMultipleTargets(BatchGroup group, Map<ShardId, List<BulkItemRequest>> requestsByShard) {
        int n = group.source.docCount();
        // Count rows per target to size the per-target arrays.
        int[] targetRowCounts = new int[group.targetCount];
        for (int row = 0; row < n; row++) {
            if (group.items[row] != null) {
                targetRowCounts[group.rowTargets[row]]++;
            }
        }

        // Build per-target row-index arrays (in ascending order) and IndexRequest arrays.
        int[][] rowsByTarget = new int[group.targetCount][];
        IndexRequest[][] requestsByTarget = new IndexRequest[group.targetCount][];
        for (int t = 0; t < group.targetCount; t++) {
            rowsByTarget[t] = new int[targetRowCounts[t]];
            requestsByTarget[t] = new IndexRequest[targetRowCounts[t]];
        }
        int[] fill = new int[group.targetCount];
        for (int row = 0; row < n; row++) {
            if (group.items[row] != null) {
                int t = group.rowTargets[row];
                int slot = fill[t]++;
                rowsByTarget[t][slot] = row;
                requestsByTarget[t][slot] = (IndexRequest) group.items[row].request();
            }
        }

        // Route each target. Collect shard ids before touching requestsByShard so a failure on a
        // later target leaves earlier targets' items out of the map (all-or-none).
        // No try/finally around postProcess: indexShard() clears batchHashes at entry (before it
        // can throw), so the routing object is already clean on failure. Calling postProcess() in
        // a finally would itself throw (batchHashes == null) and mask the real exception.
        int[][] shardsByTarget = new int[group.targetCount][];
        for (int t = 0; t < group.targetCount; t++) {
            IndexTarget target = group.targets[t];
            target.routing().preProcess(requestsByTarget[t]);
            shardsByTarget[t] = target.routing().indexShard(requestsByTarget[t], group.source, rowsByTarget[t]);
            target.routing().postProcess(requestsByTarget[t]);
        }

        // All routing succeeded — commit to partitionIds and requestsByShard.
        for (int t = 0; t < group.targetCount; t++) {
            IndexTarget target = group.targets[t];
            int[] rows = rowsByTarget[t];
            int[] shards = shardsByTarget[t];
            for (int k = 0; k < rows.length; k++) {
                int row = rows[k];
                int globalPartition = target.partitionBase() + shards[k];
                group.partitionIds[row] = globalPartition;
                requestsByShard.computeIfAbsent(new ShardId(target.index(), shards[k]), key -> new ArrayList<>()).add(group.items[row]);
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
        Map<ShardId, SourceBatch> result = new HashMap<>();
        for (int g = 0; g < groupCount; g++) {
            scatterGroup(groups[g], result);
        }
        return result;
    }

    private static void scatterGroup(BatchGroup group, Map<ShardId, SourceBatch> result) {
        if (group.routedCount == 0) {
            return;
        }
        int n = group.source.docCount();
        if (group.totalPartitions == 1 && group.routedCount == n) {
            // Fast path: single target, single shard, no dropped rows — hand the original batch
            // through untouched. Source rows were already set by the encoder or external producer.
            result.put(new ShardId(group.targets[0].index(), 0), group.source);
            return;
        }

        boolean hasOrphans = group.routedCount != n;
        int discardPartition = group.totalPartitions; // only meaningful when hasOrphans
        int totalPartitionsForScatter = hasOrphans ? group.totalPartitions + 1 : group.totalPartitions;

        // Remap unclaimed rows (partitionIds == -1) to the discard partition.
        if (hasOrphans) {
            for (int row = 0; row < n; row++) {
                if (group.partitionIds[row] < 0) {
                    group.partitionIds[row] = discardPartition;
                }
            }
        }

        EscfBatch[] parts;
        try (EscfBatchScatterer scatterer = new EscfBatchScatterer(BytesRefRecycler.NON_RECYCLING_INSTANCE)) {
            parts = scatterer.scatter(group.source, group.partitionIds, totalPartitionsForScatter);
        }

        // Immediately release the discard partition — it is the only copy of orphan rows and must
        // not be mapped to any shard.
        if (hasOrphans && parts[discardPartition] != null) {
            parts[discardPartition].close();
            parts[discardPartition] = null;
        }

        int[] nextRow = new int[group.totalPartitions];
        for (int row = 0; row < n; row++) {
            int globalPartition = group.partitionIds[row];
            EscfBatch part = parts[globalPartition];
            if (part == null) {
                // Discard partition (released above) or a zero-row real partition — skip.
                continue;
            }

            IndexTarget target = targetForPartition(group, globalPartition);
            int localShardId = globalPartition - target.partitionBase();
            result.putIfAbsent(new ShardId(target.index(), localShardId), part);

            IndexRequest req = (IndexRequest) group.items[row].request();
            req.indexSource().setSourceRow(part, nextRow[globalPartition]++, req.indexSource().contentType());
        }
    }

    /** Returns the target that owns the given global partition id. Linear scan; N is typically 1–2. */
    private static IndexTarget targetForPartition(BatchGroup group, int globalPartition) {
        for (int t = 0; t < group.targetCount; t++) {
            IndexTarget target = group.targets[t];
            if (globalPartition >= target.partitionBase() && globalPartition < target.partitionBase() + target.shardCount()) {
                return target;
            }
        }
        throw new AssertionError("no target found for global partition " + globalPartition);
    }

    /**
     * Verifies 1:1 alignment between shard items and their batch rows. The wire format rebuilds row
     * numbers from item ordinal, so misalignment would silently index the wrong source.
     */
    static void validateBatchAlignment(Map<ShardId, List<BulkItemRequest>> requestsByShard, Map<ShardId, SourceBatch> shardBatches) {
        for (Map.Entry<ShardId, List<BulkItemRequest>> entry : requestsByShard.entrySet()) {
            List<BulkItemRequest> items = entry.getValue();
            SourceBatch shardBatch = shardBatches.get(entry.getKey());
            if (shardBatch == null) {
                for (BulkItemRequest item : items) {
                    if (item.request() instanceof IndexRequest indexRequest && indexRequest.indexSource().hasSourceRow()) {
                        throw new IllegalStateException(
                            "item ["
                                + item.id()
                                + "] of shard ["
                                + entry.getKey()
                                + "] holds a source-row reference but its shard request has no batch attached;"
                                + " it would be indexed with an empty source"
                        );
                    }
                }
            } else if (BulkShardBatch.rowsAlignWithItems(shardBatch, items) == false) {
                throw new IllegalStateException(
                    "batch for shard ["
                        + entry.getKey()
                        + "] does not align with its items (batch rows: "
                        + shardBatch.docCount()
                        + ", items: "
                        + items.size()
                        + "); this indicates a bug in the scatter logic"
                );
            }
        }
    }

    @Override
    public void close() {
        // Nothing to release: source batches are owned by the caller (BulkEscfEncodePass for
        // internally-encoded batches, or the external producer for pre-built batches).
    }
}
