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
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
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
 * Per-bulk router: decides each item's destination shard and builds the per-shard {@link SourceBatch}.
 * Provided-batch mode maps pre-built {@link EscfBatch} rows to shards and scatters; x-content mode
 * delegates encoding and routing to {@link BulkBatchEncoders} (TODO: temporary — goes away once all
 * producers build ESCF at the index-abstraction level).
 */
final class BatchModeRouter implements Releasable {

    /**
     * One concrete backing index plus its routing and its global partition base.
     * {@code partitionBase + localShardId} is the global partition id for a row going to this target.
     */
    private record IndexTarget(Index index, IndexRouting routing, int shardCount, int partitionBase) {}

    private static final Logger logger = LogManager.getLogger(BatchModeRouter.class);
    @Nullable
    private final String indexAbstractionName;
    @Nullable
    private final EscfBatch source;
    @Nullable
    private final int[] partitionIds;
    @Nullable
    private final BulkItemRequest[] items;
    @Nullable
    private IndexTarget[] targets;
    private int targetCount;
    /** Sum of all {@link IndexTarget#shardCount()} values; also the next available partitionBase. */
    private int totalPartitions;

    /**
     * Target index (into {@link #targets}) for each row. Allocated lazily on the first second-target
     * sighting; stays null while {@code targetCount <= 1}. Zero-filled allocation is safe because all
     * rows recorded before the second target belongs to target 0 (the default zero value is correct).
     */
    @Nullable
    private int[] rowTargets;

    private int lastRow = -1;
    private int routedCount;
    private boolean scattered;
    private boolean groupingBuilt;

    // x-content mode state (null in provided-batch mode)
    @Nullable
    private final BulkBatchEncoders encoders;

    private BatchModeRouter(String indexAbstractionName, EscfBatch source) {
        this.indexAbstractionName = indexAbstractionName;
        this.source = source;
        this.partitionIds = new int[source.docCount()];
        this.items = new BulkItemRequest[source.docCount()];
        this.encoders = null;
    }

    private BatchModeRouter(BulkBatchEncoders encoders) {
        this.indexAbstractionName = null;
        this.source = null;
        this.partitionIds = null;
        this.items = null;
        this.encoders = encoders;
    }

    /** Returns the router for this bulk, or {@code null} when batch indexing does not apply. */
    @Nullable
    static BatchModeRouter create(BulkRequest bulkRequest, boolean batchIndexingSupported) {
        Map<String, SourceBatch> provided = bulkRequest.getPreBuiltBatches();
        boolean hasProvidedBatch = provided != null && provided.isEmpty() == false;

        if (hasProvidedBatch) {
            if (batchIndexingSupported == false) {
                throw new IllegalStateException(
                    "pre-built source batch submitted but batch indexing is not supported"
                        + " (setting disabled, feature flag off, or mixed-version cluster)"
                );
            }
            if (provided.size() > 1) {
                throw new IllegalArgumentException(
                    "pre-built source batch bulk carries "
                        + provided.size()
                        + " batches, but at most one is supported in step 1; multi-batch support will be added in a follow-up"
                );
            }
        } else if (batchIndexingSupported == false || bulkRequest.isSimulated() || bulkRequest.requests().isEmpty()) {
            return null;
        }

        // Single scan: both paths require all items to be IndexRequests; the provided-batch path
        // additionally requires every item to carry a source-row reference; the x-content path
        // requires each item to carry inline source with a known content type.
        for (DocWriteRequest<?> request : bulkRequest.requests()) {
            if (request instanceof IndexRequest indexRequest) {
                if (hasProvidedBatch) {
                    if (indexRequest.indexSource().hasSourceRow() == false) {
                        throw new IllegalArgumentException(
                            "item targeting index ["
                                + request.index()
                                + "] must carry a source-row reference when a pre-built batch is attached"
                        );
                    }
                } else if (BulkBatchEncoders.isItemBatchEligible(indexRequest) == false) {
                    return null;
                }
            } else {
                if (hasProvidedBatch) {
                    throw new IllegalArgumentException(
                        "["
                            + request.opType()
                            + "] operation on index ["
                            + request.index()
                            + "] cannot be mixed with pre-built source batches; every item of such a bulk must be an index"
                            + " request carrying a source-row reference"
                    );
                }
                return null;
            }
        }

        if (hasProvidedBatch) {
            Map.Entry<String, SourceBatch> only = provided.entrySet().iterator().next();
            String name = only.getKey();
            SourceBatch batch = only.getValue();
            if (batch instanceof EscfBatch escfBatch) {
                return new BatchModeRouter(name, escfBatch);
            }
            throw new IllegalArgumentException(
                "pre-built batch for index [" + name + "] must be an EscfBatch but was [" + batch.getClass().getName() + "]"
            );
        }

        return new BatchModeRouter(new BulkBatchEncoders());
    }

    /**
     * Resolves {@code @timestamp} from the ESCF columns and caches it on each item's
     * {@link IndexRequest} via {@link IndexRequest#setTimeSeriesTimestamp}. Must be called before the
     * per-item routing loop (before {@link #route}) so that
     * {@link DataStream#getWriteIndex(IndexRequest, ProjectMetadata)} finds the memoized value and can
     * select the correct backing index.
     *
     * <p>No-op in x-content mode (items have inline source) and for non-TSDB data streams.
     *
     * <p>All-or-none: if every request already has a timestamp set (producer supplied it out-of-band),
     * returns immediately. A mixed batch (some set, some not) throws.
     *
     * @param requests the bulk request's items, in the same order as the bulk; each row-bearing
     *                 {@link IndexRequest}'s row index is used to map it to the batch column
     * @throws IllegalArgumentException if {@code @timestamp} is missing, an unsupported kind,
     *                                  or a row has no value for it
     */
    void preResolveTimestamps(ProjectMetadata project, List<DocWriteRequest<?>> requests) {
        if (encoders != null || source == null) {
            return;
        }
        // Only TSDB data streams need per-document backing-index selection.
        IndexAbstraction ia = project.getIndicesLookup().get(indexAbstractionName);
        if (ia == null) {
            return;
        }
        DataStream dataStream = DataStream.resolveDataStream(ia, project);
        if (dataStream == null || IndexMode.isTsdb(dataStream.getIndexMode()) == false) {
            return;
        }

        // Build a row→IndexRequest map so we can set timestamps by row index.
        // Rows arrive in ascending order per the bulk invariant; any non-row-bearing request is skipped.
        IndexRequest[] byRow = new IndexRequest[source.docCount()];
        boolean anyHasTimestamp = false;
        boolean anyLacksTimestamp = false;
        for (DocWriteRequest<?> req : requests) {
            if (req instanceof IndexRequest ir && ir.indexSource().hasSourceRow()) {
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
                    + indexAbstractionName
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
                    + indexAbstractionName
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
                    + indexAbstractionName
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
     * {@link IndexRequest}. An absent row throws because TSDB requires every document to have a timestamp.
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
        checkAllRowsHaveTimestamp(seen, byRow, indexAbstractionName);
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
        checkAllRowsHaveTimestamp(seen, byRow, indexAbstractionName);
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
                        + indexAbstractionName
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
        checkAllRowsHaveTimestamp(seen, byRow, indexAbstractionName);
    }

    private static void checkAllRowsHaveTimestamp(boolean[] seen, IndexRequest[] byRow, String indexAbstractionName) {
        for (int row = 0; row < seen.length; row++) {
            if (seen[row] == false && byRow[row] != null) {
                throw new IllegalArgumentException(
                    "pre-built batch for ["
                        + indexAbstractionName
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
     * Records one item for routing. For the provided-batch mode the actual shard assignment is
     * deferred until {@link #buildGrouping}; for x-content the item is encoded and routed immediately.
     *
     * @param requestsByShard the grouping map to fill; both modes write into it
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
        if (encoders != null) {
            request.preRoutingProcess(routing);
            int shardId = encoders.tryEncodeAndRoute((IndexRequest) request, concreteIndex, routing);
            if (shardId == BulkBatchEncoders.NOT_BATCHABLE) {
                shardId = request.route(routing);
            }
            request.postRoutingProcess(routing);
            requestsByShard.computeIfAbsent(new ShardId(concreteIndex, shardId), k -> new ArrayList<>()).add(bulkItem);
        } else {
            IndexRequest batchItem = (IndexRequest) request;
            int targetIdx = prepareRouting(batchItem, abstraction, concreteIndex, routing, project);
            recordDeferredItem(bulkItem, batchItem.indexSource().rowIndex(), targetIdx);
        }
    }

    /**
     * Validates the request, resolves (or registers) the concrete-index target, and returns the target
     * index into {@link #targets}.
     */
    private int prepareRouting(
        IndexRequest request,
        IndexAbstraction abstraction,
        Index concreteIndex,
        IndexRouting routing,
        ProjectMetadata project
    ) {
        if (indexAbstractionName.equals(abstraction.getName()) == false) {
            throw new IllegalArgumentException(
                "item targeting index ["
                    + request.index()
                    + "] carries a source-row reference but no pre-built batch was supplied under that name;"
                    + " batches must be keyed by the name set on the requests whose rows they hold"
            );
        }

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
                "rowIndex " + rowIndex + " is out of range [0, " + docCount + ") for pre-built batch [" + indexAbstractionName + "]"
            );
        }
        if (rowIndex <= lastRow) {
            throw new IllegalArgumentException(
                "rowIndex "
                    + rowIndex
                    + " is not strictly greater than the previous row "
                    + lastRow
                    + " of pre-built batch ["
                    + indexAbstractionName
                    + "]; rows must arrive in ascending order"
            );
        }
        lastRow = rowIndex;
        items[rowIndex] = bulkItem;
        if (rowTargets != null) {
            rowTargets[rowIndex] = targetIdx;
        }
        routedCount++;
    }

    /**
     * Completes routing for provided-batch mode: computes the shard assignment for each deferred
     * item and adds it to {@code requestsByShard}, then returns that map. In x-content mode the
     * items were already routed in {@link #route}, so this is a no-op that returns the map as-is.
     * Must be called exactly once per bulk; {@link #shardBatches()} scatters the source data to
     * match the grouping produced here.
     */
    Map<ShardId, List<BulkItemRequest>> buildGrouping(
        Map<ShardId, List<BulkItemRequest>> requestsByShard,
        BiConsumer<BulkItemRequest, Exception> onItemFailure
    ) {
        assert groupingBuilt == false : "buildGrouping called more than once";
        groupingBuilt = true;
        if (encoders != null) {
            return requestsByShard;
        }
        if (routedCount == 0) {
            return requestsByShard;
        }
        // Count mismatch is a caller precondition violation (wrong batch attached), not a per-item
        // routing failure, so we throw rather than routing through onItemFailure.
        if (routedCount != source.docCount()) {
            throw new IllegalStateException(
                "pre-built batch ["
                    + indexAbstractionName
                    + "] had "
                    + source.docCount()
                    + " rows but only "
                    + routedCount
                    + " were routed; dropped rows in pre-built batches are not yet supported and will be added in a follow-up"
            );
        }
        try {
            if (targetCount == 1) {
                routeSingleTarget(requestsByShard);
            } else {
                routeMultipleTargets(requestsByShard);
            }
        } catch (Exception e) {
            // The trio does not give us a row index, so we cannot isolate which row(s) caused the
            // problem. Fail every deferred item with the same exception.
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
     * Fast path for a single concrete index — identical semantics to the original single-index logic.
     */
    private void routeSingleTarget(Map<ShardId, List<BulkItemRequest>> requestsByShard) {
        IndexTarget target = targets[0];
        IndexRequest[] requests = buildRequestArray();
        target.routing().preProcess(requests);
        int[] shards = target.routing().indexShard(requests, source, null);
        target.routing().postProcess(requests);
        for (int i = 0; i < requests.length; i++) {
            int shardId = shards[i];
            partitionIds[i] = shardId; // partitionBase is 0 for target 0
            requestsByShard.computeIfAbsent(new ShardId(target.index(), shardId), k -> new ArrayList<>()).add(items[i]);
        }
    }

    /**
     * Routes multiple targets. Builds per-target row-index arrays, calls the routing trio once per
     * target (rollover-safe: each target has its own dimension predicate), then fills
     * {@link #partitionIds} and {@code requestsByShard} atomically.
     *
     * <p>All target routing completes before {@code requestsByShard} is modified, so a failure on any
     * target leaves {@code requestsByShard} untouched.
     */
    private void routeMultipleTargets(Map<ShardId, List<BulkItemRequest>> requestsByShard) {
        // Count rows per target to size the per-target arrays.
        int[] targetRowCounts = new int[targetCount];
        for (int row = 0; row < routedCount; row++) {
            targetRowCounts[rowTargets[row]]++;
        }

        // Build per-target row-index arrays (in ascending order) and IndexRequest arrays.
        int[][] rowsByTarget = new int[targetCount][];
        IndexRequest[][] requestsByTarget = new IndexRequest[targetCount][];
        for (int t = 0; t < targetCount; t++) {
            rowsByTarget[t] = new int[targetRowCounts[t]];
            requestsByTarget[t] = new IndexRequest[targetRowCounts[t]];
        }
        int[] fill = new int[targetCount];
        for (int row = 0; row < routedCount; row++) {
            int t = rowTargets[row];
            int slot = fill[t]++;
            rowsByTarget[t][slot] = row;
            requestsByTarget[t][slot] = (IndexRequest) items[row].request();
        }

        // Route each target. Collect shard ids before touching requestsByShard so a failure on a
        // later target leaves earlier targets' items out of the map (all-or-none).
        // No try/finally around postProcess: indexShard() clears batchHashes at entry (before it
        // can throw), so the routing object is already clean on failure. Calling postProcess() in
        // a finally would itself throw (batchHashes == null) and mask the real exception.
        // The failed items are never re-routed, so the preProcess side-effect (auto-generated id)
        // on partially-processed requests is harmless.
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
                requestsByShard.computeIfAbsent(new ShardId(target.index(), shards[k]), key -> new ArrayList<>()).add(items[row]);
            }
        }
    }

    private IndexRequest[] buildRequestArray() {
        IndexRequest[] requests = new IndexRequest[routedCount];
        for (int i = 0; i < routedCount; i++) {
            requests[i] = (IndexRequest) items[i].request();
        }
        return requests;
    }

    /**
     * Returns the per-shard batches. For provided-batch mode, returns empty on any call after the
     * first — the failure-store redirect pass must not re-scatter batches already in flight.
     */
    Map<ShardId, SourceBatch> shardBatches() {
        if (encoders != null) {
            return encoders.finalizeBatches();
        }
        if (scattered) {
            return Map.of();
        }
        scattered = true;
        if (routedCount == 0) {
            return Map.of();
        }
        if (totalPartitions == 1) {
            // Fast path: single target, single shard — hand the original batch through untouched.
            return Map.of(new ShardId(targets[0].index(), 0), source);
        }
        return scatter();
    }

    private Map<ShardId, SourceBatch> scatter() {
        EscfBatch[] parts;
        try (EscfBatchScatterer scatterer = new EscfBatchScatterer(BytesRefRecycler.NON_RECYCLING_INSTANCE)) {
            parts = scatterer.scatter(source, partitionIds, totalPartitions);
        }
        Map<ShardId, SourceBatch> result = new HashMap<>();
        int[] nextRow = new int[totalPartitions];
        for (int row = 0; row < routedCount; row++) {
            int globalPartition = partitionIds[row];
            EscfBatch part = parts[globalPartition];
            assert part != null : "null partition " + globalPartition + " for row " + row;

            // Recover the ShardId from the global partition: find which target owns this partition.
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
        if (encoders != null) {
            encoders.close();
        }
    }
}
