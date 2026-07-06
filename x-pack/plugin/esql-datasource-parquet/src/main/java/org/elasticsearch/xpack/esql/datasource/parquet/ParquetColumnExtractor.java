/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.parquet.column.ColumnReader;
import org.apache.parquet.column.impl.ColumnReadStoreImpl;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.spi.ColumnExtractor;
import org.elasticsearch.xpack.esql.datasources.spi.DeclaredTypeCoercions;
import org.elasticsearch.xpack.esql.datasources.spi.ErrorPolicy;
import org.elasticsearch.xpack.esql.datasources.spi.SkipWarnings;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

/**
 * Parquet implementation of {@link ColumnExtractor}.
 * <p>
 * The extractor owns the file's full {@link ParquetMetadata} and addresses rows by file-global
 * physical row index — the same identity the {@link OptimizedParquetColumnIterator} stamps into
 * the synthetic {@code _rowPosition} column during the forward scan. Address space is therefore
 * {@code [0, rowCount())} where {@code rowCount} is the sum of all row groups' row counts; the
 * mapping from a position to the (row group, in-row-group offset) pair comes from a binary
 * search over a precomputed prefix sum of row-group row counts. Identities are stable across
 * splits, so any extractor on the same file resolves any survivor's identity correctly,
 * regardless of which split first emitted it.
 * <p>
 * Algorithm per multi-column {@link #extract(String[], DataType[], long[], BlockFactory) extract} call (no
 * slow / sequential fallback path; the extractor is deliberately strict so a regression that
 * would silently fall back to whole-RG scanning fails loud instead):
 * <ol>
 *   <li><b>Bucket positions by row group, once.</b> A single linear scan over
 *       {@code localPositions} maps each request to its owning row group via binary search over
 *       the cumulative-row-count prefix sum. No global sort: ordering is needed only inside each
 *       bucket, and per-bucket sorting on a handful of positions is essentially free. Row groups
 *       with no surviving position are never opened. The bucket structure is column-agnostic, so
 *       it is reused across every requested column.</li>
 *   <li><b>Per-row-group async prefetch, dispatched in parallel.</b> One
 *       {@link ColumnChunkPrefetcher#prefetchAsync} call per visited row group, each carrying the
 *       full multi-column projection. {@link CoalescedRangeReader} merges adjacent column-chunk
 *       ranges <em>within</em> the row group (column chunks in one row group are written
 *       contiguously, so the multi-column projection coalesces naturally) and dispatches the
 *       merged ranges to {@link StorageObject#readBytesAsync}. All buckets fan out at once: the
 *       extractor never blocks on row group {@code k}'s bytes before issuing row group
 *       {@code k+1}'s GET. Per-request RTT/TTFB cost goes from {@code O(row groups × columns)}
 *       down to roughly {@code O(row groups)}, and the wall-clock cost of the slowest GET is no
 *       longer additive across row groups.</li>
 *   <li><b>Pipelined decode in arrival order.</b> A small bounded queue receives each
 *       per-bucket prefetch as it completes. The decode loop drains buckets in arrival order,
 *       running flat ({@link PageColumnReader#readBatchSparse}) or list (skip/read driven by a
 *       {@link ColumnReader}) decode for every requested column against that bucket's
 *       prefetched chunk map. Decoding bucket {@code i} therefore overlaps with the still
 *       in-flight S3 reads for the slower buckets — the synchronous barrier of
 *       <em>"wait for the slowest GET, then start any decode"</em> is removed.</li>
 *   <li><b>Stitch back to caller order.</b> Each bucket produces one per-(bucket, column) block
 *       sized to that bucket's unique positions; per-column blocks are concatenated in
 *       bucket-visit order (independent of decode arrival order). A gather permutation then
 *       routes each caller slot to its position in the concatenated block via
 *       {@link Block#filter(boolean, int...)}, replaying duplicate requests for the same row.</li>
 *   <li><b>Coerce to the target type.</b> The extractor always decodes a column at the file's own
 *       physical type; when the caller-supplied target (planner/declared) type differs, the
 *       stitched block is coerced through {@link DeclaredTypeCoercions#castBlock} — the same
 *       engine (and therefore the same values and the same policy-aware failure behavior:
 *       warn+null by default, fail the read under {@code fail_fast}) as the eager decode paths
 *       in {@link ParquetFormatReader}. Unlike the eager paths nothing is
 *       fused into the decode loops here, so every non-identity supported pair coerces this way;
 *       a pair {@link DeclaredTypeCoercions#supports} rejects (a drifted glob file) reads as
 *       all-null with a response Warning, mirroring the eager per-file validation.</li>
 * </ol>
 * <p>
 * I/O cost: one async coalesced fetch per visited row group, all dispatched in parallel up to
 * the per-query concurrency budget. For TopN with {@code limit = N} survivors against a file
 * with {@code G} row groups and a {@code C}-column deferred projection, at most {@code min(N, G)}
 * row groups are visited and roughly {@code min(N, G)} HTTP GETs fly concurrently rather than
 * {@code min(N, G) × C} sequentially. End-to-end latency is bounded below by
 * {@code max(slowest_RTT, fastest_RTT + total_decode_time)} instead of
 * {@code slowest_RTT + total_decode_time}.
 * <p>
 * Threading: a single driver thread owns an instance; callers must not invoke {@link #extract}
 * concurrently from different threads.
 */
final class ParquetColumnExtractor implements ColumnExtractor {

    private final StorageObject storageObject;
    private final ParquetFormatReader reader;
    private final ParquetMetadata ownedFooter;
    /** See {@link #castBlockWarnings()}. */
    private final ErrorPolicy errorPolicy;
    private final long rowCount;
    /**
     * Precomputed prefix sum of row counts over {@link #ownedFooter}'s blocks, with a leading
     * zero — i.e. {@code rowGroupOffsets[i]} is the file-global first row index of row group
     * {@code i}, and {@code rowGroupOffsets[ownedFooter.getBlocks().size()] == rowCount}. Used by
     * {@link #findRowGroup(long)} to map a file-global row identity to its owning block in
     * O(log G). Always derived from the full file footer (range-split iterators emit identities
     * in the file's address space; the extractor does not need or want a subset view).
     */
    private final long[] rowGroupOffsets;

    /**
     * @param storageObject the storage handle for the file (extractor borrows it; caller closes)
     * @param reader        a {@link ParquetFormatReader} to use for codec-factory access; the
     *                      extractor never opens iterators through it
     * @param ownedFooter   the {@link ParquetMetadata} for the file. Always the full footer —
     *                      iterators (full-file or range-split) emit file-global row identities,
     *                      so the extractor's address space matches the file rather than any
     *                      split slice.
     * @param errorPolicy   the read's error policy, inherited from the iterator that produced the
     *                      row identities so the deferred columns fail (or warn+null) exactly like
     *                      the eagerly scanned ones
     */
    ParquetColumnExtractor(StorageObject storageObject, ParquetFormatReader reader, ParquetMetadata ownedFooter, ErrorPolicy errorPolicy) {
        this.storageObject = Objects.requireNonNull(storageObject, "storageObject");
        this.reader = Objects.requireNonNull(reader, "reader");
        this.ownedFooter = Objects.requireNonNull(ownedFooter, "ownedFooter");
        this.errorPolicy = Objects.requireNonNull(errorPolicy, "errorPolicy");
        List<BlockMetaData> blocks = ownedFooter.getBlocks();
        this.rowGroupOffsets = new long[blocks.size() + 1];
        long acc = 0;
        for (int i = 0; i < blocks.size(); i++) {
            rowGroupOffsets[i] = acc;
            acc += blocks.get(i).getRowCount();
        }
        rowGroupOffsets[blocks.size()] = acc;
        this.rowCount = acc;
    }

    @Override
    public long rowCount() {
        return rowCount;
    }

    @Override
    public Block[] extract(String[] columnNames, @Nullable DataType[] targetTypes, long[] localPositions, BlockFactory blockFactory)
        throws IOException {
        Objects.requireNonNull(columnNames, "columnNames");
        Objects.requireNonNull(localPositions, "localPositions");
        Objects.requireNonNull(blockFactory, "blockFactory");
        for (String columnName : columnNames) {
            Objects.requireNonNull(columnName, "columnNames must not contain nulls");
            if (columnName.equals(ROW_POSITION_COLUMN)) {
                throw new IllegalArgumentException("cannot extract reserved column [" + ROW_POSITION_COLUMN + "]");
            }
        }
        if (targetTypes != null && targetTypes.length != columnNames.length) {
            throw new IllegalArgumentException(
                "targetTypes length [" + targetTypes.length + "] must match columnNames length [" + columnNames.length + "]"
            );
        }

        int colCount = columnNames.length;
        int count = localPositions.length;
        if (count == 0) {
            Block[] empty = new Block[colCount];
            for (int c = 0; c < colCount; c++) {
                empty[c] = blockFactory.newConstantNullBlock(0);
            }
            return empty;
        }
        if (colCount == 0) {
            return new Block[0];
        }
        // The per-bucket {@link Bucket#sortAndDedup} packs each input slot index into the low
        // {@link #INDEX_BITS} of a long; if the input batch ever exceeds {@link #INDEX_RANGE},
        // packing collapses neighbouring slots into the same packed key and the sort silently
        // corrupts. The optimizer caps the deferred-extraction TopN limit well below this bound
        // today, but enforce the local invariant here so the contract surfaces if either side ever
        // moves.
        if (count > INDEX_RANGE) {
            throw new IllegalArgumentException(
                "extract requested for [" + count + "] positions exceeds packed-sort capacity [" + INDEX_RANGE + "]"
            );
        }

        validatePositions(localPositions, count);

        // Resolve every requested column to its {@link ColumnInfo} once so we have the descriptors
        // ready for the single coalesced fetch (we need their dotted paths to compute the byte
        // ranges) and for the per-column decode loop afterwards.
        MessageType schema = ownedFooter.getFileMetaData().getSchema();
        ColumnInfo[] infos = new ColumnInfo[colCount];
        for (int c = 0; c < colCount; c++) {
            ColumnInfo info = ParquetFormatReader.resolveColumnInfo(schema, columnNames[c]);
            if (info == null) {
                throw new IllegalArgumentException(
                    "column [" + columnNames[c] + "] is missing or has an unsupported type in [" + storageObject.path() + "]"
                );
            }
            infos[c] = info;
        }

        // Bucket positions by row group once. Per-column decode walks the same buckets, so paying
        // the bucketing cost once across the whole projection is significantly cheaper than
        // re-bucketing per column (the bucket structure is column-agnostic; the in-row-group
        // positions are the same regardless of which column we materialise).
        List<Bucket> buckets = bucketByRowGroup(localPositions, count);

        // Sort+dedup every bucket up-front (independent of decode order). This lets the
        // arrival-order decode loop call PageColumnReader.readBatchSparse without any
        // bucket-vs-bucket coordination.
        for (Bucket bucket : buckets) {
            bucket.sortAndDedup();
        }

        // Projection set keyed on each column descriptor's dotted path. For flat columns this is
        // just the column name; for LIST<primitive> it's e.g. "vals.list.element". This must match
        // ColumnChunkMetaData.getPath().toDotString(), which is what
        // ColumnChunkPrefetcher.computeColumnChunkRanges and PrefetchedRowGroupBuilder both key
        // off. The combined projection covers every requested column for one row group;
        // CoalescedRangeReader merges physically-adjacent chunks (column chunks within one row
        // group are written contiguously in Parquet), so the per-bucket fetch typically resolves
        // to a single S3 GET per row group rather than one per (row group, column).
        Set<String> projection = new java.util.LinkedHashSet<>(colCount);
        for (ColumnInfo info : infos) {
            projection.add(String.join(".", info.descriptor().getPath()));
        }

        // Per-bucket async prefetch dispatch. We do NOT block on any one fetch before kicking
        // off the next: every bucket's GET is in flight before we touch the first byte. This is
        // the parallelism win — for N visited row groups end-to-end latency drops from
        // (slowest GET) + (sum of decodes) towards max(slowest GET, fastest GET + sum of decodes),
        // bounded by the per-query concurrency budget which sits inside readBytesAsync.
        List<BlockMetaData> blocks = ownedFooter.getBlocks();
        @SuppressWarnings("unchecked")
        CompletableFuture<ColumnChunkPrefetcher.PrefetchedChunks>[] futures = (CompletableFuture<
            ColumnChunkPrefetcher.PrefetchedChunks>[]) new CompletableFuture<?>[buckets.size()];
        for (int i = 0; i < buckets.size(); i++) {
            BlockMetaData block = blocks.get(buckets.get(i).rowGroupIndex);
            futures[i] = ColumnChunkPrefetcher.prefetchAsync(storageObject, block, projection, blockFactory.arrowAllocator());
        }

        // result[c][b] = block for column c in bucket b (bucket-visit order). We populate this
        // out of order as buckets' prefetches arrive; the per-column concat happens once every
        // bucket has been decoded so the layout-vs-arrival distinction is local to this method.
        Block[][] perBucketBlocks = new Block[colCount][buckets.size()];
        Block[] result = new Block[colCount];
        boolean built = false;
        try {
            decodeBucketsAsTheyArrive(buckets, infos, schema, columnNames, projection, futures, perBucketBlocks, blockFactory);
            for (int c = 0; c < colCount; c++) {
                // stitchAndGather releases its per-bucket blocks and nulls the array entries so
                // the outer defensive cleanup is a no-op for already-stitched columns.
                Block stitched = stitchAndGather(perBucketBlocks[c], buckets, count, blockFactory);
                result[c] = stitched;
                DataType target = targetTypes == null ? null : targetTypes[c];
                if (target != null && target != infos[c].esqlType()) {
                    // coerceToTarget consumes `stitched` (its finally closes it), so clear the
                    // slot first — a throw mid-coercion must not double-close via the outer
                    // defensive cleanup.
                    result[c] = null;
                    result[c] = coerceToTarget(stitched, infos[c].esqlType(), target, columnNames[c], count, blockFactory);
                }
            }
            built = true;
            return result;
        } finally {
            // Defensive cleanup: anything left in perBucketBlocks (e.g. when decode partially
            // completed before failing, or stitchAndGather threw between two columns) needs
            // releasing. Built columns in result are released only on the failure path.
            for (Block[] perBucket : perBucketBlocks) {
                for (Block b : perBucket) {
                    if (b != null) {
                        Releasables.closeExpectNoException(b);
                    }
                }
            }
            if (built == false) {
                Releasables.closeExpectNoException(result);
            }
        }
    }

    /**
     * Drains the per-bucket prefetch futures in arrival order, decoding every requested column
     * for each bucket as soon as its bytes land. Decode of bucket {@code i} therefore overlaps
     * with the still in-flight S3 reads for the slower buckets — the wall-clock cost of the
     * slowest GET is no longer additive with decode time.
     *
     * <p>If any prefetch fails, every other in-flight prefetch is cancelled (or its result
     * discarded once it lands) and the original failure is rethrown.
     */
    private void decodeBucketsAsTheyArrive(
        List<Bucket> buckets,
        ColumnInfo[] infos,
        MessageType schema,
        String[] columnNames,
        Set<String> projection,
        CompletableFuture<ColumnChunkPrefetcher.PrefetchedChunks>[] futures,
        Block[][] perBucketBlocks,
        BlockFactory blockFactory
    ) throws IOException {
        // Per-column projected schemas + single-column projection sets reused across every
        // bucket's decode. Building these once outside the drain loop keeps the per-bucket fast
        // path branch-free and avoids re-walking the message schema for each column-bucket pair.
        int colCount = columnNames.length;
        MessageType[] perColumnSchemas = new MessageType[colCount];
        @SuppressWarnings("unchecked")
        Set<String>[] perColumnProjections = (Set<String>[]) new Set<?>[colCount];
        for (int c = 0; c < colCount; c++) {
            // Use the descriptor's first path segment as the top-level field name. For flat
            // columns and LIST<primitive> this equals columnNames[c]; for dotted struct-leaf
            // columns (e.g. "event.action") columnNames[c] is not a top-level schema field and
            // schema.getType(columnNames[c]) would throw.
            perColumnSchemas[c] = new MessageType(schema.getName(), schema.getType(infos[c].descriptor().getPath()[0]));
            perColumnProjections[c] = Set.of(String.join(".", infos[c].descriptor().getPath()));
        }
        String createdBy = ownedFooter.getFileMetaData().getCreatedBy();
        List<BlockMetaData> blocks = ownedFooter.getBlocks();

        // Use CompletableFuture.anyOf in a draining loop so we always pick the next-completed
        // future. Track which slots are still pending via a mutable view over the futures array
        // — completed slots get nulled out and skipped on subsequent iterations.
        int pending = futures.length;
        try {
            while (pending > 0) {
                int bucketIdx = waitForNextCompleted(futures);
                ColumnChunkPrefetcher.PrefetchedChunks prefetchedResult;
                try {
                    prefetchedResult = futures[bucketIdx].get();
                } catch (ExecutionException e) {
                    // Unwrap the CompletableFuture wrapper; the underlying cause is what callers
                    // expect (e.g. an IOException from S3) and what existing tests assert against.
                    Throwable cause = e.getCause();
                    if (cause instanceof IOException io) {
                        throw io;
                    }
                    if (cause instanceof RuntimeException re) {
                        throw re;
                    }
                    throw new IOException("prefetch failed for row group " + buckets.get(bucketIdx).rowGroupIndex, cause);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException("interrupted while awaiting row group prefetch", e);
                }
                futures[bucketIdx] = null;
                pending--;

                Bucket bucket = buckets.get(bucketIdx);
                BlockMetaData block = blocks.get(bucket.rowGroupIndex);
                int rgRowCount = Math.toIntExact(block.getRowCount());

                try {
                    // Decode every requested column from this bucket's prefetched chunk map. Each
                    // call passes a single-column projection so PrefetchedRowGroupBuilder only
                    // materialises that column's bytes despite the prefetched map carrying every
                    // projected column for this row group.
                    for (int c = 0; c < colCount; c++) {
                        perBucketBlocks[c][bucketIdx] = decodeBucket(
                            bucket,
                            block,
                            infos[c],
                            perColumnSchemas[c],
                            perColumnProjections[c],
                            prefetchedResult.chunks(),
                            rgRowCount,
                            createdBy,
                            blockFactory
                        );
                    }
                } finally {
                    // Release this bucket's prefetched buffers as soon as the loop exits — the
                    // decoded values now live in perBucketBlocks and the raw bytes are no longer
                    // referenced.
                    prefetchedResult.release().close();
                }
            }
        } catch (Throwable t) {
            // Cancel every still-pending prefetch and release any buffers that already landed so
            // the failure path leaves no breaker reservation outstanding.
            for (int i = 0; i < futures.length; i++) {
                CompletableFuture<ColumnChunkPrefetcher.PrefetchedChunks> f = futures[i];
                if (f == null) {
                    continue;
                }
                FutureUtils.cancel(f);
                ColumnChunkPrefetcher.PrefetchedChunks landed;
                try {
                    landed = f.getNow(null);
                } catch (CompletionException | CancellationException ignored) {
                    // Future completed exceptionally (or was cancelled) — no chunks were ever
                    // produced so there is nothing for us to release.
                    continue;
                }
                if (landed != null) {
                    try {
                        landed.release().close();
                    } catch (Throwable releaseFailure) {
                        // Surface release failures (e.g. ArrowBuf double-decrement) as suppressed
                        // exceptions on the original error so they don't mask the root cause and
                        // we still drain the rest of the prefetched chunks.
                        t.addSuppressed(releaseFailure);
                    }
                }
            }
            throw t;
        }
    }

    /**
     * Polls the futures array for any completed slot, returning its index. Blocks on
     * {@link CompletableFuture#anyOf} until at least one new future completes; subsequent
     * scans are O(N) in the per-call array length, which is bounded by the number of
     * surviving row groups (typically &lt; 32).
     */
    private static int waitForNextCompleted(CompletableFuture<?>[] futures) {
        // Fast path: a future from a previous round may already be done. Scan first.
        int idx = findCompleted(futures);
        if (idx >= 0) {
            return idx;
        }
        // Build a snapshot of still-pending futures and wait for one to complete.
        List<CompletableFuture<?>> pendingList = new ArrayList<>(futures.length);
        for (CompletableFuture<?> f : futures) {
            if (f != null) {
                pendingList.add(f);
            }
        }
        if (pendingList.isEmpty()) {
            throw new IllegalStateException("waitForNextCompleted called with no pending futures");
        }
        // anyOf blocks until any future completes (success or failure). We don't use anyOf's
        // result; we re-scan for the now-completed slot to recover the bucket index. That keeps
        // the index ↔ bucket binding without a parallel HashMap lookup.
        try {
            CompletableFuture.anyOf(pendingList.toArray(CompletableFuture[]::new)).join();
        } catch (CompletionException ignored) {
            // anyOf surfaces the first failure; ignore here — the caller's get() on the failed
            // future will rethrow the underlying cause with proper unwrapping.
        }
        idx = findCompleted(futures);
        if (idx < 0) {
            // Defensive: if anyOf returned but the array shows no completed entry it means the
            // future we waited on was a stale reference. Re-loop on the caller side.
            throw new IllegalStateException("anyOf signalled completion but no future is done");
        }
        return idx;
    }

    private static int findCompleted(CompletableFuture<?>[] futures) {
        for (int i = 0; i < futures.length; i++) {
            CompletableFuture<?> f = futures[i];
            if (f != null && f.isDone()) {
                return i;
            }
        }
        return -1;
    }

    /**
     * Concatenates per-bucket blocks for one column in bucket-visit order and applies the gather
     * permutation that maps caller slots to their position in the concatenated block. This is
     * the same logic the previous synchronous path used; we just feed it pre-decoded per-bucket
     * blocks instead of decoding inside the loop.
     */
    private Block stitchAndGather(Block[] perBucketBlocks, List<Bucket> buckets, int totalCount, BlockFactory blockFactory) {
        Block concatenated = null;
        try {
            if (perBucketBlocks.length == 0) {
                throw new IllegalStateException("no row groups visited for [" + storageObject.path() + "] (count=" + totalCount + ")");
            }
            for (int b = 0; b < perBucketBlocks.length; b++) {
                if (perBucketBlocks[b] == null) {
                    throw new IllegalStateException("missing decoded block for bucket [" + buckets.get(b).rowGroupIndex + "]");
                }
            }
            // The shared helper resolves the element type from the first non-NULL bucket block, so
            // an all-null leading bucket cannot poison a ConstantNullBlock builder. It closes the
            // per-bucket blocks on success; null the slots so the finally below and the caller's
            // defensive cleanup do not double-close. On a throw it leaves them for the finally.
            concatenated = BlockChunks.concat(Arrays.asList(perBucketBlocks), blockFactory);
            Arrays.fill(perBucketBlocks, null);
            int[] gather = buildGatherPermutation(buckets, totalCount);
            // mayContainDuplicates is true: the same bucket position may serve multiple caller
            // slots when localPositions repeats a row.
            return concatenated.filter(true, gather);
        } finally {
            if (concatenated != null) {
                Releasables.closeExpectNoException(concatenated);
            }
            // Per-bucket blocks live until stitch completes; release any still-present slot here so
            // the caller doesn't have to track them. Null the slots out so the caller's defensive
            // cleanup doesn't double-close.
            for (int i = 0; i < perBucketBlocks.length; i++) {
                Block b = perBucketBlocks[i];
                if (b != null) {
                    Releasables.closeExpectNoException(b);
                    perBucketBlocks[i] = null;
                }
            }
        }
    }

    /**
     * Coerces one stitched physical-type block to the caller's target (planner/declared) type
     * through {@link DeclaredTypeCoercions#castBlock} — the identical engine, formatter (the
     * column's declared date {@code format} via
     * {@link ParquetFormatReader#declaredDateFormatterFor}), and policy-aware failure behavior
     * ({@link #castBlockWarnings}) as the eager decode paths, so a deferred column reads exactly
     * like an eagerly scanned one — including the declared-vs-inferred null-fill decision: the lossy
     * {@link DeclaredTypeCoercions#supports} escape (which admits narrowing) is honored only for a
     * DECLARED column, mirroring {@code validatePlannerTypesAgainstFile}. An inferred target may only
     * widen ({@link ParquetFormatReader#plannerTypeCompatibleWithFileDerivedType}); a narrowing or a
     * drifted-glob pair reads the column as all-null rather than downcasting (which would disagree
     * with the eager scan of the same file). Always consumes {@code physical} and returns a fresh
     * caller-owned block.
     */
    private Block coerceToTarget(Block physical, DataType fileType, DataType target, String columnName, int count, BlockFactory factory) {
        try {
            // Same gate as the eager reader (validatePlannerTypesAgainstFile). castBlock only functions on a supports()
            // pair, so that stays the precondition; on top of it the lossy narrowing it admits is licensed only for a
            // DECLARED column — an inferred target may coerce only when the pair also widens. So an inferred narrowing
            // (supports true, not widening, not declared) null-fills instead of downcasting, agreeing with the eager
            // scan of the same file. columnName is physical here (the extractor projects physical names), matching the set.
            boolean coercible = DeclaredTypeCoercions.supports(fileType, target)
                && (ParquetFormatReader.plannerTypeCompatibleWithFileDerivedType(target, fileType)
                    || reader.isDeclaredTypeColumn(columnName));
            if (coercible) {
                return DeclaredTypeCoercions.castBlock(
                    physical,
                    fileType,
                    target,
                    reader.declaredDateFormatterFor(columnName),
                    factory,
                    columnName,
                    castBlockWarnings()
                );
            }
            coercionWarnings().add(
                "Column ["
                    + columnName
                    + "] in file ["
                    + storageObject.path()
                    + "] has type ["
                    + fileType
                    + "] incompatible with planner type ["
                    + target
                    + "]; returning nulls for this column"
            );
            return factory.newConstantNullBlock(count);
        } finally {
            physical.close();
        }
    }

    /**
     * Lazily-created sink for per-value declared-coercion failures (capped response Warning
     * headers + nulled cells); shared by every column and {@link #extract} call of this extractor
     * so the cap is per deferred-extraction pass, mirroring the per-iterator sink the eager scan
     * paths keep ({@code ParquetColumnIterator#coercionWarnings()}). Single driver thread owns
     * the instance (see the class Javadoc threading note).
     */
    private SkipWarnings coercionWarnings;

    private SkipWarnings coercionWarnings() {
        if (coercionWarnings == null) {
            coercionWarnings = new SkipWarnings(
                "Parquet file ["
                    + storageObject.path()
                    + "] has values that could not be coerced to the declared column type; they are returned as null"
            );
        }
        return coercionWarnings;
    }

    /**
     * The per-value coercion-failure sink handed to {@code castBlock}, or {@code null} under
     * {@code fail_fast} so the failure propagates — identical to the eager iterators'
     * policy-aware {@code coercionWarnings()}. The drifted-glob whole-column-null fallback in
     * {@link #coerceToTarget} stays on the always-live {@link #coercionWarnings()} regardless of
     * policy, mirroring the eager per-file validation which also warns+nulls under every policy.
     */
    @Nullable
    private SkipWarnings castBlockWarnings() {
        return errorPolicy.isStrict() ? null : coercionWarnings();
    }

    private void validatePositions(long[] localPositions, int count) {
        long min = localPositions[0];
        long max = localPositions[0];
        for (int i = 1; i < count; i++) {
            long p = localPositions[i];
            if (p < min) min = p;
            if (p > max) max = p;
        }
        if (min < 0 || max >= rowCount) {
            throw new IllegalArgumentException("row position out of range [0, " + rowCount + "): min=" + min + ", max=" + max);
        }
    }

    /**
     * Group every requested position into a per-row-group {@link Bucket}. Each bucket stores the
     * caller slot ({@code originalIndex}) and the in-row-group position for every entry; the
     * order of insertion is the order positions appear in {@code localPositions}, so the
     * concatenated output later in {@link #stitchAndGather} preserves that ordering and the
     * gather permutation we build inline maps caller slot directly into concatenated space.
     */
    private List<Bucket> bucketByRowGroup(long[] localPositions, int count) {
        Map<Integer, Bucket> byRg = new HashMap<>();
        List<Bucket> ordered = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            long pos = localPositions[i];
            int rgIndex = findRowGroup(pos);
            Bucket b = byRg.get(rgIndex);
            if (b == null) {
                b = new Bucket(rgIndex);
                byRg.put(rgIndex, b);
                ordered.add(b);
            }
            int inGroup = Math.toIntExact(pos - rowGroupOffsets[rgIndex]);
            b.add(i, inGroup);
        }
        return ordered;
    }

    /**
     * Builds the permutation {@code gather} where {@code gather[callerSlot]} is the position in
     * the concatenated unique-position block (whose layout is {@code bucket0 ++ bucket1 ++ …} in
     * visit order) holding the value for that caller slot. Combines the per-bucket concatenation
     * offset with each entry's {@code runToBucketSlot} index to produce every gather index in one
     * pass.
     */
    private static int[] buildGatherPermutation(List<Bucket> buckets, int totalCount) {
        int[] gather = new int[totalCount];
        int offset = 0;
        for (Bucket b : buckets) {
            int n = b.size;
            for (int i = 0; i < n; i++) {
                int callerSlot = b.originalIndex[i];
                int posInBucket = b.runToBucketSlot[i];
                gather[callerSlot] = offset + posInBucket;
            }
            offset += b.uniqueCount;
        }
        return gather;
    }

    /**
     * Decodes the surviving rows for one row group, dispatching to the flat (rep-level 0) or
     * list (rep-level &gt; 0) sparse-read path. Returns a block with {@code bucket.uniqueCount}
     * positions ordered by ascending in-row-group position; the caller stitches per-bucket blocks
     * into the concatenated output and uses the gather permutation to route caller slots and replay
     * duplicates.
     */
    private Block decodeBucket(
        Bucket bucket,
        BlockMetaData block,
        ColumnInfo info,
        MessageType projectedSchema,
        Set<String> projection,
        NavigableMap<Long, ColumnChunkPrefetcher.PrefetchedChunk> prefetched,
        int rgRowCount,
        String createdBy,
        BlockFactory blockFactory
    ) throws IOException {
        try (
            PrefetchedPageReadStore store = PrefetchedRowGroupBuilder.build(
                block,
                bucket.rowGroupIndex,
                projectedSchema,
                projection,
                /* rowRanges = */ null,
                /* preloadedMetadata = */ null,
                prefetched,
                storageObject,
                reader.codecFactory(),
                blockFactory.arrowAllocator()
            )
        ) {
            if (info.maxRepLevel() == 0) {
                return decodeFlat(bucket, info, store, rgRowCount, blockFactory);
            }
            return decodeList(bucket, info, store, projectedSchema, rgRowCount, createdBy, blockFactory);
        }
    }

    /**
     * Flat-column sparse decode via {@link PageColumnReader#readBatchSparse}. The reader walks
     * the row-group rows in source order, alternating skips and reads to produce exactly the
     * surviving rows.
     */
    private static Block decodeFlat(
        Bucket bucket,
        ColumnInfo info,
        PrefetchedPageReadStore store,
        int rgRowCount,
        BlockFactory blockFactory
    ) {
        PageReader pr = store.getPageReader(info.descriptor());
        try (
            PageColumnReader pageReader = new PageColumnReader(
                pr,
                info.descriptor(),
                info,
                // RowRanges.all() lets every page through loadNextPage()'s page-skip check; the
                // sparse loop drives skip/read by in-group position from there.
                RowRanges.all(rgRowCount)
            )
        ) {
            return pageReader.readBatchSparse(rgRowCount, blockFactory, bucket.uniquePositions, bucket.uniqueCount);
        }
    }

    /**
     * List-column sparse decode. parquet-mr's {@link ColumnReader} is repetition-aware so we
     * walk the row-group rows in source order via {@link #skipListRows} (a sparse row-granular
     * skip — {@code ParquetColumnDecoding#skipListValues} skips values, not rows, and would leave
     * the data cursor stale here) and {@link ParquetColumnDecoding#readListColumn}, alternating skip/read runs in lock-step
     * with the unique survivor positions. Same in-memory chunks as the flat path; we only swap
     * the decoder.
     */
    private Block decodeList(
        Bucket bucket,
        ColumnInfo info,
        PrefetchedPageReadStore store,
        MessageType projectedSchema,
        int rgRowCount,
        String createdBy,
        BlockFactory blockFactory
    ) {
        ColumnReadStoreImpl crs = new ColumnReadStoreImpl(
            store,
            new ParquetColumnDecoding.NoOpGroupConverter(projectedSchema),
            projectedSchema,
            createdBy != null ? createdBy : ""
        );
        ColumnReader cr = crs.getColumnReader(info.descriptor());

        int unique = bucket.uniqueCount;
        int[] positions = bucket.uniquePositions;
        // Decompose into contiguous runs the same way PageColumnReader does for the flat path;
        // each run yields one readListColumn call against ColumnReader, and gaps between runs
        // are handled by skipListValues. List-read is unavoidably row-by-row at the rep-level
        // boundary, so the runs help amortise the per-row builder begin/endPositionEntry cost.
        List<Block> chunks = new ArrayList<>();
        int cursor = 0;
        int runStart = 0;
        int maxDef = info.maxDefLevel();
        try {
            while (runStart < unique) {
                int p = positions[runStart];
                int gap = p - cursor;
                if (gap > 0) {
                    skipListRows(cr, maxDef, gap);
                    cursor += gap;
                }
                int runEnd = runStart + 1;
                while (runEnd < unique && positions[runEnd] == positions[runEnd - 1] + 1) {
                    runEnd++;
                }
                int runLength = runEnd - runStart;
                chunks.add(ParquetColumnDecoding.readListColumn(cr, info, runLength, blockFactory));
                cursor += runLength;
                runStart = runEnd;
            }
            // No need to drain trailing rows: ColumnReader is bounded by the row group's chunk
            // and we discard it at end of try-with-resources without consuming further values.
            if (chunks.size() == 1) {
                Block only = chunks.get(0);
                chunks.clear();
                return only;
            }
            // BlockChunks.concat closes the run chunks on success; clear the list so the catch
            // below does not double-close. On a throw it leaves them for the catch to release.
            Block joined = BlockChunks.concat(chunks, blockFactory);
            chunks.clear();
            return joined;
        } catch (RuntimeException e) {
            for (Block c : chunks) {
                Releasables.closeExpectNoException(c);
            }
            throw e;
        }
    }

    /**
     * Skip {@code rowsToSkip} entire list rows, advancing both the column reader's level cursor
     * and its data cursor. {@link ParquetColumnDecoding#skipListValues} only calls
     * {@link ColumnReader#consume}, which advances rep/def levels but leaves the underlying data
     * cursor at the first value — fine when the column is read sequentially row-by-row (each
     * read calls {@code getXxx()} or appendNull and the cursors stay in lock-step) but wrong for
     * a sparse skip ahead of a {@link ParquetColumnDecoding#readListColumn} call: the next
     * {@code getInteger()} would return a stale value from row 0. We therefore call
     * {@link ColumnReader#skip} for every value with {@code def == maxDef} (the only values that
     * are physically present in the page) so the data cursor advances in lock-step with the
     * level cursor.
     */
    private static void skipListRows(ColumnReader cr, int maxDef, int rowsToSkip) {
        for (int row = 0; row < rowsToSkip; row++) {
            if (cr.getCurrentDefinitionLevel() == maxDef) {
                cr.skip();
            }
            cr.consume();
            while (cr.getCurrentRepetitionLevel() > 0) {
                if (cr.getCurrentDefinitionLevel() == maxDef) {
                    cr.skip();
                }
                cr.consume();
            }
        }
    }

    @Override
    public void close() {
        // The StorageObject is owned by the caller (source factory). The extractor borrows it; the
        // caller closes it when the driver finishes. The cached footer is just metadata — no
        // resources to release.
    }

    /**
     * Binary-searches {@link #rowGroupOffsets} for the row group that owns {@code position}.
     * Returns an index into {@code ownedFooter.getBlocks()}.
     */
    private int findRowGroup(long position) {
        // rowGroupOffsets[i] is the first row of block i; blocks span [offsets[i], offsets[i+1]).
        // binarySearch on a strictly-increasing prefix sum: when found, position is the first row
        // of the block with that offset; otherwise the negative insertion point identifies the
        // first offset > position, and the owning block is one before that.
        int idx = Arrays.binarySearch(rowGroupOffsets, 0, rowGroupOffsets.length - 1, position);
        if (idx >= 0) {
            return idx;
        }
        return (-idx - 1) - 1;
    }

    /**
     * Per-row-group accumulator. Holds the caller slots and in-row-group positions for every
     * request that lands in this bucket, in input order. After {@link #sortAndDedup} the bucket
     * also exposes {@link #uniquePositions} (strictly-ascending) and {@link #runToBucketSlot}
     * (per-input-slot index into the per-bucket output block).
     */
    private static final class Bucket {
        final int rowGroupIndex;
        int[] originalIndex = new int[4];
        int[] inGroupPositions = new int[4];
        int size = 0;

        // Populated by sortAndDedup:
        int[] uniquePositions;
        int uniqueCount;
        /**
         * For sorted input slot {@code i}, the index in the per-bucket unique-position output
         * block holding the value for that input. When the bucket has no duplicates this is the
         * identity {@code [0, uniqueCount)}; with duplicates it routes each duplicate to its
         * single decoded value.
         */
        int[] runToBucketSlot;

        Bucket(int rowGroupIndex) {
            this.rowGroupIndex = rowGroupIndex;
        }

        void add(int callerSlot, int inGroupPos) {
            if (size == originalIndex.length) {
                int newCap = originalIndex.length * 2;
                originalIndex = Arrays.copyOf(originalIndex, newCap);
                inGroupPositions = Arrays.copyOf(inGroupPositions, newCap);
            }
            originalIndex[size] = callerSlot;
            inGroupPositions[size] = inGroupPos;
            size++;
        }

        /**
         * Sorts (inGroupPosition, callerSlot) pairs ascending by position, then deduplicates
         * adjacent equal positions. After this call:
         * <ul>
         *   <li>{@link #uniquePositions}/{@link #uniqueCount} satisfy
         *       {@code PageColumnReader.readBatchSparse}'s strictly-ascending contract.</li>
         *   <li>{@link #runToBucketSlot} maps each (now-sorted) entry to its slot in the
         *       per-bucket output block produced by the per-RG decode.</li>
         *   <li>{@link #originalIndex} is reordered in lockstep with the sort so the gather
         *       step in {@link ParquetColumnExtractor#buildGatherPermutation} maps caller slots
         *       to {@code runToBucketSlot} positions correctly.</li>
         * </ul>
         */
        void sortAndDedup() {
            // Pack (position, callerSlot) into a single long to drive a primitive sort. {@link #size}
            // is bounded by the input {@code localPositions.length} which {@link #extract} caps at
            // {@link #INDEX_RANGE} on entry, so the slot half of every packed long fits in
            // {@link #INDEX_BITS} unambiguously.
            assert size <= INDEX_RANGE : "bucket size [" + size + "] exceeds packed-sort capacity [" + INDEX_RANGE + "]";
            long[] packed = new long[size];
            for (int i = 0; i < size; i++) {
                // inGroupPositions are non-negative ints, so the high 49 bits hold the position
                // unsigned and Arrays.sort gives ascending position order.
                packed[i] = ((long) inGroupPositions[i] << INDEX_BITS) | (originalIndex[i] & INDEX_MASK);
            }
            Arrays.sort(packed);
            int[] sortedOriginal = new int[size];
            int[] sortedPositions = new int[size];
            for (int i = 0; i < size; i++) {
                sortedPositions[i] = (int) (packed[i] >>> INDEX_BITS);
                sortedOriginal[i] = (int) (packed[i] & INDEX_MASK);
            }
            originalIndex = sortedOriginal;
            inGroupPositions = sortedPositions;

            uniquePositions = new int[size];
            runToBucketSlot = new int[size];
            int u = 0;
            int prev = -1;
            for (int i = 0; i < size; i++) {
                int p = sortedPositions[i];
                if (i == 0 || p != prev) {
                    uniquePositions[u++] = p;
                    prev = p;
                }
                runToBucketSlot[i] = u - 1;
            }
            uniqueCount = u;
        }
    }

    /**
     * Bit budget for the packed (position, slot) sort: {@link #INDEX_BITS} for the slot,
     * {@code 64 - INDEX_BITS} for the position. The slot half must fit every input slot index, so
     * {@link #extract} caps {@code localPositions.length} at {@link #INDEX_RANGE} on entry — that
     * upper bound (16_384) is the local invariant of this packed-sort scheme and is independent of
     * any caller-side TopN limit. The remaining 49 bits address Parquet files up to roughly 500
     * trillion rows, well past anything realistic.
     */
    private static final int INDEX_BITS = 14;
    private static final int INDEX_RANGE = 1 << INDEX_BITS;
    private static final long INDEX_MASK = INDEX_RANGE - 1L;
}
