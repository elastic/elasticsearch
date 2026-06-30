/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.datasources.spi.ColumnExtractor;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Driver-scoped lookup table of per-file {@link ColumnExtractor}s, addressed by a compact
 * {@code int} id allocated at registration time. Acts as the rendezvous between an
 * {@code AsyncExternalSourceOperatorFactory} (which {@link #register registers} a new extractor
 * each time it opens a file) and an {@code ExternalFieldExtractOperator} (which decodes the
 * synthetic {@code _rowPosition} carried on pages and {@link #materialize routes} extraction
 * requests to the correct extractor).
 *
 * <h2>{@code _rowPosition} encoding</h2>
 * The synthetic column flowing between source and extract carries a {@code long} per row that
 * packs the extractor id and the file-local position into a single value:
 * <pre>
 *   bit  63       reserved zero (keeps the encoded value non-negative for signed long order)
 *   bits 62..48   extractor id (15 bits, 0..32767)
 *   bits 47..0    file-local row position (48 bits, 0..2^48-1)
 * </pre>
 * Concretely: {@code encoded = ((long) id << 48) | (localPos & MAX_LOCAL_POSITION)}. This keeps
 * the row identifier a primitive {@code long}, so the existing {@code LongBlock} type carries it
 * end-to-end without any new block-type plumbing. Bit 63 is deliberately reserved so every
 * encoded value is non-negative; that way Java's signed-long ordering (used by ESQL's TopN
 * encoder, which calls {@code NumericUtils.longToSortableBytes}) coincides with
 * {@code (id, localPos)} lexicographic order, and the encoding remains usable as a stable
 * tiebreaker should anything downstream sort by it.
 *
 * <h2>Lifecycle</h2>
 * One {@code SourceExtractors} per driver. The source operator registers extractors during the
 * async producer loop; the extract operator reads from it on the driver thread. Both sides hold
 * the same instance via {@code AsyncExternalSourceOperatorFactory#sourceExtractorsFor(DriverContext)}.
 * The extract operator owns {@link #close}; closing is idempotent.
 *
 * <h2>Threading</h2>
 * {@link #register} is invoked from source executor threads (one at a time per driver; the source
 * loop is single-producer within a driver). {@link #get} and {@link #materialize} are invoked
 * from the driver thread. The data-flow invariant — the source publishes a page only after the
 * file's extractor has been registered — establishes the necessary happens-before via the page
 * buffer's internal synchronization. We use a synchronized {@link ArrayList} as a defensive
 * choice; the cost is negligible for the few-dozen registrations per driver in practice.
 */
public final class SourceExtractors implements Releasable {

    /**
     * Number of bits reserved for the file-local row position; the rest go to the extractor id.
     * Mirrors {@link ColumnExtractor#LOCAL_POSITION_BITS} (the SPI-side constant iterator
     * implementations OR against when they pre-encode {@code _rowPosition} values); kept aligned
     * via the {@code assert} below so a divergent edit surfaces in tests instead of as a wire-
     * format mismatch.
     */
    public static final int LOCAL_POSITION_BITS = 48;

    static {
        assert LOCAL_POSITION_BITS == ColumnExtractor.LOCAL_POSITION_BITS
            : "SourceExtractors.LOCAL_POSITION_BITS must match ColumnExtractor.LOCAL_POSITION_BITS";
    }

    /** Maximum representable file-local row position (inclusive): {@code 2^48 - 1}. */
    public static final long MAX_LOCAL_POSITION = (1L << LOCAL_POSITION_BITS) - 1;

    /**
     * Maximum representable extractor id (inclusive): {@code 2^15 - 1 = 32 767}. We use 15 bits
     * (not the 16 available between {@link #LOCAL_POSITION_BITS} and {@code Long.SIZE}) so that
     * bit 63 stays clear and every encoded value is non-negative — see class-level javadoc.
     */
    public static final int MAX_EXTRACTOR_ID = (1 << (Long.SIZE - LOCAL_POSITION_BITS - 1)) - 1;

    /**
     * Pack an {@code (extractorId, localPosition)} pair into a single {@code long}. The packed
     * value is the on-the-wire representation of {@code _rowPosition} downstream of the source.
     */
    public static long encode(int extractorId, long localPosition) {
        if (extractorId < 0 || extractorId > MAX_EXTRACTOR_ID) {
            throw new IllegalArgumentException("extractor id [" + extractorId + "] is out of range [0, " + MAX_EXTRACTOR_ID + "]");
        }
        if (localPosition < 0 || localPosition > MAX_LOCAL_POSITION) {
            throw new IllegalArgumentException(
                "local row position [" + localPosition + "] is out of range [0, " + MAX_LOCAL_POSITION + "]"
            );
        }
        return ((long) extractorId << LOCAL_POSITION_BITS) | localPosition;
    }

    /** Extract the extractor id (high 15 bits) from an encoded row reference. */
    public static int decodeExtractorId(long encoded) {
        return (int) (encoded >>> LOCAL_POSITION_BITS);
    }

    /** Extract the file-local position (low 48 bits) from an encoded row reference. */
    public static long decodeLocalPosition(long encoded) {
        return encoded & MAX_LOCAL_POSITION;
    }

    private final List<ColumnExtractor> extractors = new ArrayList<>();
    /**
     * Resources whose lifecycle must extend beyond the source operator's, because the deferred
     * extraction path keeps using them to satisfy point lookups long after the source finished
     * producing pages. Typical example: the storage objects that wrap the provider — extractors keep
     * reading through them (bounded by the read thread pool / SDK connection pool, not a per-read acquire)
     * when {@link #materialize} runs. These closeables are closed (LIFO) by {@link #close()} after the
     * registered extractors, so any storage objects they own are torn down first.
     */
    private final List<Closeable> trailingCloseables = new ArrayList<>();
    private boolean closed = false;

    /**
     * Register a new {@link ColumnExtractor} and return the id assigned to it. Ids are allocated
     * sequentially starting at 0, in registration order. The returned id is what callers must
     * pack into the {@code _rowPosition} values they emit for rows coming from this extractor's
     * file.
     *
     * @throws IllegalStateException if the table is closed or if the id space is exhausted
     */
    public synchronized int register(ColumnExtractor extractor) {
        if (extractor == null) {
            throw new IllegalArgumentException("extractor must not be null");
        }
        if (closed) {
            throw new IllegalStateException("SourceExtractors is closed");
        }
        int id = extractors.size();
        if (id > MAX_EXTRACTOR_ID) {
            throw new IllegalStateException("extractor id space exhausted; maximum is " + MAX_EXTRACTOR_ID + " registrations per driver");
        }
        extractors.add(extractor);
        return id;
    }

    /**
     * Look up a registered extractor by id. Throws {@link IllegalArgumentException} for ids that
     * were never assigned — this should never happen in normal operation because the id always
     * comes from a previously-encoded row reference, so failure here indicates a wiring bug.
     */
    public synchronized ColumnExtractor get(int id) {
        if (id < 0 || id >= extractors.size()) {
            throw new IllegalArgumentException("extractor id [" + id + "] is out of range [0, " + extractors.size() + ")");
        }
        return extractors.get(id);
    }

    /** Number of extractors currently registered. */
    public synchronized int size() {
        return extractors.size();
    }

    /**
     * Attach a closeable whose lifecycle must extend beyond the source operator's. The closeable
     * is closed during {@link #close()} after all registered extractors, so any storage objects
     * the extractors hold get released before the trailing resource (the per-query concurrency
     * budget) is torn down.
     * <p>
     * If the registry has already been closed, the closeable is closed immediately to preserve
     * the invariant that registered resources are eventually closed exactly once.
     */
    public void registerTrailingCloseable(Closeable closeable) {
        if (closeable == null) {
            return;
        }
        boolean closeNow;
        synchronized (this) {
            if (closed) {
                closeNow = true;
            } else {
                trailingCloseables.add(closeable);
                closeNow = false;
            }
        }
        if (closeNow) {
            IOUtils.closeWhileHandlingException(closeable);
        }
    }

    /**
     * Materialize {@code columns} for the rows referenced by {@code encodedRefs[0..count)}.
     * Output blocks preserve the input order: row {@code i} of each output block corresponds to
     * {@code encodedRefs[i]}.
     * <p>
     * Algorithm: classify each input ref by its extractor id, build per-extractor packed arrays
     * of file-local positions, dispatch one {@link ColumnExtractor#extract} call per (column, id)
     * pair, then reassemble the output by walking the original slot order. The grouping makes
     * each underlying extractor see a single batched call per column rather than one call per
     * row, which matters for parquet because each call walks the file forward.
     *
     * @param encodedRefs encoded row references (as emitted via {@code _rowPosition})
     * @param count       number of valid entries in {@code encodedRefs}
     * @param columns     column names to materialize
     * @param factory     block factory for memory accounting
     * @return one Block per column, each with exactly {@code count} rows; in caller-supplied order
     */
    public Block[] materialize(long[] encodedRefs, int count, List<String> columns, BlockFactory factory) {
        if (encodedRefs == null) {
            throw new IllegalArgumentException("encodedRefs must not be null");
        }
        if (columns == null) {
            throw new IllegalArgumentException("columns must not be null");
        }
        if (factory == null) {
            throw new IllegalArgumentException("factory must not be null");
        }
        if (count < 0) {
            throw new IllegalArgumentException("count must be non-negative, got [" + count + "]");
        }
        if (count > encodedRefs.length) {
            throw new IllegalArgumentException("count [" + count + "] exceeds encodedRefs length [" + encodedRefs.length + "]");
        }

        // Snapshot the current size and grab a stable reference. Source registrations are
        // monotonic; we never see fewer extractors than encoded ids reference, so snapshotting
        // is safe even though new extractors may register after this read.
        final List<ColumnExtractor> snapshot;
        synchronized (this) {
            if (closed) {
                throw new IllegalStateException("SourceExtractors is closed");
            }
            snapshot = new ArrayList<>(extractors);
        }

        int colCount = columns.size();
        Block[] result = new Block[colCount];
        if (count == 0) {
            for (int i = 0; i < colCount; i++) {
                result[i] = factory.newConstantNullBlock(0);
            }
            return result;
        }
        if (snapshot.isEmpty()) {
            throw new IllegalStateException("extract requested for [" + count + "] positions but no extractors are registered");
        }

        // Phase 1: classify each input ref by extractor id, validating bounds along the way.
        int snapSize = snapshot.size();
        int[] idPerInput = new int[count];
        int[] perIdCounts = new int[snapSize];
        for (int i = 0; i < count; i++) {
            long ref = encodedRefs[i];
            int id = decodeExtractorId(ref);
            if (id < 0 || id >= snapSize) {
                throw new IllegalArgumentException(
                    "row reference [" + ref + "] decodes to extractor id [" + id + "] which is out of range [0, " + snapSize + ")"
                );
            }
            idPerInput[i] = id;
            perIdCounts[id]++;
        }

        // Phase 2: build per-id packed arrays of file-local positions and the original output slot
        // each one will fill in the final blocks.
        long[][] localPositionsById = new long[snapSize][];
        int[][] outputSlotsById = new int[snapSize][];
        for (int id = 0; id < snapSize; id++) {
            int c = perIdCounts[id];
            if (c > 0) {
                localPositionsById[id] = new long[c];
                outputSlotsById[id] = new int[c];
            }
        }
        int[] cursors = new int[snapSize];
        for (int i = 0; i < count; i++) {
            int id = idPerInput[i];
            int slot = cursors[id]++;
            localPositionsById[id][slot] = decodeLocalPosition(encodedRefs[i]);
            outputSlotsById[id][slot] = i;
        }

        // Phase 3: per-id multi-column extraction. Each ColumnExtractor's multi-column extract
        // returns one Block per requested column; row k of every returned block aligns with
        // localPositionsById[id][k] (per-id grouping order). One call per id, not one per
        // (column, id) pair: implementations can coalesce I/O across columns within the same
        // call, and we never make more remote requests than there are extractor ids in the
        // batch. perColIdBlocks[c][id] holds the column-c block for extractor id.
        Block[][] perColIdBlocks = new Block[colCount][snapSize];
        boolean assembled = false;
        try {
            String[] columnNames = columns.toArray(String[]::new);
            for (int id = 0; id < snapSize; id++) {
                if (perIdCounts[id] == 0) {
                    continue;
                }
                Block[] perIdBlocks;
                try {
                    perIdBlocks = snapshot.get(id).extract(columnNames, localPositionsById[id], factory);
                } catch (IOException e) {
                    throw new UncheckedIOException("failed to materialize columns " + columns + " from extractor id [" + id + "]", e);
                }
                if (perIdBlocks == null || perIdBlocks.length != colCount) {
                    if (perIdBlocks != null) {
                        Releasables.closeExpectNoException(perIdBlocks);
                    }
                    throw new IllegalStateException(
                        "extractor id ["
                            + id
                            + "] returned "
                            + (perIdBlocks == null ? "null" : perIdBlocks.length + " blocks")
                            + " for "
                            + colCount
                            + " requested columns"
                    );
                }
                for (int c = 0; c < colCount; c++) {
                    perColIdBlocks[c][id] = perIdBlocks[c];
                }
            }

            // Phase 4: invert the per-id grouping so we can walk output slots in order.
            int[] inverseId = new int[count];
            int[] inverseIndex = new int[count];
            for (int id = 0; id < snapSize; id++) {
                int[] slots = outputSlotsById[id];
                if (slots == null) {
                    continue;
                }
                for (int i = 0; i < slots.length; i++) {
                    inverseId[slots[i]] = id;
                    inverseIndex[slots[i]] = i;
                }
            }

            // Phase 5: build the final per-column blocks by copying from the per-id blocks in slot
            // order. Element type comes from the first non-null per-id block for each column.
            // Hot-path optimization opportunity (deferred): when {@code snapSize == 1} the inverse
            // mapping is identity ({@code inverseId[s] == 0}, {@code inverseIndex[s] == s}) and the
            // builder copy reduces to a verbatim re-build of the per-id block. Detecting that case
            // and returning the per-id block directly (with an extra {@code incRef}) would avoid
            // {@code count * colCount} {@code copyFrom} calls on the dominant single-file path.
            Block.Builder[] builders = new Block.Builder[colCount];
            boolean built = false;
            try {
                for (int c = 0; c < colCount; c++) {
                    for (int id = 0; id < snapSize; id++) {
                        if (perColIdBlocks[c][id] != null) {
                            builders[c] = perColIdBlocks[c][id].elementType().newBlockBuilder(count, factory);
                            break;
                        }
                    }
                }
                for (int slot = 0; slot < count; slot++) {
                    int id = inverseId[slot];
                    int idx = inverseIndex[slot];
                    for (int c = 0; c < colCount; c++) {
                        Block source = perColIdBlocks[c][id];
                        // source is non-null because perIdCounts[id] > 0 (slot exists for this id).
                        builders[c].copyFrom(source, idx, idx + 1);
                    }
                }
                try {
                    for (int c = 0; c < colCount; c++) {
                        if (builders[c] == null) {
                            result[c] = factory.newConstantNullBlock(count);
                        } else {
                            result[c] = builders[c].build();
                        }
                    }
                    built = true;
                } finally {
                    if (built == false) {
                        Releasables.closeExpectNoException(result);
                        java.util.Arrays.fill(result, null);
                    }
                }
            } finally {
                for (Block.Builder b : builders) {
                    if (b != null) {
                        Releasables.closeExpectNoException(b);
                    }
                }
            }
            assembled = true;
        } finally {
            if (assembled == false) {
                // result blocks are released in the inner finally above; here we just drop the
                // per-id intermediate blocks that we own.
            }
            for (Block[] perCol : perColIdBlocks) {
                for (Block b : perCol) {
                    if (b != null) {
                        Releasables.closeExpectNoException(b);
                    }
                }
            }
        }
        return result;
    }

    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        closed = true;
        Releasables.closeExpectNoException(extractors.toArray(new Releasable[0]));
        extractors.clear();
        // Close trailing resources (e.g. per-query budget) after the extractors so any
        // storage-object closes that flow through them succeed before the budget is torn down.
        // LIFO order — last attached, first closed.
        for (int i = trailingCloseables.size() - 1; i >= 0; i--) {
            IOUtils.closeWhileHandlingException(trailingCloseables.get(i));
        }
        trailingCloseables.clear();
    }
}
