/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.escf;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IntsRef;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;

import java.util.Arrays;

/**
 * Scatters a single {@link EscfBatch} into {@code partitionCount} per-partition batches, driven
 * by a caller-supplied selector array: {@code selectors[row]} gives the destination partition
 * (shard index) for each row.
 *
 * <p>This is the alternative to computing routing during encoding. The caller encodes all documents
 * into one batch, then calls {@link #scatter} once the shard mapping is known. This decouples the
 * encode path (parsing, column building) from the routing decision, which is useful when the shard
 * count changes between encoding and routing (re-sharding), or when the routing decision requires
 * the fully-built batch.
 *
 * <p>Scatter is a <em>mechanical reshuffle</em>: values are never decoded into Java scalars and
 * re-encoded. The implementation uses three branches based on physical representation:
 * <ol>
 *   <li>Bitset ({@link EscfColumnKind#BOOL}): random-access bitset reads.</li>
 *   <li>Fixed 64-bit words ({@link EscfColumnKind#LONG}, {@link EscfColumnKind#DOUBLE}, and their
 *       array variants): raw bit patterns pass through via
 *       {@link EscfColumnBuilder#setRawFixed64} / {@link EscfColumnBuilder#appendFixedBits}.</li>
 *   <li>Var-width byte ranges ({@link EscfColumnKind#STRING}, {@link EscfColumnKind#BINARY}, their
 *       array variants, and {@link EscfColumnKind#UNION}): opaque byte payloads move verbatim
 *       through a {@link AbstractVarColumn.DenseBytesRefValuesCursor}.</li>
 * </ol>
 *
 * <p>Scatter is <em>kind-preserving for all seven column kinds</em>:
 * {@link EscfColumnBuilder#hintScalar}, {@link EscfColumnBuilder#hintArray}, and
 * {@link EscfColumnBuilder#hintUnion} are applied up front so that even an all-absent destination
 * partition finishes with the source column's kind. UNION columns stay UNION in every destination
 * partition regardless of how homogeneous the received rows are — raw slot copying requires the
 * destination to be a union builder from row 0.
 *
 * <p>The scatterer is <em>reusable</em>: scratch arrays are retained between calls and grown as
 * needed. Call {@link #close()} to release any in-flight state. The recycler is <em>not</em> owned
 * by this class and must not be closed via {@link #close()}.
 *
 * <p>Scatter builds all per-partition {@link EscfColumnData} arrays first, then constructs all
 * {@link EscfBatch} objects in a single non-throwing loop. This ensures that a failure during
 * column building can release all allocated buffers without double-closing already-handed-off data.
 *
 * <p><strong>Must live in {@code org.elasticsearch.escf}</strong>: it accesses package-private
 * APIs on {@link EscfBatch}, {@link EscfColumn} subtypes, and {@link EscfColumnBuilder}.
 */
public final class EscfBatchScatterer implements Releasable {

    private final Recycler<BytesRef> recycler;

    /** Per-partition column builders; grown as needed; nulled out after scatter. */
    private EscfColumnBuilder[] builders = new EscfColumnBuilder[0];

    /** {@code columns[p][c]} holds finished column data for partition {@code p}, column {@code c}. */
    private EscfColumnData[][] columns = new EscfColumnData[0][];

    public EscfBatchScatterer(Recycler<BytesRef> recycler) {
        this.recycler = recycler;
    }

    /**
     * Scatters {@code source}'s rows into up to {@code partitionCount} new batches.
     * {@code selectors[row]} must be in {@code [0, partitionCount)} for every row in
     * {@code [0, source.docCount())}.
     *
     * <p>Returns an array of length {@code partitionCount}, indexed by partition. Partitions that
     * received no rows have a {@code null} entry. Each non-null batch owns its buffers; the caller
     * is responsible for closing them. The source batch's {@link EscfBatch#schema()} is shared
     * (not copied) — do not modify the schema after scatter.
     *
     * <p>The returned batches are fully independent of the source and of each other; the caller may
     * close the source immediately after scatter returns.
     *
     * @throws IllegalArgumentException if any selector is out of {@code [0, partitionCount)} or
     *                                  if {@code selectors.length < source.docCount()}
     */
    public EscfBatch[] scatter(EscfBatch source, int[] selectors, int partitionCount) {
        if (partitionCount <= 0) {
            throw new IllegalArgumentException("partitionCount must be positive, got " + partitionCount);
        }
        final int docCount = source.docCount();
        if (selectors.length < docCount) {
            throw new IllegalArgumentException("selectors length " + selectors.length + " is shorter than source docCount " + docCount);
        }

        // Count pass: validate selectors and build per-partition row counts.
        int[] destCounts = new int[partitionCount];
        for (int row = 0; row < docCount; row++) {
            int p = selectors[row];
            if (p < 0 || p >= partitionCount) {
                throw new IllegalArgumentException("selectors[" + row + "] = " + p + " is out of [0, " + partitionCount + ")");
            }
            destCounts[p]++;
        }

        // columnCount() == schema.leafCount(); columns array must match.
        final int columnCount = source.columnCount();

        // Grow scratch arrays if needed.
        if (builders.length < partitionCount) {
            builders = new EscfColumnBuilder[partitionCount];
        }
        if (columns.length < partitionCount) {
            columns = new EscfColumnData[partitionCount][];
        }
        for (int p = 0; p < partitionCount; p++) {
            if (destCounts[p] > 0) {
                columns[p] = new EscfColumnData[columnCount];
            }
        }

        // Scatter all columns. Track per-partition "next row" counters so positional writes work.
        int[] destRow = new int[partitionCount];
        try {
            for (int c = 0; c < columnCount; c++) {
                EscfColumn col = source.column(c);
                scatterColumn(col, selectors, docCount, destCounts, destRow, c);
            }
        } catch (Exception e) {
            releaseInFlight();
            throw e;
        }

        // All columns built. Now construct EscfBatch objects in a non-throwing loop.
        // After handoff, null the columns field so close() does not double-release them.
        EscfBatch[] results = new EscfBatch[partitionCount];
        for (int p = 0; p < partitionCount; p++) {
            if (destCounts[p] > 0) {
                EscfColumnData[] partCols = columns[p];
                columns[p] = null;
                results[p] = new EscfBatch(source.schema(), destCounts[p], partCols, Releasables.wrap(partCols));
            }
        }
        return results;
    }

    /**
     * Releases any in-flight column builders and finished column data that have not yet been
     * handed off to an {@link EscfBatch}. Safe to call at any point; idempotent.
     */
    @Override
    public void close() {
        releaseInFlight();
    }

    // -------------------------------------------------------------------------
    // Per-kind scatter dispatch
    // -------------------------------------------------------------------------

    private void scatterColumn(EscfColumn col, int[] selectors, int docCount, int[] destCounts, int[] destRow, int columnIndex) {
        // Reset per-partition row counters for this column pass.
        Arrays.fill(destRow, 0, destCounts.length, 0);

        // Create one builder per non-empty partition and apply the kind hint so that all-absent
        // partitions finish with the source column's kind rather than the default all-absent LONG.
        int partitionCount = destCounts.length;
        for (int p = 0; p < partitionCount; p++) {
            if (destCounts[p] > 0) {
                EscfColumnBuilder b = new EscfColumnBuilder(EscfColumnBuilder.CollisionPolicy.SPLIT, recycler);
                applyHint(b, col);
                builders[p] = b;
            }
        }

        byte kind = col.kind();
        switch (kind) {
            // Branch 1: bitset
            case EscfColumnKind.BOOL -> scatterBool(col, selectors, docCount, destRow);
            // Branch 2: fixed 64-bit words (LONG and DOUBLE share one loop via setRawFixed64)
            case EscfColumnKind.LONG, EscfColumnKind.DOUBLE -> scatterFixed64(col, selectors, docCount, destRow);
            case EscfColumnKind.ARRAY -> scatterArray((EscfArrayColumn) col, selectors, docCount, destRow);
            // Branch 3: var-width byte ranges (STRING, BINARY, UNION)
            case EscfColumnKind.STRING, EscfColumnKind.BINARY -> scatterVarWidth(col, selectors, docCount, destRow);
            case EscfColumnKind.UNION -> scatterUnion((EscfUnionColumn) col, selectors, docCount, destRow);
            default -> throw new IllegalStateException("Unknown column kind: " + EscfColumnKind.name(kind));
        }

        // Finish all builders for this column, storing in columns[p][columnIndex].
        for (int p = 0; p < partitionCount; p++) {
            if (builders[p] != null) {
                try {
                    columns[p][columnIndex] = builders[p].finish(destCounts[p]);
                } finally {
                    builders[p] = null;
                }
            }
        }
    }

    /**
     * Applies a type hint to {@code builder} based on {@code col}'s kind so that scatter is
     * kind-preserving: an all-absent partition finishes with the source column's kind rather
     * than the default all-absent LONG, and a UNION partition receiving only LONG rows stays UNION.
     */
    private static void applyHint(EscfColumnBuilder builder, EscfColumn col) {
        switch (col.kind()) {
            case EscfColumnKind.LONG, EscfColumnKind.DOUBLE, EscfColumnKind.BOOL, EscfColumnKind.STRING, EscfColumnKind.BINARY -> builder
                .hintScalar(col.kind());
            case EscfColumnKind.ARRAY -> builder.hintArray(((EscfArrayColumn) col).child().kind());
            case EscfColumnKind.UNION -> builder.hintUnion();
            default -> throw new IllegalStateException("Unknown column kind: " + EscfColumnKind.name(col.kind()));
        }
    }

    // ---- Branch 1: bitset (BOOL) --------------------------------------------

    private void scatterBool(EscfColumn col, int[] selectors, int docCount, int[] destRow) {
        // EscfBoolColumn is a FixedBitSet — random access is cheaper than a cursor.
        for (int row = 0; row < docCount; row++) {
            int p = selectors[row];
            if (col.isPresent(row)) {
                builders[p].setBoolean(destRow[p], col.getBooleanValue(row));
            }
            // Absent rows: fillGapTo in finish() will back-fill.
            destRow[p]++;
        }
    }

    // ---- Branch 2: fixed 64-bit words (LONG, DOUBLE, ARRAY of those) --------

    /**
     * Scatters a LONG or DOUBLE scalar column. Both kinds store raw 64-bit LE words, so they share
     * a single loop via {@link EscfColumnBuilder#setRawFixed64}. {@link AbstractFixed64Column#longCursor}
     * is documented to yield "the raw 64-bit stored word" for both kinds, so no {@code
     * Double.longBitsToDouble} conversion is needed on the read side.
     */
    private void scatterFixed64(EscfColumn col, int[] selectors, int docCount, int[] destRow) {
        byte kind = col.kind();
        var cursor = col.longCursor();
        int nextPresentDoc = cursor.nextDoc();
        for (int row = 0; row < docCount; row++) {
            int p = selectors[row];
            if (row == nextPresentDoc) {
                builders[p].setRawFixed64(destRow[p], kind, cursor.longValue());
                nextPresentDoc = cursor.nextDoc();
            }
            // Absent rows: fillGapTo in finish() will back-fill.
            destRow[p]++;
        }
    }

    private void scatterArray(EscfArrayColumn col, int[] selectors, int docCount, int[] destRow) {
        EscfColumn child = col.child();
        byte childKind = child.kind();

        IntsRef rowOffsets = col.rowOffsets();
        int[] offs = rowOffsets.ints;
        int base = rowOffsets.offset;

        // The child is kept unsliced by sliceInternal, so skip the first offs[base] elements
        // (which belong to rows before this window's start).
        int startElem = offs[base];

        switch (childKind) {
            case EscfColumnKind.LONG, EscfColumnKind.DOUBLE -> {
                // Both LONG and DOUBLE: raw 64-bit words via appendFixedBits (no decode/re-encode).
                var childCursor = ((AbstractFixed64Column) child).longValuesCursor();
                if (docCount > 0 && startElem > 0) {
                    childCursor.skip(startElem);
                }
                for (int row = 0; row < docCount; row++) {
                    int p = selectors[row];
                    int elemCount = offs[base + row + 1] - offs[base + row];
                    if (col.isPresent(row)) {
                        builders[p].beginArray(destRow[p]);
                        for (int e = 0; e < elemCount; e++) {
                            builders[p].appendFixedBits(childKind, childCursor.nextLong());
                        }
                        builders[p].endArray();
                    }
                    // Absent row: child elements take up zero space (zero-width offset range).
                    destRow[p]++;
                }
            }
            case EscfColumnKind.STRING, EscfColumnKind.BINARY -> {
                // Var-width children: opaque byte payloads via a dense cursor.
                var childCursor = ((AbstractVarColumn) child).bytesRefValuesCursor(false);
                if (docCount > 0 && startElem > 0) {
                    childCursor.skip(startElem);
                }
                for (int row = 0; row < docCount; row++) {
                    int p = selectors[row];
                    int elemCount = offs[base + row + 1] - offs[base + row];
                    if (col.isPresent(row)) {
                        builders[p].beginArray(destRow[p]);
                        if (childKind == EscfColumnKind.STRING) {
                            for (int e = 0; e < elemCount; e++) {
                                builders[p].appendString(childCursor.nextValue());
                            }
                        } else {
                            for (int e = 0; e < elemCount; e++) {
                                builders[p].appendBinary(childCursor.nextValue());
                            }
                        }
                        builders[p].endArray();
                    }
                    destRow[p]++;
                }
            }
            default -> throw new UnsupportedOperationException(
                "EscfBatchScatterer: ARRAY scatter not supported for child kind "
                    + EscfColumnKind.name(childKind)
                    + " (BOOL/ARRAY/UNION are not valid array children)"
            );
        }
    }

    // ---- Branch 3: var-width byte ranges (STRING, BINARY) -------------------

    /**
     * Scatters a STRING or BINARY scalar column. Values are passed as opaque byte ranges via the
     * existing sparse {@link EscfColumn#bytesRefCursor} — no UTF-8 parsing or length prefixing.
     */
    private void scatterVarWidth(EscfColumn col, int[] selectors, int docCount, int[] destRow) {
        boolean isString = col.kind() == EscfColumnKind.STRING;
        var cursor = col.bytesRefCursor(false);
        int nextPresentDoc = cursor.nextDoc();
        for (int row = 0; row < docCount; row++) {
            int p = selectors[row];
            if (row == nextPresentDoc) {
                BytesRef value = cursor.value();
                if (isString) {
                    builders[p].setString(destRow[p], value);
                } else {
                    builders[p].setBinary(destRow[p], value);
                }
                nextPresentDoc = cursor.nextDoc();
            }
            destRow[p]++;
        }
    }

    // ---- Branch 3: UNION (raw type + payload) --------------------------------

    /**
     * Scatters a UNION column as a bytes-to-bytes copy. Each row's type byte is read from
     * {@link EscfUnionColumn#typeVec()} and its payload from a dense
     * {@link AbstractVarColumn.DenseBytesRefValuesCursor} over the union's {@code (offsets, data)}.
     *
     * <p>Unlike the var-width cursor, the union cursor is dense (one entry per row, including absent
     * and null rows), because the UNION offset vector has one slot per row regardless of validity.
     * Payload width comes from the offset vector — never from the type byte — so absent rows promoted
     * from a numeric column (which occupy 8-byte payload slots) are copied correctly.
     *
     * <p>Because {@link #applyHint} calls {@link EscfColumnBuilder#hintUnion()} up front, the
     * destination is always a union builder; rows that happen to all be LONG in one partition still
     * finish as UNION.
     */
    private void scatterUnion(EscfUnionColumn col, int[] selectors, int docCount, int[] destRow) {
        BytesRef typeVec = col.typeVec();
        byte[] typeVecBytes = typeVec.bytes;
        int typeVecOffset = typeVec.offset;
        AbstractVarColumn.DenseBytesRefValuesCursor payloads = col.payloadCursor();
        for (int row = 0; row < docCount; row++) {
            int p = selectors[row];
            byte type = typeVecBytes[typeVecOffset + row];
            BytesRef payload = payloads.nextValue();
            builders[p].addRawUnionRow(type, payload);
        }
    }

    // -------------------------------------------------------------------------
    // Failure handling
    // -------------------------------------------------------------------------

    /**
     * Releases all in-flight builders (discarding their recycler-backed buffers) and any
     * already-finished {@link EscfColumnData} that have not yet been handed off to an
     * {@link EscfBatch}. Safe to call at any point; idempotent.
     */
    private void releaseInFlight() {
        // discard() on a builder is idempotent after finish() (the stream is already moved/nulled).
        for (int p = 0; p < builders.length; p++) {
            if (builders[p] != null) {
                builders[p].discard();
                builders[p] = null;
            }
        }
        // Close any finished column data that has not yet been handed off to a batch.
        for (int p = 0; p < columns.length; p++) {
            if (columns[p] != null) {
                Releasables.close(columns[p]);
                columns[p] = null;
            }
        }
    }
}
