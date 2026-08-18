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
 * Scatters a single {@link EscfBatch} into per-partition batches, where {@code partitionIds[row]}
 * gives the destination partition for each row.
 *
 * <p>Values are never decoded into Java scalars and re-encoded — BOOL uses bitset reads, fixed
 * 64-bit kinds (LONG, DOUBLE) copy raw words, and var-width kinds (STRING, BINARY, UNION) copy
 * opaque byte payloads. Kind hints are applied up front so all-absent partitions preserve the
 * source column's kind, and UNION stays UNION in every partition.
 *
 * <p>Reusable: scratch arrays grow as needed; call {@link #close()} to release in-flight state.
 * The recycler is not owned by this class. Must live in {@code org.elasticsearch.escf} for
 * package-private access to {@link EscfBatch}, {@link EscfColumn} subtypes, and
 * {@link EscfColumnBuilder}.
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
     * Scatters {@code source}'s rows into {@code partitionCount} new batches.
     *
     * <p>Returns an array of length {@code partitionCount}; partitions with no rows have a
     * {@code null} entry. Each non-null batch owns its buffers and must be closed by the caller.
     * The schema is shared, not copied. The source may be closed immediately after this returns.
     *
     * @throws IllegalArgumentException if any partitionId is outside {@code [0, partitionCount)} or
     *                                  if {@code partitionIds.length < source.docCount()}
     */
    public EscfBatch[] scatter(EscfBatch source, int[] partitionIds, int partitionCount) {
        if (partitionCount <= 0) {
            throw new IllegalArgumentException("partitionCount must be positive, got " + partitionCount);
        }
        final int docCount = source.docCount();
        if (partitionIds.length < docCount) {
            throw new IllegalArgumentException(
                "partitionIds length " + partitionIds.length + " is shorter than source docCount " + docCount
            );
        }

        // Count pass: validate partitionIds and build per-partition row counts.
        int[] destCounts = new int[partitionCount];
        for (int row = 0; row < docCount; row++) {
            int p = partitionIds[row];
            if (p < 0 || p >= partitionCount) {
                throw new IllegalArgumentException("partitionIds[" + row + "] = " + p + " is out of [0, " + partitionCount + ")");
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
                scatterColumn(col, partitionIds, docCount, destCounts, destRow, c);
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

    /** Releases in-flight builders and column data not yet handed off. Safe to call at any point. */
    @Override
    public void close() {
        releaseInFlight();
    }

    private void scatterColumn(EscfColumn col, int[] partitionIds, int docCount, int[] destCounts, int[] destRow, int columnIndex) {
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
            case EscfColumnKind.BOOL -> scatterBool(col, partitionIds, docCount, destRow);
            // Branch 2: fixed 64-bit words (LONG and DOUBLE share one loop via setRawFixed64)
            case EscfColumnKind.LONG, EscfColumnKind.DOUBLE -> scatterFixed64(col, partitionIds, docCount, destRow);
            case EscfColumnKind.ARRAY -> scatterArray((EscfArrayColumn) col, partitionIds, docCount, destRow);
            // Branch 3: var-width byte ranges (STRING, BINARY, UNION)
            case EscfColumnKind.STRING, EscfColumnKind.BINARY -> scatterVarWidth(col, partitionIds, docCount, destRow);
            case EscfColumnKind.UNION -> scatterUnion((EscfUnionColumn) col, partitionIds, docCount, destRow);
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

    /** Applies a kind hint so all-absent partitions preserve the source column's kind. */
    private static void applyHint(EscfColumnBuilder builder, EscfColumn col) {
        switch (col.kind()) {
            case EscfColumnKind.LONG, EscfColumnKind.DOUBLE, EscfColumnKind.BOOL, EscfColumnKind.STRING, EscfColumnKind.BINARY -> builder
                .hintScalar(col.kind());
            case EscfColumnKind.ARRAY -> builder.hintArray(((EscfArrayColumn) col).child().kind());
            case EscfColumnKind.UNION -> builder.hintUnion();
            default -> throw new IllegalStateException("Unknown column kind: " + EscfColumnKind.name(col.kind()));
        }
    }

    private void scatterBool(EscfColumn col, int[] partitionIds, int docCount, int[] destRow) {
        // EscfBoolColumn is a FixedBitSet — random access is cheaper than a cursor.
        for (int row = 0; row < docCount; row++) {
            int p = partitionIds[row];
            if (col.isPresent(row)) {
                builders[p].setBoolean(destRow[p], col.getBooleanValue(row));
            }
            // Absent rows: fillGapTo in finish() will back-fill.
            destRow[p]++;
        }
    }

    /**
     * Scatters a LONG or DOUBLE column. Both store raw 64-bit LE words, so they share a loop via
     * {@link EscfColumnBuilder#setRawFixed64} with no decode/re-encode.
     */
    private void scatterFixed64(EscfColumn col, int[] partitionIds, int docCount, int[] destRow) {
        byte kind = col.kind();
        var cursor = col.longCursor();
        int nextPresentDoc = cursor.nextDoc();
        for (int row = 0; row < docCount; row++) {
            int p = partitionIds[row];
            if (row == nextPresentDoc) {
                builders[p].setRawFixed64(destRow[p], kind, cursor.longValue());
                nextPresentDoc = cursor.nextDoc();
            }
            // Absent rows: fillGapTo in finish() will back-fill.
            destRow[p]++;
        }
    }

    private void scatterArray(EscfArrayColumn col, int[] partitionIds, int docCount, int[] destRow) {
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
                    int p = partitionIds[row];
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
                    int p = partitionIds[row];
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

    /** Scatters a STRING or BINARY column via the sparse cursor; bytes are copied verbatim. */
    private void scatterVarWidth(EscfColumn col, int[] partitionIds, int docCount, int[] destRow) {
        boolean isString = col.kind() == EscfColumnKind.STRING;
        var cursor = col.bytesRefCursor(false);
        int nextPresentDoc = cursor.nextDoc();
        for (int row = 0; row < docCount; row++) {
            int p = partitionIds[row];
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

    private void scatterUnion(EscfUnionColumn col, int[] partitionIds, int docCount, int[] destRow) {
        var cursor = col.bytesRefCursor(false);
        int nextPresentDoc = cursor.nextDoc();
        for (int row = 0; row < docCount; row++) {
            int p = partitionIds[row];
            if (row == nextPresentDoc) {
                builders[p].addRawUnionRow(col.typeByteForPresent(row), cursor.value());
                nextPresentDoc = cursor.nextDoc();
            } else {
                builders[p].addAbsent();
            }
            destRow[p]++;
        }
    }


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
