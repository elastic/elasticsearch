/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing;

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.hash.BufferedMurmur3Hasher;
import org.elasticsearch.common.hash.MurmurHash3;
import org.elasticsearch.escf.EscfBatch;
import org.elasticsearch.escf.EscfColumn;
import org.elasticsearch.escf.EscfColumnKind;
import org.elasticsearch.escf.PresentDocIterator;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.sourcebatch.ArrayReader;
import org.elasticsearch.sourcebatch.SourceBatch;
import org.elasticsearch.sourcebatch.SourceSchema;
import org.elasticsearch.sourcebatch.SourceValueType;
import org.elasticsearch.xcontent.XContentString;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.function.Predicate;

/**
 * Computes time series identifiers (TSIDs) for a batch of documents in one column-major pass,
 * reading dimension values directly from an {@link EscfBatch} without parsing JSON source.
 *
 * <p>Three wins over the per-document {@link XContentParserTsidFunnel} path:
 * <ol>
 *   <li>No XContent parse — dimension values are read from typed columns.</li>
 *   <li>Path hash computed once per column, not once per (row, column) pair.</li>
 *   <li>Columns visited in path-sorted order, so each row's dimension list is already in the
 *       order that {@link TsidBuilder#buildTsid} needs, turning its {@code Collections.sort} into
 *       an O(n) already-sorted scan.</li>
 * </ol>
 *
 * <p><b>Value hash parity with {@link XContentParserTsidFunnel}:</b> each column kind maps to the
 * same {@link TsidBuilder} call the funnel would have made for the equivalent JSON token:
 * <ul>
 *   <li>{@link EscfColumnKind#LONG} → {@code Hash128(1, v)} (int and long both use tag 1)</li>
 *   <li>{@link EscfColumnKind#DOUBLE} → {@code Hash128(2, Double.doubleToLongBits(v))}</li>
 *   <li>{@link EscfColumnKind#BOOL} → {@code Hash128(3, v ? 1 : 0)}</li>
 *   <li>{@link EscfColumnKind#STRING} → murmur3-128 of the UTF-8 bytes</li>
 *   <li>{@link EscfColumnKind#ARRAY} → element-granular dispatch on the above rules</li>
 *   <li>{@link EscfColumnKind#UNION} → per-row type dispatch on the above rules;
 *       {@code NULL} rows contribute no entry</li>
 *   <li>{@link EscfColumnKind#BINARY} → throws {@link IllegalArgumentException}
 *       (JSON dimensions cannot produce binary values)</li>
 * </ul>
 *
 * <p><b>Known edge case:</b> {@link org.elasticsearch.escf.EscfEncoder} does not merge dotted and
 * nested spellings — {@code {"a.b":1}} and {@code {"a":{"b":1}}} produce two distinct leaf columns
 * that both report {@link SourceSchema#getFullPath(int) full path} {@code "a.b"}. Column-major
 * ordering tie-breaks such a collision on leaf index (first-encoded column wins), whereas the
 * source-parser funnel would order by per-document source order. This is a pre-existing encoder
 * quirk and is not fixed here.
 *
 * <p><b>Rows with no dimension values</b> cause {@link TsidBuilder#buildTsid} to throw
 * {@link IllegalArgumentException}("Dimensions are empty"), identical to the per-document path.
 * Attribution of that failure to a specific row (so the rest of the bulk survives) is left to the
 * integration caller.
 */
public final class ColumnarTsidCalculator {

    private ColumnarTsidCalculator() {}

    /** Resolved dimension column: leaf schema index, full dotted path, and precomputed path hash. */
    private record DimColumn(int leafIdx, String path, long pathH1, long pathH2) {}

    /**
     * Computes one tsid per row of {@code batch}.
     *
     * @param batch           column-major document batch; must be an {@link EscfBatch}
     * @param isDimension     returns {@code true} for dimension field full paths
     * @param creationVersion index creation version, forwarded to {@link TsidBuilder#buildTsid}
     * @return one {@link BytesRef} per row
     * @throws UnsupportedOperationException if {@code batch} is not an {@link EscfBatch}
     * @throws IllegalArgumentException      if a dimension column has kind {@code BINARY}, or if a
     *                                       row has no dimension values
     */
    public static BytesRef[] computeTsids(SourceBatch batch, Predicate<String> isDimension, IndexVersion creationVersion) {
        if (batch instanceof EscfBatch == false) {
            throw new UnsupportedOperationException(
                "ColumnarTsidCalculator requires an EscfBatch; got " + batch.getClass().getSimpleName()
            );
        }
        EscfBatch escfBatch = (EscfBatch) batch;
        int docCount = batch.docCount();

        // Phase A: discover dimension columns, sort by path, precompute path hashes.
        List<DimColumn> dimColumns = resolveDimColumns(escfBatch.schema(), isDimension);

        // Phase B: two-pass CSR build.
        // Pass 1 — count how many dimension entries each row contributes (scalars: 1, arrays: N).
        int[] rowCounts = countEntriesPerRow(escfBatch, dimColumns);
        // Prefix sum → per-row start positions in the CSR arrays.
        int[] rowStarts = prefixSum(rowCounts, docCount);
        int totalEntries = rowStarts[docCount];
        // Pass 2 — fill CSR arrays, columns visited in path-sorted (ordinal) order.
        int[] csrColOrd = new int[totalEntries];
        long[] csrH1 = new long[totalEntries];
        long[] csrH2 = new long[totalEntries];
        // Write cursors start at each row's beginning; they advance as entries are placed.
        int[] writeCursors = Arrays.copyOf(rowStarts, docCount);
        BufferedMurmur3Hasher hasher = new BufferedMurmur3Hasher(0L);
        fillEntries(escfBatch, dimColumns, hasher, writeCursors, csrColOrd, csrH1, csrH2);

        // Phase C: build one tsid per row by replaying each row's CSR slice.
        BytesRef[] tsids = new BytesRef[docCount];
        TsidBuilder builder = new TsidBuilder();
        for (int r = 0; r < docCount; r++) {
            builder.reset();
            for (int e = rowStarts[r]; e < rowStarts[r + 1]; e++) {
                DimColumn dc = dimColumns.get(csrColOrd[e]);
                builder.addPrehashedDimension(dc.path(), dc.pathH1(), dc.pathH2(), csrH1[e], csrH2[e]);
            }
            // Throws IllegalArgumentException("Dimensions are empty") if no entries for this row.
            tsids[r] = builder.buildTsid(creationVersion);
        }
        return tsids;
    }

    // ── Phase A ─────────────────────────────────────────────────────────────

    private static List<DimColumn> resolveDimColumns(SourceSchema schema, Predicate<String> isDimension) {
        int leafCount = schema.leafCount();
        List<DimColumn> result = new ArrayList<>();
        BufferedMurmur3Hasher hasher = new BufferedMurmur3Hasher(0L);
        for (int leafIdx = 0; leafIdx < leafCount; leafIdx++) {
            String path = schema.getFullPath(leafIdx);
            if (isDimension.test(path)) {
                MurmurHash3.Hash128 pathHash = TsidBuilder.hashPath(hasher, path);
                result.add(new DimColumn(leafIdx, path, pathHash.h1, pathHash.h2));
            }
        }
        // Sort by path, tie-break by leafIdx so the order is deterministic and matches
        // Dimension.compareTo in TsidBuilder (which sorts by path then insertionOrder).
        result.sort(Comparator.comparing(DimColumn::path).thenComparingInt(DimColumn::leafIdx));
        return result;
    }

    // ── Phase B — count ─────────────────────────────────────────────────────

    private static int[] countEntriesPerRow(EscfBatch batch, List<DimColumn> dimColumns) {
        int docCount = batch.docCount();
        int[] rowCounts = new int[docCount];
        for (DimColumn dc : dimColumns) {
            addCountsForColumn(batch.column(dc.leafIdx()), rowCounts);
        }
        return rowCounts;
    }

    private static void addCountsForColumn(EscfColumn col, int[] rowCounts) {
        byte kind = col.kind();
        if (kind == EscfColumnKind.BINARY) {
            throw new IllegalArgumentException("A dimension column has kind BINARY; JSON dimensions cannot produce binary values");
        }
        PresentDocIterator it = col.presentDocs();
        int r;
        while ((r = it.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
            if (kind == EscfColumnKind.ARRAY) {
                rowCounts[r] += countNonNullElements(col.getArrayValue(r));
            } else if (kind == EscfColumnKind.UNION) {
                byte typeByte = col.getTypeByte(r);
                if (typeByte == SourceValueType.NULL) continue;
                if (typeByte == SourceValueType.FIXED_ARRAY || typeByte == SourceValueType.UNION_ARRAY) {
                    rowCounts[r] += countNonNullElements(col.getArrayValue(r));
                } else {
                    rowCounts[r]++;
                }
            } else {
                rowCounts[r]++;
            }
        }
    }

    private static int countNonNullElements(ArrayReader ar) {
        int count = 0;
        while (ar.next()) {
            if (ar.isNull() == false) count++;
        }
        return count;
    }

    private static int[] prefixSum(int[] rowCounts, int docCount) {
        int[] rowStarts = new int[docCount + 1];
        for (int r = 0; r < docCount; r++) {
            rowStarts[r + 1] = rowStarts[r] + rowCounts[r];
        }
        return rowStarts;
    }

    // ── Phase B — fill ──────────────────────────────────────────────────────

    private static void fillEntries(
        EscfBatch batch,
        List<DimColumn> dimColumns,
        BufferedMurmur3Hasher hasher,
        int[] writeCursors,
        int[] csrColOrd,
        long[] csrH1,
        long[] csrH2
    ) {
        for (int ord = 0; ord < dimColumns.size(); ord++) {
            DimColumn dc = dimColumns.get(ord);
            fillColumn(batch.column(dc.leafIdx()), ord, hasher, writeCursors, csrColOrd, csrH1, csrH2);
        }
    }

    private static void fillColumn(
        EscfColumn col,
        int colOrd,
        BufferedMurmur3Hasher hasher,
        int[] writeCursors,
        int[] csrColOrd,
        long[] csrH1,
        long[] csrH2
    ) {
        byte kind = col.kind();
        PresentDocIterator it = col.presentDocs();
        int r;
        while ((r = it.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
            switch (kind) {
                case EscfColumnKind.LONG -> fillScalar(r, colOrd, 1L, col.getLongValue(r), writeCursors, csrColOrd, csrH1, csrH2);
                case EscfColumnKind.DOUBLE -> fillScalar(
                    r,
                    colOrd,
                    2L,
                    Double.doubleToLongBits(col.getDoubleValue(r)),
                    writeCursors,
                    csrColOrd,
                    csrH1,
                    csrH2
                );
                case EscfColumnKind.BOOL -> fillScalar(
                    r,
                    colOrd,
                    3L,
                    col.getBooleanValue(r) ? 1L : 0L,
                    writeCursors,
                    csrColOrd,
                    csrH1,
                    csrH2
                );
                case EscfColumnKind.STRING -> {
                    // getBinaryValue gives the raw UTF-8 bytes of the string without allocating a Text wrapper.
                    BytesRef bytes = col.getBinaryValue(r);
                    hasher.reset();
                    hasher.update(bytes.bytes, bytes.offset, bytes.length);
                    MurmurHash3.Hash128 vh = hasher.digestHash();
                    fillScalar(r, colOrd, vh.h1, vh.h2, writeCursors, csrColOrd, csrH1, csrH2);
                }
                case EscfColumnKind.ARRAY -> fillArrayElements(
                    col.getArrayValue(r),
                    r,
                    colOrd,
                    hasher,
                    writeCursors,
                    csrColOrd,
                    csrH1,
                    csrH2
                );
                case EscfColumnKind.UNION -> fillUnionRow(col, r, colOrd, hasher, writeCursors, csrColOrd, csrH1, csrH2);
                // BINARY: already rejected in the count pass; unreachable here.
                default -> throw new IllegalArgumentException("Unexpected column kind for tsid dimension: " + EscfColumnKind.name(kind));
            }
        }
    }

    private static void fillScalar(int row, int colOrd, long h1, long h2, int[] writeCursors, int[] csrColOrd, long[] csrH1, long[] csrH2) {
        int pos = writeCursors[row]++;
        csrColOrd[pos] = colOrd;
        csrH1[pos] = h1;
        csrH2[pos] = h2;
    }

    private static void fillArrayElements(
        ArrayReader ar,
        int row,
        int colOrd,
        BufferedMurmur3Hasher hasher,
        int[] writeCursors,
        int[] csrColOrd,
        long[] csrH1,
        long[] csrH2
    ) {
        while (ar.next()) {
            if (ar.isNull()) continue;
            byte elemType = ar.type();
            long h1;
            long h2;
            if (elemType == SourceValueType.INT || elemType == SourceValueType.LONG) {
                h1 = 1L;
                h2 = elemType == SourceValueType.INT ? ar.intValue() : ar.longValue();
            } else if (elemType == SourceValueType.FLOAT || elemType == SourceValueType.DOUBLE) {
                h1 = 2L;
                double d = elemType == SourceValueType.FLOAT ? (double) ar.floatValue() : ar.doubleValue();
                h2 = Double.doubleToLongBits(d);
            } else if (elemType == SourceValueType.TRUE) {
                h1 = 3L;
                h2 = 1L;
            } else if (elemType == SourceValueType.FALSE) {
                h1 = 3L;
                h2 = 0L;
            } else if (elemType == SourceValueType.STRING) {
                XContentString.UTF8Bytes utf8 = ar.textValue().bytes();
                hasher.reset();
                hasher.update(utf8.bytes(), utf8.offset(), utf8.length());
                MurmurHash3.Hash128 vh = hasher.digestHash();
                h1 = vh.h1;
                h2 = vh.h2;
            } else {
                throw new IllegalArgumentException(
                    "Unexpected element type " + SourceValueType.name(elemType) + " in tsid dimension array"
                );
            }
            fillScalar(row, colOrd, h1, h2, writeCursors, csrColOrd, csrH1, csrH2);
        }
    }

    private static void fillUnionRow(
        EscfColumn col,
        int row,
        int colOrd,
        BufferedMurmur3Hasher hasher,
        int[] writeCursors,
        int[] csrColOrd,
        long[] csrH1,
        long[] csrH2
    ) {
        byte typeByte = col.getTypeByte(row);
        if (typeByte == SourceValueType.NULL) {
            // Explicit null: funnel skips VALUE_NULL; so do we.
            return;
        }
        if (typeByte == SourceValueType.FIXED_ARRAY || typeByte == SourceValueType.UNION_ARRAY) {
            fillArrayElements(col.getArrayValue(row), row, colOrd, hasher, writeCursors, csrColOrd, csrH1, csrH2);
            return;
        }
        long h1;
        long h2;
        if (typeByte == SourceValueType.INT) {
            h1 = 1L;
            h2 = col.getIntValue(row);
        } else if (typeByte == SourceValueType.LONG) {
            h1 = 1L;
            h2 = col.getLongValue(row);
        } else if (typeByte == SourceValueType.FLOAT) {
            h1 = 2L;
            h2 = Double.doubleToLongBits((double) col.getFloatValue(row));
        } else if (typeByte == SourceValueType.DOUBLE) {
            h1 = 2L;
            h2 = Double.doubleToLongBits(col.getDoubleValue(row));
        } else if (typeByte == SourceValueType.TRUE) {
            h1 = 3L;
            h2 = 1L;
        } else if (typeByte == SourceValueType.FALSE) {
            h1 = 3L;
            h2 = 0L;
        } else if (typeByte == SourceValueType.STRING) {
            // getBinaryValue on a UNION column returns the raw UTF-8 bytes without allocating a Text wrapper.
            BytesRef bytes = col.getBinaryValue(row);
            hasher.reset();
            hasher.update(bytes.bytes, bytes.offset, bytes.length);
            MurmurHash3.Hash128 vh = hasher.digestHash();
            h1 = vh.h1;
            h2 = vh.h2;
        } else if (typeByte == SourceValueType.BINARY) {
            throw new IllegalArgumentException("A BINARY value in a UNION dimension column cannot contribute to a tsid");
        } else {
            throw new IllegalArgumentException(
                "Unexpected type byte " + SourceValueType.name(typeByte) + " in UNION dimension column at row " + row
            );
        }
        fillScalar(row, colOrd, h1, h2, writeCursors, csrColOrd, csrH1, csrH2);
    }
}
