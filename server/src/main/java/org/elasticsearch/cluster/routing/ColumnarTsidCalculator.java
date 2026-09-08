/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing;

import org.apache.lucene.document.column.LongTupleCursor;
import org.apache.lucene.document.column.ObjectTupleCursor;
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
import java.util.Comparator;
import java.util.List;
import java.util.function.Predicate;

/**
 * Computes TSIDs for a batch of documents in one column-major pass, reading dimension values
 * directly from an {@link EscfBatch} without parsing JSON source.
 *
 * <p>Compared to {@link XContentParserTsidFunnel}: no XContent parse, path hashes computed once per
 * column, and columns visited in path-sorted order so each row's dimensions arrive already sorted.
 * Each value is folded straight into its row's {@link ColumnarTsidAccumulator} state, so nothing is
 * buffered between the scan and the finished tsids.
 */
public final class ColumnarTsidCalculator {

    private ColumnarTsidCalculator() {}

    /**
     * Resolved dimension column: leaf schema index, full dotted path, precomputed path hash, and the
     * path's {@link TsidBuilder#prefixByteRank} (order-independent, so it is resolved up front).
     */
    private record DimColumn(int leafIdx, String path, long pathH1, long pathH2, int prefixRank) {}

    /**
     * @param batch           column-major document batch; must be an {@link EscfBatch}
     * @param isDimension     returns {@code true} for dimension field full paths
     * @param creationVersion selects the tsid layout
     * @return one {@link BytesRef} tsid per row
     * @throws UnsupportedOperationException if {@code batch} is not an {@link EscfBatch}
     * @throws IllegalArgumentException      if a dimension column has kind {@code BINARY}, or a row
     *                                       has no dimension values
     */
    public static BytesRef[] computeTsids(SourceBatch batch, Predicate<String> isDimension, IndexVersion creationVersion) {
        if (batch instanceof EscfBatch == false) {
            throw new UnsupportedOperationException(
                "ColumnarTsidCalculator requires an EscfBatch; got " + batch.getClass().getSimpleName()
            );
        }
        EscfBatch escfBatch = (EscfBatch) batch;

        List<DimColumn> dimColumns = resolveDimColumns(escfBatch, isDimension);
        // Hoisted out of the scan: the layout is a property of the index, not of a row.
        ColumnarTsidAccumulator accumulator = ColumnarTsidAccumulator.create(
            batch.docCount(),
            TsidBuilder.useSingleBytePrefixLayout(creationVersion)
        );

        MurmurHash3.Hash128 valueHash = new MurmurHash3.Hash128();
        // Path groups are keyed on path equality, not column index: EscfEncoder does not merge dotted
        // and nested spellings, so two leaf columns can report the same full path and must dedup as
        // one path for the value-similarity bytes.
        int pathGroup = TsidBuilder.NO_PATH_GROUP;
        String previousPath = null;
        for (DimColumn dc : dimColumns) {
            if (dc.path().equals(previousPath) == false) {
                previousPath = dc.path();
                pathGroup++;
            }
            scanColumn(escfBatch.column(dc.leafIdx()), dc, pathGroup, accumulator, valueHash);
        }
        return accumulator.build();
    }

    private static List<DimColumn> resolveDimColumns(EscfBatch batch, Predicate<String> isDimension) {
        SourceSchema schema = batch.schema();
        int leafCount = schema.leafCount();
        List<DimColumn> result = new ArrayList<>();
        BufferedMurmur3Hasher hasher = new BufferedMurmur3Hasher(0L);
        for (int leafIdx = 0; leafIdx < leafCount; leafIdx++) {
            String path = schema.getFullPath(leafIdx);
            if (isDimension.test(path)) {
                // leafValueKind() is the element kind for an ARRAY column and the column's own kind
                // otherwise, so one check rejects both a BINARY column and an array of BINARY before the
                // scan. A BINARY value inside a UNION column is not visible here; addUnionRow rejects it.
                byte leafKind = batch.column(leafIdx).leafValueKind();
                if (leafKind == EscfColumnKind.BINARY) {
                    throw new IllegalArgumentException(
                        "Dimension column [" + path + "] has kind BINARY; JSON dimensions cannot produce binary values"
                    );
                }
                MurmurHash3.Hash128 pathHash = TsidBuilder.hashPath(hasher, path);
                result.add(new DimColumn(leafIdx, path, pathHash.h1, pathHash.h2, TsidBuilder.prefixByteRank(path)));
            }
        }
        // Sort by path, tie-break by leafIdx, so each row's dimensions reach the accumulator in the
        // (path, insertion order) order that both tsid layouts assume.
        result.sort(Comparator.comparing(DimColumn::path).thenComparingInt(DimColumn::leafIdx));
        return result;
    }

    private static void scanColumn(
        EscfColumn col,
        DimColumn dc,
        int pathGroup,
        ColumnarTsidAccumulator accumulator,
        MurmurHash3.Hash128 valueHash
    ) {
        final byte kind = col.kind();
        switch (kind) {
            // Scalar and ARRAY columns share one scan. The cursors are polymorphic across the two
            // shapes — a scalar column yields one tuple per present row, an array column one tuple per
            // element with the row id repeated — and either way the accumulator wants exactly that: one
            // (row, value) pair at a time. leafValueKind() collapses the two, returning the column's own
            // kind for a scalar and the element kind for an array.
            case EscfColumnKind.LONG, EscfColumnKind.DOUBLE, EscfColumnKind.STRING, EscfColumnKind.ARRAY -> scanValues(
                col,
                col.leafValueKind(),
                dc,
                pathGroup,
                accumulator,
                valueHash
            );
            case EscfColumnKind.BOOL -> scanBoolColumn(col, dc, pathGroup, accumulator);
            case EscfColumnKind.UNION -> scanUnionColumn(col, dc, pathGroup, accumulator, valueHash);
            default -> throw new IllegalArgumentException("Unexpected column kind for tsid dimension: " + EscfColumnKind.name(kind));
        }
    }

    private static void scanValues(
        EscfColumn col,
        byte valueKind,
        DimColumn dc,
        int pathGroup,
        ColumnarTsidAccumulator accumulator,
        MurmurHash3.Hash128 valueHash
    ) {
        switch (valueKind) {
            case EscfColumnKind.LONG -> {
                LongTupleCursor cursor = col.longCursor();
                int r;
                while ((r = cursor.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                    add(accumulator, r, dc, pathGroup, TsidBuilder.LONG_VALUE_TAG, cursor.longValue());
                }
            }
            case EscfColumnKind.DOUBLE -> {
                LongTupleCursor cursor = col.longCursor();
                int r;
                while ((r = cursor.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                    long bits = Double.doubleToLongBits(Double.longBitsToDouble(cursor.longValue()));
                    add(accumulator, r, dc, pathGroup, TsidBuilder.DOUBLE_VALUE_TAG, bits);
                }
            }
            case EscfColumnKind.STRING -> {
                ObjectTupleCursor<BytesRef> cursor = col.bytesRefCursor(false);
                int r;
                while ((r = cursor.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                    BytesRef bytes = cursor.value();
                    TsidBuilder.hashStringValue(bytes.bytes, bytes.offset, bytes.length, valueHash);
                    add(accumulator, r, dc, pathGroup, valueHash.h1, valueHash.h2);
                }
            }
            default -> throw new IllegalArgumentException("Unexpected value kind for tsid dimension: " + EscfColumnKind.name(valueKind));
        }
    }

    private static void scanBoolColumn(EscfColumn col, DimColumn dc, int pathGroup, ColumnarTsidAccumulator accumulator) {
        PresentDocIterator it = col.presentDocs();
        int r;
        while ((r = it.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
            add(accumulator, r, dc, pathGroup, TsidBuilder.BOOLEAN_VALUE_TAG, col.getBooleanValue(r) ? 1L : 0L);
        }
    }

    private static void scanUnionColumn(
        EscfColumn col,
        DimColumn dc,
        int pathGroup,
        ColumnarTsidAccumulator accumulator,
        MurmurHash3.Hash128 valueHash
    ) {
        ObjectTupleCursor<BytesRef> cursor = col.bytesRefCursor(false);
        int r;
        while ((r = cursor.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
            addUnionRow(col, r, cursor.value(), dc, pathGroup, accumulator, valueHash);
        }
    }

    private static void add(ColumnarTsidAccumulator accumulator, int row, DimColumn dc, int pathGroup, long valueH1, long valueH2) {
        accumulator.add(row, dc.pathH1(), dc.pathH2(), valueH1, valueH2, pathGroup, dc.prefixRank());
    }

    private static void addArrayElements(
        ArrayReader ar,
        int row,
        DimColumn dc,
        int pathGroup,
        ColumnarTsidAccumulator accumulator,
        MurmurHash3.Hash128 valueHash
    ) {
        while (ar.next()) {
            if (ar.isNull()) {
                continue;
            }
            byte elemType = ar.type();
            long h1;
            long h2;
            if (elemType == SourceValueType.INT || elemType == SourceValueType.LONG) {
                h1 = TsidBuilder.LONG_VALUE_TAG;
                h2 = elemType == SourceValueType.INT ? ar.intValue() : ar.longValue();
            } else if (elemType == SourceValueType.FLOAT || elemType == SourceValueType.DOUBLE) {
                h1 = TsidBuilder.DOUBLE_VALUE_TAG;
                h2 = Double.doubleToLongBits(elemType == SourceValueType.FLOAT ? ar.floatValue() : ar.doubleValue());
            } else if (elemType == SourceValueType.TRUE) {
                h1 = TsidBuilder.BOOLEAN_VALUE_TAG;
                h2 = 1L;
            } else if (elemType == SourceValueType.FALSE) {
                h1 = TsidBuilder.BOOLEAN_VALUE_TAG;
                h2 = 0L;
            } else if (elemType == SourceValueType.STRING) {
                XContentString.UTF8Bytes utf8 = ar.textValue().bytes();
                TsidBuilder.hashStringValue(utf8.bytes(), utf8.offset(), utf8.length(), valueHash);
                h1 = valueHash.h1;
                h2 = valueHash.h2;
            } else {
                throw new IllegalArgumentException(
                    "Unexpected element type " + SourceValueType.name(elemType) + " in tsid dimension array"
                );
            }
            add(accumulator, row, dc, pathGroup, h1, h2);
        }
    }

    private static void addUnionRow(
        EscfColumn col,
        int row,
        BytesRef payload,
        DimColumn dc,
        int pathGroup,
        ColumnarTsidAccumulator accumulator,
        MurmurHash3.Hash128 valueHash
    ) {
        byte typeByte = col.getTypeByte(row);
        if (typeByte == SourceValueType.NULL) {
            // Explicit null: the funnel skips VALUE_NULL, so do we.
            return;
        }
        if (typeByte == SourceValueType.FIXED_ARRAY || typeByte == SourceValueType.UNION_ARRAY) {
            addArrayElements(col.getArrayValue(row), row, dc, pathGroup, accumulator, valueHash);
            return;
        }
        long h1;
        long h2;
        if (typeByte == SourceValueType.INT) {
            h1 = TsidBuilder.LONG_VALUE_TAG;
            h2 = col.getIntValue(row);
        } else if (typeByte == SourceValueType.LONG) {
            h1 = TsidBuilder.LONG_VALUE_TAG;
            h2 = col.getLongValue(row);
        } else if (typeByte == SourceValueType.FLOAT) {
            h1 = TsidBuilder.DOUBLE_VALUE_TAG;
            h2 = Double.doubleToLongBits(col.getFloatValue(row));
        } else if (typeByte == SourceValueType.DOUBLE) {
            h1 = TsidBuilder.DOUBLE_VALUE_TAG;
            h2 = Double.doubleToLongBits(col.getDoubleValue(row));
        } else if (typeByte == SourceValueType.TRUE) {
            h1 = TsidBuilder.BOOLEAN_VALUE_TAG;
            h2 = 1L;
        } else if (typeByte == SourceValueType.FALSE) {
            h1 = TsidBuilder.BOOLEAN_VALUE_TAG;
            h2 = 0L;
        } else if (typeByte == SourceValueType.STRING) {
            // Raw UTF-8 bytes straight off the cursor, no Text wrapper and no per-row slice.
            TsidBuilder.hashStringValue(payload.bytes, payload.offset, payload.length, valueHash);
            h1 = valueHash.h1;
            h2 = valueHash.h2;
        } else if (typeByte == SourceValueType.BINARY) {
            throw new IllegalArgumentException("A BINARY value in a UNION dimension column cannot contribute to a tsid");
        } else {
            throw new IllegalArgumentException(
                "Unexpected type byte " + SourceValueType.name(typeByte) + " in UNION dimension column at row " + row
            );
        }
        add(accumulator, row, dc, pathGroup, h1, h2);
    }
}
