/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Per-file value describing how this file's blocks become the query's output blocks: for each
 * output column, the source position in the file's natural-order page (or {@code -1} when the
 * file lacks that column), and the cast (if any) the coordinator chose during reconciliation.
 * Built by {@link SchemaReconciliation}; consumed by {@link SchemaAdaptingIterator} (page
 * mapping) and {@code AsyncExternalSourceOperatorFactory} (pushed-filter mapping).
 * <p>
 * For the four-schema model (File / Unified / Query / Per-file query) this bridges, see the
 * class Javadoc on {@link SchemaReconciliation}.
 */
public final class ColumnMapping implements Writeable {

    /**
     * Supported widening cast targets.
     * Serialized via {@link StreamOutput#writeEnum}/{@link StreamInput#readEnum} (ordinal-based).
     * New entries must only be appended at the end; reordering or inserting breaks the wire
     * protocol. The ordinal mapping is pinned by {@code SchemaReconciliationTests#testCastTypeEnumSerialization}.
     */
    enum CastType {
        NONE(null),
        LONG(DataType.LONG),
        DOUBLE(DataType.DOUBLE),
        DATE_NANOS(DataType.DATE_NANOS);

        private static final CastType[] VALUES = values();

        private final DataType dataType;

        CastType(@Nullable DataType dataType) {
            this.dataType = dataType;
        }

        @Nullable
        private DataType toDataType() {
            return dataType;
        }

        private static CastType fromDataType(@Nullable DataType type) {
            if (type == null) return NONE;
            for (CastType ct : VALUES) {
                if (ct.dataType == type) return ct;
            }
            throw new IllegalArgumentException("Unsupported cast target type: " + type.typeName());
        }
    }

    private final int[] index;
    @Nullable
    private final DataType[] cast;

    ColumnMapping(int[] index, @Nullable DataType[] cast) {
        if (cast != null && cast.length != index.length) {
            throw new IllegalArgumentException(
                "cast array length [" + cast.length + "] must match index array length [" + index.length + "]"
            );
        }
        this.index = Arrays.copyOf(index, index.length);
        this.cast = cast != null ? Arrays.copyOf(cast, cast.length) : null;
    }

    ColumnMapping(StreamInput in) throws IOException {
        this.index = in.readIntArray();
        int castLen = in.readVInt();
        if (castLen > 0) {
            if (castLen != index.length) {
                throw new IllegalArgumentException(
                    "cast array length [" + castLen + "] must match index array length [" + index.length + "]"
                );
            }
            this.cast = new DataType[castLen];
            for (int i = 0; i < castLen; i++) {
                this.cast[i] = in.readEnum(CastType.class).toDataType();
            }
        } else {
            this.cast = null;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeIntArray(index);
        if (cast != null) {
            out.writeVInt(cast.length);
            for (DataType c : cast) {
                out.writeEnum(CastType.fromDataType(c));
            }
        } else {
            out.writeVInt(0);
        }
    }

    /** Width of this mapping — i.e. the number of output columns it produces per page. */
    int width() {
        return index.length;
    }

    int localIndex(int globalIndex) {
        return index[globalIndex];
    }

    private boolean hasMissingColumns() {
        for (int idx : index) {
            if (idx == -1) return true;
        }
        return false;
    }

    private boolean hasCasts() {
        if (cast == null) return false;
        for (DataType c : cast) {
            if (c != null) return true;
        }
        return false;
    }

    boolean isIdentity() {
        if (hasMissingColumns() || hasCasts()) {
            return false;
        }
        for (int i = 0; i < index.length; i++) {
            if (index[i] != i) {
                return false;
            }
        }
        return true;
    }

    /**
     * Prunes this mapping to the Per-file Query shape — the single transformation that bridges
     * the four-schema model on the coordinator before splits cross the wire. The output dimension
     * narrows from the {@code unifiedSchema} to the {@code querySchema} (drops columns the
     * optimizer pruned); the read dimension narrows from the {@code fileSchema} to the per-file
     * query projection (rewrites source indices from file-natural positions to projected-page
     * positions). See the four-schema doc on {@link SchemaReconciliation}.
     * <p>
     * Both halves are always called together — keeping them separate at the API level would just
     * be two callers in lockstep, with no caller benefit. Returns {@code this} when nothing needs
     * to change ({@code querySchema} covers every unified column and the file reads it whole).
     */
    ColumnMapping pruneToPerFileQuery(Schema unifiedSchema, Schema fileSchema, Schema querySchema) {
        if (unifiedSchema.isEmpty() || querySchema.isEmpty()) {
            return this;
        }
        Set<String> queryNames = querySchema.names();

        int[] fileToProjected = new int[fileSchema.size()];
        int projectedPos = 0;
        for (int f = 0; f < fileSchema.size(); f++) {
            fileToProjected[f] = queryNames.contains(fileSchema.get(f).name()) ? projectedPos++ : -1;
        }
        boolean fileReadIsIdentity = projectedPos == fileSchema.size();

        int unifiedSize = unifiedSchema.size();
        boolean outputIsIdentity = queryNames.size() == unifiedSize;
        int kept = 0;
        int[] newIndex = new int[unifiedSize];
        DataType[] newCasts = cast != null ? new DataType[unifiedSize] : null;
        for (int i = 0; i < unifiedSize; i++) {
            if (queryNames.contains(unifiedSchema.get(i).name()) == false) {
                outputIsIdentity = false;
                continue;
            }
            int src = index[i];
            newIndex[kept] = (src == -1 || fileReadIsIdentity) ? src : fileToProjected[src];
            if (newCasts != null) {
                newCasts[kept] = cast[i];
            }
            kept++;
        }
        if (outputIsIdentity && fileReadIsIdentity) {
            return this;
        }
        if (kept < unifiedSize) {
            int[] trimmed = new int[kept];
            System.arraycopy(newIndex, 0, trimmed, 0, kept);
            newIndex = trimmed;
            if (newCasts != null) {
                DataType[] trimmedCasts = new DataType[kept];
                System.arraycopy(newCasts, 0, trimmedCasts, 0, kept);
                newCasts = trimmedCasts;
            }
        }
        return new ColumnMapping(newIndex, newCasts);
    }

    /**
     * Produces the output page from a file's page: null-fill for missing columns, cast for
     * widened types, ref-counted pass-through otherwise. On mid-page failure, closes any blocks
     * already built before rethrowing.
     */
    Page mapPage(Page filePage, BlockFactory blockFactory) {
        int positions = filePage.getPositionCount();
        Block[] blocks = new Block[index.length];
        try {
            for (int i = 0; i < blocks.length; i++) {
                int localIndex = index[i];
                if (localIndex == -1) {
                    blocks[i] = blockFactory.newConstantNullBlock(positions);
                } else {
                    Block source = filePage.getBlock(localIndex);
                    DataType castTo = cast != null ? cast[i] : null;
                    if (castTo != null) {
                        blocks[i] = castBlock(source, castTo, positions, blockFactory);
                    } else {
                        source.incRef();
                        blocks[i] = source;
                    }
                }
            }
            return new Page(positions, blocks);
        } catch (Exception e) {
            Releasables.closeExpectNoException(blocks);
            throw new RuntimeException("Failed to map page", e);
        }
    }

    /**
     * Adapts pushed-down filter expressions to this file: drops conjuncts that reference columns
     * missing from this file, downcasts literals for columns the coordinator widened. Returns the
     * input list unchanged when no adaptation is needed (no missing columns and no cast).
     *
     * @param filters     pushed conjuncts, in the query's per-file-query-schema shape
     * @param querySchema the per-file query schema (one attribute per position in this mapping)
     */
    List<Expression> mapFilters(List<Expression> filters, Schema querySchema) {
        if (hasMissingColumns() == false && hasCasts() == false) {
            return filters;
        }
        Set<String> fileColumnNames = new LinkedHashSet<>();
        Map<String, DataType> fileColumnTypes = new HashMap<>();
        for (int i = 0; i < index.length; i++) {
            if (index[i] == -1) continue;
            String name = querySchema.get(i).name();
            fileColumnNames.add(name);
            if (cast != null && cast[i] != null) {
                DataType fileType = inferFileType(cast[i]);
                if (fileType != null) {
                    fileColumnTypes.put(name, fileType);
                }
            }
        }
        return FilterAdaptation.adaptFilterForFile(filters, fileColumnNames, fileColumnTypes);
    }

    private static Block castBlock(Block source, DataType targetType, int positions, BlockFactory bf) {
        if (source instanceof IntBlock intBlock) {
            if (targetType == DataType.LONG) {
                return castIntToLong(intBlock, positions, bf);
            } else if (targetType == DataType.DOUBLE) {
                return castIntToDouble(intBlock, positions, bf);
            }
        } else if (source instanceof LongBlock longBlock && targetType == DataType.DATE_NANOS) {
            return castDatetimeToDateNanos(longBlock, positions, bf);
        }
        throw new UnsupportedOperationException(
            "Unsupported block cast: " + source.getClass().getSimpleName() + " → " + targetType.typeName()
        );
    }

    private static Block castIntToLong(IntBlock intBlock, int positions, BlockFactory bf) {
        try (LongBlock.Builder builder = bf.newLongBlockBuilder(positions)) {
            for (int pos = 0; pos < positions; pos++) {
                int count = intBlock.getValueCount(pos);
                if (intBlock.isNull(pos) || count == 0) {
                    builder.appendNull();
                } else if (count == 1) {
                    builder.appendLong(intBlock.getInt(intBlock.getFirstValueIndex(pos)));
                } else {
                    int firstIdx = intBlock.getFirstValueIndex(pos);
                    builder.beginPositionEntry();
                    for (int v = 0; v < count; v++) {
                        builder.appendLong(intBlock.getInt(firstIdx + v));
                    }
                    builder.endPositionEntry();
                }
            }
            return builder.build();
        }
    }

    private static Block castIntToDouble(IntBlock intBlock, int positions, BlockFactory bf) {
        try (DoubleBlock.Builder builder = bf.newDoubleBlockBuilder(positions)) {
            for (int pos = 0; pos < positions; pos++) {
                int count = intBlock.getValueCount(pos);
                if (intBlock.isNull(pos) || count == 0) {
                    builder.appendNull();
                } else if (count == 1) {
                    builder.appendDouble(intBlock.getInt(intBlock.getFirstValueIndex(pos)));
                } else {
                    int firstIdx = intBlock.getFirstValueIndex(pos);
                    builder.beginPositionEntry();
                    for (int v = 0; v < count; v++) {
                        builder.appendDouble(intBlock.getInt(firstIdx + v));
                    }
                    builder.endPositionEntry();
                }
            }
            return builder.build();
        }
    }

    private static Block castDatetimeToDateNanos(LongBlock longBlock, int positions, BlockFactory bf) {
        try (LongBlock.Builder builder = bf.newLongBlockBuilder(positions)) {
            for (int pos = 0; pos < positions; pos++) {
                int count = longBlock.getValueCount(pos);
                if (longBlock.isNull(pos) || count == 0) {
                    builder.appendNull();
                } else if (count == 1) {
                    builder.appendLong(longBlock.getLong(longBlock.getFirstValueIndex(pos)) * 1_000_000L);
                } else {
                    int firstIdx = longBlock.getFirstValueIndex(pos);
                    builder.beginPositionEntry();
                    for (int v = 0; v < count; v++) {
                        builder.appendLong(longBlock.getLong(firstIdx + v) * 1_000_000L);
                    }
                    builder.endPositionEntry();
                }
            }
            return builder.build();
        }
    }

    /**
     * Returns the file's narrower type given a cast target — used by {@link #mapFilters} to push
     * literals through the inverse cast. {@code null} means the inversion is unsafe (e.g.
     * DOUBLE → INTEGER would truncate fractional values, breaking comparison semantics).
     */
    @Nullable
    private static DataType inferFileType(DataType castTarget) {
        if (castTarget == DataType.LONG) {
            return DataType.INTEGER;
        }
        return null;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ColumnMapping that = (ColumnMapping) o;
        return Arrays.equals(index, that.index) && Arrays.equals(cast, that.cast);
    }

    @Override
    public int hashCode() {
        int result = Arrays.hashCode(index);
        result = 31 * result + Arrays.hashCode(cast);
        return result;
    }
}
