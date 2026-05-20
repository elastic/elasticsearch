/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.datasources;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter;

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
     * Guards the {@link CastType#KEYWORD} entry added in this version's UBN keyword-fallback
     * work. A coordinator that produced a KEYWORD cast slot but is talking to an older data
     * node that doesn't know the ordinal would otherwise corrupt the page; we fail fast with
     * "rolling upgrade in progress" instead. See {@link #writeTo}.
     */
    private static final TransportVersion ESQL_COLUMN_MAPPING_KEYWORD_CAST = TransportVersion.fromName("esql_column_mapping_keyword_cast");

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
        DATE_NANOS(DataType.DATE_NANOS),
        /**
         * UNION_BY_NAME fallback target: file values are stringified into a
         * {@link BytesRefBlock} matching {@code TO_STRING(col)} bytes. Wire-gated by
         * {@link #ESQL_COLUMN_MAPPING_KEYWORD_CAST} — see {@link #writeTo}.
         */
        KEYWORD(DataType.KEYWORD);

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
        // Pre-flight the KEYWORD cast against the peer's transport version *before* touching the
        // output stream: a coordinator that produced this mapping under UBN is on a newer version
        // than a data node that would not recognize the ordinal. Throwing here surfaces the
        // rolling-upgrade window cleanly; the stream stays untouched so callers can recover
        // (write another message, fail the request, etc.) without a half-written ColumnMapping.
        // hasKeywordCast() short-circuits the typical no-KEYWORD path with a single null check.
        if (hasKeywordCast() && out.getTransportVersion().supports(ESQL_COLUMN_MAPPING_KEYWORD_CAST) == false) {
            throw new IllegalStateException(
                "KEYWORD cast not supported on transport version " + out.getTransportVersion() + "; rolling upgrade in progress"
            );
        }
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

    @Nullable
    DataType cast(int globalIndex) {
        return cast == null ? null : cast[globalIndex];
    }

    private boolean hasMissingColumns() {
        for (int idx : index) {
            if (idx == -1) return true;
        }
        return false;
    }

    /**
     * Returns {@code true} if any cast slot targets {@link DataType#KEYWORD}. Cheap O(width) scan
     * used to short-circuit work that's only meaningful for the UBN stringification path
     * (per-file source-type lookup; rolling-upgrade wire gate). The common multi-file UBN case
     * — every file agrees on the column's type — has no KEYWORD casts and skips that work.
     */
    boolean hasKeywordCast() {
        if (cast == null) return false;
        for (DataType c : cast) {
            if (c == DataType.KEYWORD) return true;
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
    ColumnMapping pruneToPerFileQuery(ExternalSchema unifiedSchema, ExternalSchema fileSchema, ExternalSchema querySchema) {
        assert unifiedSchema.size() == index.length
            : "unifiedSchema width [" + unifiedSchema.size() + "] disagrees with mapping width [" + index.length + "]";
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
     * <p>
     * {@code fileColumnTypes} carries the file-side ES|QL types of the reader's emitted columns
     * in the reader's natural (projected) order — used by {@link #castBlock} to disambiguate
     * {@link LongBlock} sources (DATETIME vs DATE_NANOS vs LONG share the same block class). May
     * be {@code null} when this mapping has no casts that require source-type disambiguation.
     */
    Page mapPage(Page filePage, BlockFactory blockFactory, @Nullable DataType[] fileColumnTypes) {
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
                        DataType sourceType = fileColumnTypes != null ? fileColumnTypes[localIndex] : null;
                        blocks[i] = castBlock(source, sourceType, castTo, positions, blockFactory);
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
        } catch (AssertionError e) {
            // Programmer-error tripwires (e.g. the LongBlock → KEYWORD sourceType contract) still
            // leak blocks if we don't release here. Release and rethrow so callers see the
            // original AssertionError unchanged.
            Releasables.closeExpectNoException(blocks);
            throw e;
        }
    }

    /**
     * Two-arg overload retained for callers that have no source-type information available (and
     * therefore no LongBlock-source ambiguity to resolve). Equivalent to
     * {@link #mapPage(Page, BlockFactory, DataType[])} with {@code null} for source types.
     */
    Page mapPage(Page filePage, BlockFactory blockFactory) {
        return mapPage(filePage, blockFactory, null);
    }

    /**
     * Computes the per-position file-side type array that callers pass to
     * {@link #mapPage(Page, BlockFactory, DataType[])} (via {@link SchemaAdaptingIterator}).
     * Each entry is the ES|QL type of the column the reader emits at that position, aligned with
     * the projected per-file column order ({@code perFileCols}). The synthetic
     * {@code _rowPosition} slot, when present at the tail of the reader page, is not included
     * — the iterator handles it out-of-band via {@code rowPositionInputIndex}.
     * <p>
     * Returns {@code null} when either input is missing; callers fall back to the no-source-type
     * path, which is correct for every cast that doesn't need to disambiguate {@link LongBlock}
     * sources.
     */
    @Nullable
    static DataType[] buildPerFileColumnTypes(@Nullable List<Attribute> perFileReadSchema, @Nullable List<String> perFileCols) {
        if (perFileReadSchema == null || perFileReadSchema.isEmpty() || perFileCols == null || perFileCols.isEmpty()) {
            return null;
        }
        HashMap<String, DataType> nameToType = new HashMap<>(perFileReadSchema.size());
        for (Attribute attr : perFileReadSchema) {
            nameToType.put(attr.name(), attr.dataType());
        }
        DataType[] types = new DataType[perFileCols.size()];
        for (int i = 0; i < perFileCols.size(); i++) {
            types[i] = nameToType.get(perFileCols.get(i));
        }
        return types;
    }

    /**
     * Adapts pushed-down filter expressions to this file: drops conjuncts that reference columns
     * missing from this file, downcasts literals for columns the coordinator widened. Returns the
     * input list unchanged when no adaptation is needed (no missing columns and no cast).
     *
     * @param filters     pushed conjuncts, in the query's per-file-query-schema shape
     * @param querySchema the per-file query schema (one attribute per position in this mapping)
     */
    List<Expression> mapFilters(List<Expression> filters, ExternalSchema querySchema) {
        if (hasMissingColumns() == false && hasCasts() == false) {
            return filters;
        }
        Set<String> fileColumnNames = new LinkedHashSet<>();
        Map<String, DataType> fileColumnTypes = new HashMap<>();
        for (int i = 0; i < index.length; i++) {
            if (index[i] == -1) continue;
            String name = querySchema.get(i).name();
            DataType castTo = cast != null ? cast[i] : null;
            if (castTo != null && inferFileType(castTo) == null) {
                // We have a one-way widening cast for this column. The UBN KEYWORD fallback is
                // the main case (no safe inverse for stringification: "1" < "10" is
                // lexicographic, not numeric); INT→DOUBLE (lossy truncation if inverted) and
                // DATETIME→DATE_NANOS (semantic-only widening, no integer-literal inversion)
                // hit the same `inferFileType == null` branch and are also withheld here. Today
                // only LONG→INTEGER inversion is safe, so the only cast that *adds* the column
                // is LONG with a downcast literal. RECHECK runs the original predicate against
                // the unified-shape page, preserving correctness; we forgo the row-group/file
                // skip when the cast is one-way.
                continue;
            }
            fileColumnNames.add(name);
            if (castTo != null) {
                DataType fileType = inferFileType(castTo);
                if (fileType != null) {
                    fileColumnTypes.put(name, fileType);
                }
            }
        }
        return FilterAdaptation.adaptFilterForFile(filters, fileColumnNames, fileColumnTypes);
    }

    /**
     * @param sourceType file-side ES|QL type, or {@code null} when unknown. Required to
     *                   disambiguate {@link LongBlock} sources for KEYWORD casts (DATETIME vs
     *                   DATE_NANOS vs LONG share the same block class but stringify differently).
     */
    private static Block castBlock(Block source, @Nullable DataType sourceType, DataType targetType, int positions, BlockFactory bf) {
        if (source instanceof IntBlock intBlock) {
            if (targetType == DataType.LONG) {
                return castIntToLong(intBlock, positions, bf);
            } else if (targetType == DataType.DOUBLE) {
                return castIntToDouble(intBlock, positions, bf);
            } else if (targetType == DataType.KEYWORD) {
                return castIntToKeyword(intBlock, positions, bf);
            }
        } else if (source instanceof LongBlock longBlock) {
            if (targetType == DataType.DATE_NANOS) {
                return castDatetimeToDateNanos(longBlock, positions, bf);
            } else if (targetType == DataType.KEYWORD) {
                return castLongOrDatetimeToKeyword(longBlock, sourceType, positions, bf);
            }
        } else if (source instanceof DoubleBlock doubleBlock && targetType == DataType.KEYWORD) {
            return castDoubleToKeyword(doubleBlock, positions, bf);
        } else if (source instanceof BooleanBlock booleanBlock && targetType == DataType.KEYWORD) {
            return castBooleanToKeyword(booleanBlock, positions, bf);
        } else if (source instanceof BytesRefBlock bytesRefBlock && targetType == DataType.KEYWORD) {
            // Source is already KEYWORD/TEXT bytes — a ref-bumped pass-through is the cheapest
            // honest answer. mapPage's outer try/catch closes the block on caller exceptions; the
            // caller never sees a transferred-but-unowned reference.
            bytesRefBlock.incRef();
            return bytesRefBlock;
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

    // ===== UBN KEYWORD fallback casts =====
    // Stringification must produce bytes identical to TO_STRING(col) so a column we stringified
    // and a column the user explicitly CAST compare equal under GROUP BY / JOIN / equality.
    // We achieve that by routing through EsqlDataTypeConverter helpers used by the canonical
    // TO_STRING path: numericBooleanToString for primitives/booleans, dateTimeToString/
    // nanoTimeToString (with the default formatters) for date types.

    private static Block castIntToKeyword(IntBlock intBlock, int positions, BlockFactory bf) {
        try (BytesRefBlock.Builder builder = bf.newBytesRefBlockBuilder(positions)) {
            for (int pos = 0; pos < positions; pos++) {
                int count = intBlock.getValueCount(pos);
                if (intBlock.isNull(pos) || count == 0) {
                    builder.appendNull();
                } else if (count == 1) {
                    builder.appendBytesRef(EsqlDataTypeConverter.numericBooleanToString(intBlock.getInt(intBlock.getFirstValueIndex(pos))));
                } else {
                    int firstIdx = intBlock.getFirstValueIndex(pos);
                    builder.beginPositionEntry();
                    for (int v = 0; v < count; v++) {
                        builder.appendBytesRef(EsqlDataTypeConverter.numericBooleanToString(intBlock.getInt(firstIdx + v)));
                    }
                    builder.endPositionEntry();
                }
            }
            return builder.build();
        }
    }

    /**
     * LongBlock → KEYWORD cast for all three ES|QL types backed by LongBlock: plain LONG,
     * DATETIME (millis), and DATE_NANOS. The {@code sourceType} disambiguator picks the
     * canonical formatter so the emitted bytes match {@code TO_STRING(col)}. {@code sourceType}
     * must be non-null; the UBN operator factories thread it through
     * {@link #buildPerFileColumnTypes} from the file's read schema, and the assertion below
     * tripwires any future caller that forgets to do so. UNSIGNED_LONG is intentionally out of
     * scope — no current sampler emits it for external sources covered by this cast.
     */
    private static Block castLongOrDatetimeToKeyword(LongBlock longBlock, @Nullable DataType sourceType, int positions, BlockFactory bf) {
        assert sourceType != null
            : "LongBlock → KEYWORD cast requires sourceType to disambiguate DATETIME / DATE_NANOS / LONG; "
                + "callers must pass perFileColumnTypes from the file's read schema";
        assert sourceType != DataType.UNSIGNED_LONG
            : "UNSIGNED_LONG → KEYWORD cast is not implemented; values > Long.MAX_VALUE would stringify as negative. "
                + "If a sampler starts inferring UNSIGNED_LONG, route through Long.toUnsignedString instead";
        try (BytesRefBlock.Builder builder = bf.newBytesRefBlockBuilder(positions)) {
            for (int pos = 0; pos < positions; pos++) {
                int count = longBlock.getValueCount(pos);
                if (longBlock.isNull(pos) || count == 0) {
                    builder.appendNull();
                } else if (count == 1) {
                    builder.appendBytesRef(longValueToKeyword(longBlock.getLong(longBlock.getFirstValueIndex(pos)), sourceType));
                } else {
                    int firstIdx = longBlock.getFirstValueIndex(pos);
                    builder.beginPositionEntry();
                    for (int v = 0; v < count; v++) {
                        builder.appendBytesRef(longValueToKeyword(longBlock.getLong(firstIdx + v), sourceType));
                    }
                    builder.endPositionEntry();
                }
            }
            return builder.build();
        }
    }

    private static BytesRef longValueToKeyword(long value, @Nullable DataType sourceType) {
        if (sourceType == DataType.DATETIME) {
            return new BytesRef(EsqlDataTypeConverter.dateTimeToString(value));
        }
        if (sourceType == DataType.DATE_NANOS) {
            return new BytesRef(EsqlDataTypeConverter.nanoTimeToString(value));
        }
        return EsqlDataTypeConverter.numericBooleanToString(value);
    }

    private static Block castDoubleToKeyword(DoubleBlock doubleBlock, int positions, BlockFactory bf) {
        try (BytesRefBlock.Builder builder = bf.newBytesRefBlockBuilder(positions)) {
            for (int pos = 0; pos < positions; pos++) {
                int count = doubleBlock.getValueCount(pos);
                if (doubleBlock.isNull(pos) || count == 0) {
                    builder.appendNull();
                } else if (count == 1) {
                    builder.appendBytesRef(
                        EsqlDataTypeConverter.numericBooleanToString(doubleBlock.getDouble(doubleBlock.getFirstValueIndex(pos)))
                    );
                } else {
                    int firstIdx = doubleBlock.getFirstValueIndex(pos);
                    builder.beginPositionEntry();
                    for (int v = 0; v < count; v++) {
                        builder.appendBytesRef(EsqlDataTypeConverter.numericBooleanToString(doubleBlock.getDouble(firstIdx + v)));
                    }
                    builder.endPositionEntry();
                }
            }
            return builder.build();
        }
    }

    private static Block castBooleanToKeyword(BooleanBlock booleanBlock, int positions, BlockFactory bf) {
        try (BytesRefBlock.Builder builder = bf.newBytesRefBlockBuilder(positions)) {
            for (int pos = 0; pos < positions; pos++) {
                int count = booleanBlock.getValueCount(pos);
                if (booleanBlock.isNull(pos) || count == 0) {
                    builder.appendNull();
                } else if (count == 1) {
                    builder.appendBytesRef(
                        EsqlDataTypeConverter.numericBooleanToString(booleanBlock.getBoolean(booleanBlock.getFirstValueIndex(pos)))
                    );
                } else {
                    int firstIdx = booleanBlock.getFirstValueIndex(pos);
                    builder.beginPositionEntry();
                    for (int v = 0; v < count; v++) {
                        builder.appendBytesRef(EsqlDataTypeConverter.numericBooleanToString(booleanBlock.getBoolean(firstIdx + v)));
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
