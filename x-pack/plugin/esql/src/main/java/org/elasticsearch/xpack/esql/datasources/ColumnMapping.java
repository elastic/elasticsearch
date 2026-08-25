/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.datasources;

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
import org.elasticsearch.xpack.esql.datasources.spi.DeclaredTypeCoercions;
import org.elasticsearch.xpack.esql.datasources.spi.SkipWarnings;

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
     * <p>
     * {@code outputColumnNames} (per output slot, aligned with this mapping's width) and
     * {@code warnings} feed the per-value failure handling of the shared coercion engine
     * ({@code DeclaredTypeCoercions.castBlock}): with a live sink a value the cast cannot
     * represent (e.g. a pre-epoch or post-2262 instant under DATETIME&rarr;DATE_NANOS) nulls its
     * cell and emits a response {@code Warning} header naming the column — identical to the
     * declared-type coercion the readers run; with a {@code null} sink the failure propagates and
     * fails the page.
     */
    Page mapPage(
        Page filePage,
        BlockFactory blockFactory,
        @Nullable DataType[] fileColumnTypes,
        @Nullable String[] outputColumnNames,
        @Nullable SkipWarnings warnings
    ) {
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
                        String columnName = outputColumnNames != null ? outputColumnNames[i] : null;
                        blocks[i] = castBlock(source, sourceType, castTo, blockFactory, columnName, warnings);
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
     * Three-arg overload for callers with no column names or warnings sink (strict casts: a
     * per-value failure propagates). Equivalent to the full
     * {@link #mapPage(Page, BlockFactory, DataType[], String[], SkipWarnings)} with {@code null}
     * for both.
     */
    Page mapPage(Page filePage, BlockFactory blockFactory, @Nullable DataType[] fileColumnTypes) {
        return mapPage(filePage, blockFactory, fileColumnTypes, null, null);
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
     * Applies one reconciliation cast through the shared coercion engine
     * ({@link DeclaredTypeCoercions#castBlock}) — the SAME mechanism, values, and per-value
     * failure behavior as the readers' declared-type coercion, so a column widened by cross-file
     * schema unification reads exactly like one the user declared at the widened type. The
     * stringification (KEYWORD) casts keep their {@code TO_STRING(col)}-identical bytes: the
     * engine routes temporal sources through {@code dateTimeToString}/{@code nanoTimeToString}
     * and everything else through {@code String.valueOf}, which is what
     * {@code EsqlDataTypeConverter.numericBooleanToString} wraps.
     * <p>
     * A pair outside {@link DeclaredTypeCoercions#supports} means the mapping was built against
     * types reconciliation can never produce — fail loud (an ill-formed mapping must not limp
     * along), matching this method's historical contract.
     *
     * @param sourceType file-side ES|QL type, or {@code null} when unknown. Required to
     *                   disambiguate {@link LongBlock} sources for KEYWORD casts (DATETIME vs
     *                   DATE_NANOS vs LONG share the same block class but stringify differently).
     */
    private static Block castBlock(
        Block source,
        @Nullable DataType sourceType,
        DataType targetType,
        BlockFactory bf,
        @Nullable String columnName,
        @Nullable SkipWarnings warnings
    ) {
        DataType from = resolveSourceType(source, sourceType, targetType);
        if (DeclaredTypeCoercions.supports(from, targetType) == false) {
            throw new UnsupportedOperationException(
                "Unsupported block cast: " + source.getClass().getSimpleName() + " → " + targetType.typeName()
            );
        }
        return DeclaredTypeCoercions.castBlock(source, from, targetType, null, bf, columnName, warnings);
    }

    /**
     * Resolves the source-side ES|QL type the coercion engine dispatches on. The file's read
     * schema ({@code fileColumnTypes} via {@link #buildPerFileColumnTypes}) wins when present;
     * otherwise the block class determines it, with one seam: a {@link LongBlock} backs three
     * ES|QL types (LONG, DATETIME, DATE_NANOS). Under a DATE_NANOS target the source is DATETIME
     * (the only pair reconciliation widens into DATE_NANOS); under a KEYWORD target the three
     * stringify differently, so the type must come from the read schema — the assertion tripwires
     * any caller that forgets to thread it.
     */
    private static DataType resolveSourceType(Block source, @Nullable DataType sourceType, DataType targetType) {
        if (sourceType != null) {
            return sourceType;
        }
        if (source instanceof IntBlock) {
            return DataType.INTEGER;
        }
        if (source instanceof LongBlock) {
            assert targetType != DataType.KEYWORD
                : "LongBlock → KEYWORD cast requires sourceType to disambiguate DATETIME / DATE_NANOS / LONG; "
                    + "callers must pass perFileColumnTypes from the file's read schema";
            return targetType == DataType.DATE_NANOS ? DataType.DATETIME : DataType.LONG;
        }
        if (source instanceof DoubleBlock) {
            return DataType.DOUBLE;
        }
        if (source instanceof BooleanBlock) {
            return DataType.BOOLEAN;
        }
        if (source instanceof BytesRefBlock) {
            return DataType.KEYWORD;
        }
        throw new UnsupportedOperationException(
            "Unsupported block cast: " + source.getClass().getSimpleName() + " → " + targetType.typeName()
        );
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
