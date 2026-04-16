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
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.SourceStatistics;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Schema reconciliation algorithms for multi-file external sources.
 * <p>
 * Supports three strategies:
 * <ul>
 *   <li>{@code FIRST_FILE_WINS} — use the first file's schema (existing behavior, no reconciliation)</li>
 *   <li>{@code STRICT} — validate all files share the exact same schema</li>
 *   <li>{@code UNION_BY_NAME} — merge schemas by column name with safe type widening</li>
 * </ul>
 * <p>
 * Type widening is intentionally conservative: only lossless promotions are allowed.
 * This is NOT {@code EsqlDataTypeConverter.commonType()}, which allows LONG→DOUBLE (lossy above 2^53).
 */
public final class SchemaReconciliation {

    private SchemaReconciliation() {}

    /**
     * Result of schema reconciliation during planning.
     *
     * @param unifiedSchema the merged/validated schema used for planning
     * @param perFileInfo per-file schema info keyed by file path
     */
    public record Result(List<Attribute> unifiedSchema, Map<StoragePath, FileSchemaInfo> perFileInfo) {}

    /**
     * Per-file schema information collected during reconciliation.
     *
     * @param fileSchema the original schema from this file
     * @param mapping column mapping from unified schema to file schema, null for identity mapping
     * @param statistics optional statistics from file metadata
     */
    public record FileSchemaInfo(List<Attribute> fileSchema, @Nullable ColumnMapping mapping, @Nullable SourceStatistics statistics) {}

    /**
     * Maps unified schema column positions to file-local column positions.
     * Handles both planning-time use (with {@link DataType} references) and wire
     * serialization (via {@link Writeable}), so there is no separate "wire" class.
     * <p>
     * Cast types are serialized via {@link StreamOutput#writeEnum}/{@link StreamInput#readEnum}
     * (ordinal-based). The ordinal mapping is pinned by an {@code assertEnumSerialization} test
     * so any reordering or insertion is caught at test time.
     * <p>
     * <b>Coordinator sharing:</b> {@code FileSplitProvider} passes the same instance
     * to all splits from the same file. A dedup cache ensures files with content-equal
     * mappings share a single object. Duplication only occurs during wire serialization.
     * <p>
     * <b>Wire-size analysis:</b> for a file split into K chunks with N unified columns,
     * the overhead is {@code K * (4*N + N)} bytes when casts are present (one VInt ordinal
     * per cast), or {@code K * 4*N} when no casts are needed. For typical schemas
     * (N &lt; 200) and split counts (K &lt; 50), this is well under 50 KB total — negligible
     * next to the data payload. See {@link CoalescedSplit} for approaches to eliminate
     * per-split duplication on the wire for very wide schemas.
     * <p>
     * <b>Bitset alternative:</b> the {@code int[]} currently encodes both "missing" ({@code -1})
     * and "local index" for present columns. A bitset could represent missing-column flags
     * in {@code ceil(N/8)} bytes and store only the present-column indices, saving ~50% on
     * the int[] for schemas where most columns are absent. This trade-off only matters for
     * very wide schemas and is left as a future optimisation.
     */
    public static final class ColumnMapping implements Writeable {

        /**
         * Supported widening cast targets.
         * Serialized via {@link StreamOutput#writeEnum}/{@link StreamInput#readEnum} (ordinal-based).
         * New entries must only be appended at the end; reordering or inserting breaks the wire
         * protocol. The ordinal mapping is pinned by {@code SchemaReconciliationTests#testCastTypeEnumSerialization}.
         */
        public enum CastType {
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
            public DataType toDataType() {
                return dataType;
            }

            public static CastType fromDataType(@Nullable DataType type) {
                if (type == null) return NONE;
                for (CastType ct : VALUES) {
                    if (ct.dataType == type) return ct;
                }
                throw new IllegalArgumentException("Unsupported cast target type: " + type.typeName());
            }
        }

        private final int[] globalToLocalIndex;
        @Nullable
        private final DataType[] casts;

        public ColumnMapping(int[] globalToLocalIndex, @Nullable DataType[] casts) {
            if (casts != null && casts.length != globalToLocalIndex.length) {
                throw new IllegalArgumentException(
                    "cast array length [" + casts.length + "] must match index array length [" + globalToLocalIndex.length + "]"
                );
            }
            this.globalToLocalIndex = Arrays.copyOf(globalToLocalIndex, globalToLocalIndex.length);
            this.casts = casts != null ? Arrays.copyOf(casts, casts.length) : null;
        }

        public ColumnMapping(StreamInput in) throws IOException {
            this.globalToLocalIndex = in.readIntArray();
            int castLen = in.readVInt();
            if (castLen > 0) {
                if (castLen != globalToLocalIndex.length) {
                    throw new IllegalArgumentException(
                        "cast array length [" + castLen + "] must match index array length [" + globalToLocalIndex.length + "]"
                    );
                }
                this.casts = new DataType[castLen];
                for (int i = 0; i < castLen; i++) {
                    this.casts[i] = in.readEnum(CastType.class).toDataType();
                }
            } else {
                this.casts = null;
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeIntArray(globalToLocalIndex);
            if (casts != null) {
                out.writeVInt(casts.length);
                for (DataType cast : casts) {
                    out.writeEnum(CastType.fromDataType(cast));
                }
            } else {
                out.writeVInt(0);
            }
        }

        public int columnCount() {
            return globalToLocalIndex.length;
        }

        public int localIndex(int globalIndex) {
            return globalToLocalIndex[globalIndex];
        }

        @Nullable
        public DataType cast(int globalIndex) {
            return casts != null ? casts[globalIndex] : null;
        }

        public boolean hasMissingColumns() {
            for (int idx : globalToLocalIndex) {
                if (idx == -1) return true;
            }
            return false;
        }

        public boolean hasCasts() {
            if (casts == null) return false;
            for (DataType cast : casts) {
                if (cast != null) return true;
            }
            return false;
        }

        public boolean isIdentity() {
            if (hasMissingColumns() || hasCasts()) {
                return false;
            }
            for (int i = 0; i < globalToLocalIndex.length; i++) {
                if (globalToLocalIndex[i] != i) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ColumnMapping that = (ColumnMapping) o;
            return Arrays.equals(globalToLocalIndex, that.globalToLocalIndex) && Arrays.equals(casts, that.casts);
        }

        @Override
        public int hashCode() {
            int result = Arrays.hashCode(globalToLocalIndex);
            result = 31 * result + Arrays.hashCode(casts);
            return result;
        }
    }

    /**
     * Safe type widening for schema reconciliation.
     * Only lossless promotions are allowed; returns null if no safe supertype exists.
     * <p>
     * Widening rules:
     * <ul>
     *   <li>INTEGER + LONG → LONG (lossless: int32 ⊆ int64)</li>
     *   <li>INTEGER + DOUBLE → DOUBLE (lossless: int32 ≤ 2^31 &lt; 2^53)</li>
     *   <li>DATETIME + DATE_NANOS → DATE_NANOS (more precise type wins)</li>
     * </ul>
     * All other cross-type pairs return null (incompatible).
     *
     * @return the widened type, or null if no safe supertype exists
     */
    @Nullable
    public static DataType schemaWiden(DataType a, DataType b) {
        if (a == b) {
            return a;
        }
        DataType wider = widenOrdered(a, b);
        if (wider != null) {
            return wider;
        }
        return widenOrdered(b, a);
    }

    @Nullable
    private static DataType widenOrdered(DataType left, DataType right) {
        if (left == DataType.INTEGER && right == DataType.LONG) {
            return DataType.LONG;
        }
        if (left == DataType.INTEGER && right == DataType.DOUBLE) {
            return DataType.DOUBLE;
        }
        if (left == DataType.DATETIME && right == DataType.DATE_NANOS) {
            return DataType.DATE_NANOS;
        }
        return null;
    }

    /**
     * STRICT reconciliation: validate all files share the exact same schema.
     * Nullability differences are tolerated; all other differences produce an error.
     *
     * @param referenceFile path of the first (reference) file
     * @param fileMetadata ordered map of file path → metadata (first entry is the reference)
     * @return reconciliation result with the reference schema and per-file info
     * @throws IllegalArgumentException if any file's schema doesn't match
     */
    public static Result reconcileStrict(StoragePath referenceFile, Map<StoragePath, SourceMetadata> fileMetadata) {
        SourceMetadata refMeta = fileMetadata.get(referenceFile);
        if (refMeta == null) {
            throw new IllegalArgumentException("Reference file not found in metadata: " + referenceFile);
        }
        List<Attribute> refSchema = refMeta.schema();

        Map<StoragePath, FileSchemaInfo> perFileInfo = new LinkedHashMap<>();

        for (Map.Entry<StoragePath, SourceMetadata> entry : fileMetadata.entrySet()) {
            StoragePath filePath = entry.getKey();
            SourceMetadata meta = entry.getValue();
            List<Attribute> fileSchema = meta.schema();
            SourceStatistics stats = meta.statistics().orElse(null);

            validateNoDuplicateColumns(filePath, fileSchema);

            if (filePath.equals(referenceFile) == false) {
                validateStrictMatch(referenceFile, refSchema, filePath, fileSchema);
            }

            int[] identity = new int[refSchema.size()];
            for (int i = 0; i < identity.length; i++) {
                identity[i] = i;
            }
            perFileInfo.put(filePath, new FileSchemaInfo(fileSchema, new ColumnMapping(identity, null), stats));
        }

        return new Result(List.copyOf(refSchema), Map.copyOf(perFileInfo));
    }

    private static void validateStrictMatch(
        StoragePath refPath,
        List<Attribute> refSchema,
        StoragePath filePath,
        List<Attribute> fileSchema
    ) {
        if (refSchema.size() != fileSchema.size()) {
            throw new IllegalArgumentException(
                "Schema mismatch in ["
                    + filePath
                    + "]: expected "
                    + refSchema.size()
                    + " columns (from reference file ["
                    + refPath
                    + "]) but found "
                    + fileSchema.size()
                    + " columns."
                    + " Hint: use schema_resolution = \"union_by_name\" to automatically merge different schemas."
            );
        }
        for (int i = 0; i < refSchema.size(); i++) {
            Attribute refAttr = refSchema.get(i);
            Attribute fileAttr = fileSchema.get(i);
            if (refAttr.name().equals(fileAttr.name()) == false) {
                throw new IllegalArgumentException(
                    "Schema mismatch in ["
                        + filePath
                        + "]: column "
                        + i
                        + " is ["
                        + fileAttr.name()
                        + "] but reference file ["
                        + refPath
                        + "] has ["
                        + refAttr.name()
                        + "]."
                        + " Hint: use schema_resolution = \"union_by_name\" to automatically merge different schemas."
                );
            }
            if (refAttr.dataType() != fileAttr.dataType()) {
                throw new IllegalArgumentException(
                    "Schema mismatch in ["
                        + filePath
                        + "]: column ["
                        + fileAttr.name()
                        + "] has type ["
                        + fileAttr.dataType().typeName()
                        + "] but reference file ["
                        + refPath
                        + "] has type ["
                        + refAttr.dataType().typeName()
                        + "]."
                        + " Hint: use schema_resolution = \"union_by_name\" to automatically merge different schemas."
                );
            }
        }
    }

    /**
     * UNION_BY_NAME reconciliation: merge schemas from all files into a superset.
     * Missing columns are NULL-filled; type differences are resolved by safe widening.
     *
     * @param fileMetadata ordered map of file path → metadata (insertion order = file sort order)
     * @return reconciliation result with unified schema and per-file mappings
     * @throws IllegalArgumentException if types are incompatible
     */
    public static Result reconcileUnionByName(Map<StoragePath, SourceMetadata> fileMetadata) {
        LinkedHashMap<String, MergeEntry> unified = new LinkedHashMap<>();

        for (Map.Entry<StoragePath, SourceMetadata> entry : fileMetadata.entrySet()) {
            StoragePath filePath = entry.getKey();
            List<Attribute> fileSchema = entry.getValue().schema();

            validateNoDuplicateColumns(filePath, fileSchema);

            for (Attribute attr : fileSchema) {
                String name = attr.name();
                MergeEntry existing = unified.get(name);
                if (existing == null) {
                    boolean attrNullable = attr.nullable() == Nullability.TRUE || attr.nullable() == Nullability.UNKNOWN;
                    unified.put(name, new MergeEntry(attr.dataType(), attrNullable, filePath));
                } else {
                    if (existing.type != attr.dataType()) {
                        DataType widened = schemaWiden(existing.type, attr.dataType());
                        if (widened == null) {
                            throw new IllegalArgumentException(
                                "Cannot merge schemas for column ["
                                    + name
                                    + "]: file ["
                                    + existing.firstSeenIn
                                    + "] has type ["
                                    + existing.type.typeName()
                                    + "], file ["
                                    + filePath
                                    + "] has type ["
                                    + attr.dataType().typeName()
                                    + "]. No compatible supertype exists."
                                    + " Consider using an explicit CAST in your query."
                            );
                        }
                        existing.type = widened;
                    }
                    boolean fileIsNullable = attr.nullable() == Nullability.TRUE || attr.nullable() == Nullability.UNKNOWN;
                    existing.nullable = existing.nullable || fileIsNullable;
                }
            }
        }

        // Mark columns as nullable when missing from any file
        for (Map.Entry<StoragePath, SourceMetadata> entry : fileMetadata.entrySet()) {
            Set<String> fileColumnNames = new HashSet<>();
            for (Attribute attr : entry.getValue().schema()) {
                fileColumnNames.add(attr.name());
            }
            for (Map.Entry<String, MergeEntry> ue : unified.entrySet()) {
                if (fileColumnNames.contains(ue.getKey()) == false) {
                    ue.getValue().nullable = true;
                }
            }
        }

        List<Attribute> unifiedSchema = new ArrayList<>(unified.size());
        for (Map.Entry<String, MergeEntry> e : unified.entrySet()) {
            String name = e.getKey();
            MergeEntry me = e.getValue();
            Nullability nullability = me.nullable ? Nullability.TRUE : Nullability.FALSE;
            unifiedSchema.add(new ReferenceAttribute(Source.EMPTY, null, name, me.type, nullability, null, false));
        }

        Map<StoragePath, FileSchemaInfo> perFileInfo = new LinkedHashMap<>();
        for (Map.Entry<StoragePath, SourceMetadata> entry : fileMetadata.entrySet()) {
            StoragePath filePath = entry.getKey();
            SourceMetadata meta = entry.getValue();
            List<Attribute> fileSchema = meta.schema();
            SourceStatistics stats = meta.statistics().orElse(null);

            ColumnMapping mapping = computeMapping(unifiedSchema, fileSchema);
            perFileInfo.put(filePath, new FileSchemaInfo(fileSchema, mapping, stats));
        }

        return new Result(List.copyOf(unifiedSchema), Map.copyOf(perFileInfo));
    }

    static ColumnMapping computeMapping(List<Attribute> unifiedSchema, List<Attribute> fileSchema) {
        Map<String, Integer> fileColumnIndex = new LinkedHashMap<>();
        Map<String, DataType> fileColumnType = new LinkedHashMap<>();
        for (int i = 0; i < fileSchema.size(); i++) {
            fileColumnIndex.put(fileSchema.get(i).name(), i);
            fileColumnType.put(fileSchema.get(i).name(), fileSchema.get(i).dataType());
        }

        int[] globalToLocal = new int[unifiedSchema.size()];
        DataType[] casts = new DataType[unifiedSchema.size()];
        boolean anyCasts = false;

        for (int i = 0; i < unifiedSchema.size(); i++) {
            Attribute unifiedAttr = unifiedSchema.get(i);
            Integer localIdx = fileColumnIndex.get(unifiedAttr.name());
            if (localIdx == null) {
                globalToLocal[i] = -1;
                casts[i] = null;
            } else {
                globalToLocal[i] = localIdx;
                DataType fileType = fileColumnType.get(unifiedAttr.name());
                if (fileType != unifiedAttr.dataType()) {
                    casts[i] = unifiedAttr.dataType();
                    anyCasts = true;
                } else {
                    casts[i] = null;
                }
            }
        }

        return new ColumnMapping(globalToLocal, anyCasts ? casts : null);
    }

    private static void validateNoDuplicateColumns(StoragePath filePath, List<Attribute> schema) {
        Set<String> seen = new HashSet<>();
        for (Attribute attr : schema) {
            if (seen.add(attr.name()) == false) {
                throw new IllegalArgumentException("File [" + filePath + "] contains duplicate column name [" + attr.name() + "].");
            }
        }
    }

    private static class MergeEntry {
        DataType type;
        boolean nullable;
        final StoragePath firstSeenIn;

        MergeEntry(DataType type, boolean nullable, StoragePath firstSeenIn) {
            this.type = type;
            this.nullable = nullable;
            this.firstSeenIn = firstSeenIn;
        }
    }

}
