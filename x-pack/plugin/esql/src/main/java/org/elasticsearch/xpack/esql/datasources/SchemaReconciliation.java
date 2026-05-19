/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.SourceStatistics;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.util.ArrayList;
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
 *
 * <h2>The four schemas in an external-source query</h2>
 *
 * Four distinct schemas exist in every external-source query. In simpler modes (single file,
 * FFW, STRICT) some collapse onto each other; under UNION_BY_NAME all four are genuinely
 * distinct. Code touching {@link FileSplit#readSchema()}, {@code ExternalSourceExec.attributes},
 * or {@link ColumnMapping} reads much more clearly with these names in mind:
 *
 * <dl>
 *   <dt><b>File schema</b> (per-file, file shape)</dt>
 *   <dd>What's literally in one file. Parquet/ORC: read from the file footer. CSV/NDJSON:
 *       inferred from a byte sample. Carried per-file on {@link FileSplit#readSchema()}.</dd>
 *
 *   <dt><b>Unified schema</b> (one for the whole table)</dt>
 *   <dd>The cross-file harmonized schema. Produced here as {@link Result#unifiedSchema()}:
 *       FFW takes the anchor file's schema, STRICT validates a common schema, UBN takes the
 *       column-name union with type widening. Becomes {@code ExternalSourceExec.attributes}
 *       at first, before the optimizer's projection pruning rewrites that field.</dd>
 *
 *   <dt><b>Query schema</b> (unified shape; same for every file in the query)</dt>
 *   <dd>The subset of unified schema the query actually materializes after projection pruning.
 *       Lives on {@code ExternalSourceExec.attributes} on the wire. Drives the per-file
 *       {@link ColumnMapping} after {@link ColumnMapping#pruneToPerFileQuery}.</dd>
 *
 *   <dt><b>Per-file query schema</b> (per-file, file shape — what the reader actually produces)</dt>
 *   <dd>{@code Query schema} ∩ this file's columns, ordered to match the file's natural layout.
 *       Derived per file at split-construction time and at read time. Under FFW and STRICT it
 *       collapses to the Query schema because every file has every projected column.</dd>
 * </dl>
 *
 * <h3>Worked example (UNION_BY_NAME)</h3>
 *
 * <pre>
 *   a.csv = [name:keyword, age:int]
 *   b.csv = [age:long, name:keyword, city:keyword]
 *   query: EXTERNAL "*.csv" WITH {"schema_resolution": "union_by_name"}
 *          | KEEP name, city
 *          | SORT name
 *
 *   File schema:           a → [name:keyword, age:int]
 *                          b → [age:long, name:keyword, city:keyword]
 *   Unified schema:        [name:keyword, age:long, city:keyword]  (age widens int → long)
 *   Query schema:          [name:keyword, city:keyword]            (KEEP drops age)
 *   Per-file query schema: a → [name]                              (no city in a)
 *                          b → [name, city]                        (in b's natural order)
 * </pre>
 */
public final class SchemaReconciliation {

    private SchemaReconciliation() {}

    /**
     * Result of schema reconciliation during planning.
     *
     * @param unifiedSchema the merged/validated schema used for planning
     * @param perFileInfo per-file schema info keyed by file path
     */
    public record Result(ExternalSchema unifiedSchema, Map<StoragePath, FileSchemaInfo> perFileInfo) {}

    /**
     * Per-file schema information collected during reconciliation.
     *
     * @param fileSchema the original schema from this file
     * @param mapping column mapping from unified schema to file schema, null for identity mapping
     * @param statistics optional statistics from file metadata
     */
    public record FileSchemaInfo(ExternalSchema fileSchema, @Nullable ColumnMapping mapping, @Nullable SourceStatistics statistics) {}

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
            perFileInfo.put(filePath, new FileSchemaInfo(new ExternalSchema(fileSchema), new ColumnMapping(identity, null), stats));
        }

        return new Result(new ExternalSchema(refSchema), Map.copyOf(perFileInfo));
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
            perFileInfo.put(filePath, new FileSchemaInfo(new ExternalSchema(fileSchema), mapping, stats));
        }

        return new Result(new ExternalSchema(unifiedSchema), Map.copyOf(perFileInfo));
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
