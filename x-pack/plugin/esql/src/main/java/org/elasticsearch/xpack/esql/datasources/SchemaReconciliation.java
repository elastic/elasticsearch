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
import org.elasticsearch.xpack.esql.datasources.spi.SkipWarnings;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.SourceStatistics;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
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
 * <p>
 * Under {@code UNION_BY_NAME}, any pair the lossless table cannot widen falls back to
 * {@link DataType#KEYWORD} (the cross-type join): values from numerically-typed files are stringified
 * via {@code ColumnMapping}'s per-block cast and a single response {@code Warning} header per
 * affected column tells the user what happened. This matches the industry baseline (DuckDB,
 * ClickHouse, Spark all widen to string as the cross-type floor) and turns "samplers disagreed"
 * — the normal steady state for sampling-based readers — from a hard error into a benign
 * widening. Users who want the strict-mismatch error can opt into {@code schema_resolution =
 * "strict"} which still throws.
 * <p>
 * The lossy {@code LONG + DOUBLE} pair is *not* covered by the lossless table on purpose
 * (precision loss above 2^53). Under UBN it goes to {@code KEYWORD}, which is louder and safer
 * than silent precision loss; the lossless table itself stays unchanged.
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
    public record FileSchemaInfo(
        ExternalSchema fileSchema,
        @Nullable ColumnMapping mapping,
        @Nullable SourceStatistics statistics,
        // PRE-overlay file types, physical-keyed; null means fileSchema IS the inferred schema (no declared overlay ran),
        // so callers fall back to the fileSchema attributes' types (today's behavior). Populated only where the declared
        // overlay rebuilds this info (ExternalSourceResolver.applyNonStrictOverlay) so the split-level stats boundary can
        // normalize footer stats with the file's real inferred types instead of the overlaid declared types.
        @Nullable Map<String, DataType> inferredTypes
    ) {
        public FileSchemaInfo(ExternalSchema fileSchema, @Nullable ColumnMapping mapping, @Nullable SourceStatistics statistics) {
            this(fileSchema, mapping, statistics, null);
        }
    }

    /**
     * Safe type widening for schema reconciliation.
     * Only lossless promotions are allowed; returns {@code null} if no safe supertype exists.
     * <p>
     * Widening rules:
     * <ul>
     *   <li>INTEGER + LONG → LONG (lossless: int32 ⊆ int64)</li>
     *   <li>INTEGER + DOUBLE → DOUBLE (lossless: int32 ≤ 2^31 &lt; 2^53)</li>
     *   <li>DATETIME + DATE_NANOS → DATE_NANOS (more precise type wins)</li>
     * </ul>
     * All other cross-type pairs return null (no lossless supertype). UBN reconciliation
     * additionally falls back to {@link DataType#KEYWORD} for those — see
     * {@link #widenToCommonOrKeyword} and {@link #reconcileUnionByName}. LONG + DOUBLE
     * deliberately stays out of this table (precision loss above 2^53) and is therefore one of
     * the pairs that goes to {@code KEYWORD} under UBN.
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
     * UNION_BY_NAME widening: returns {@link #schemaWiden}'s result when one exists, otherwise
     * falls back to {@link DataType#KEYWORD} as the cross-type join (lossy for numerics — but
     * the lossy path is the one that triggers a response {@code Warning} so users see when
     * stringification happened). Never returns null: every cross-type pair has a defined UBN
     * answer.
     * <p>
     * This is the UBN-specific entry point; {@link #schemaWiden} is intentionally kept as a
     * separate {@code @Nullable}-returning method so callers that want the strict lossless-only
     * semantic still have it. The two stay aligned by construction — the KEYWORD branch here
     * fires only on inputs where {@code schemaWiden} would have returned null.
     */
    private static DataType widenToCommonOrKeyword(DataType a, DataType b) {
        DataType widened = schemaWiden(a, b);
        return widened != null ? widened : DataType.KEYWORD;
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
     * Missing columns are NULL-filled; type differences are resolved by safe widening or, when no
     * lossless supertype exists, by falling back to {@link DataType#KEYWORD} with a per-column
     * {@code Warning} response header. See the class javadoc for the rationale and the lattice
     * picture.
     *
     * @param fileMetadata ordered map of file path → metadata (insertion order = file sort order)
     * @return reconciliation result with unified schema and per-file mappings
     */
    public static Result reconcileUnionByName(Map<StoragePath, SourceMetadata> fileMetadata) {
        LinkedHashMap<String, MergeEntry> unified = new LinkedHashMap<>();
        // Per-column accumulator. We record *every* file's inferred type for every column up
        // front (it's cheap and gives the warning emitter a complete contributor list), then
        // decide at the end whether the column actually degraded to KEYWORD and a warning is
        // warranted. Building this lazily inside the merge branch would lose pre-merge files
        // when a column finally degrades on its third or later file.
        LinkedHashMap<String, KeywordFallback> contributions = new LinkedHashMap<>();

        for (Map.Entry<StoragePath, SourceMetadata> entry : fileMetadata.entrySet()) {
            StoragePath filePath = entry.getKey();
            List<Attribute> fileSchema = entry.getValue().schema();

            validateNoDuplicateColumns(filePath, fileSchema);

            for (Attribute attr : fileSchema) {
                String name = attr.name();
                contributions.computeIfAbsent(name, KeywordFallback::new).add(filePath, attr.dataType());
                MergeEntry existing = unified.get(name);
                if (existing == null) {
                    boolean attrNullable = attr.nullable() == Nullability.TRUE || attr.nullable() == Nullability.UNKNOWN;
                    unified.put(name, new MergeEntry(attr.dataType(), attrNullable, filePath));
                } else {
                    if (existing.type != attr.dataType()) {
                        existing.type = widenToCommonOrKeyword(existing.type, attr.dataType());
                    }
                    boolean fileIsNullable = attr.nullable() == Nullability.TRUE || attr.nullable() == Nullability.UNKNOWN;
                    existing.nullable = existing.nullable || fileIsNullable;
                }
            }
        }

        emitKeywordFallbackWarnings(unified, contributions);

        // Resolve esql-planning#1050 before the nullable-fill pass below: collapse any field that
        // some files infer as a scalar leaf and others infer as a nested object (dotted-prefix
        // parent) to a single shape, dropping the losing shape's entries from `unified` in place.
        // The nullable-fill pass then naturally marks the surviving name nullable for the losing
        // files (their original schema genuinely lacks it), which is exactly what we want.
        Map<StoragePath, List<Attribute>> shapeConflictOverrides = resolveShapeConflicts(unified, fileMetadata);

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
            // A file on the losing side of a shape conflict is pinned to the winning shape's
            // attribute(s) instead of its own inferred sub-schema for that field — see
            // resolveShapeConflicts for why this is what actually routes the file's real values
            // through the per-file shape-conflict/ErrorPolicy handling at read time.
            List<Attribute> fileSchema = shapeConflictOverrides.getOrDefault(filePath, meta.schema());
            SourceStatistics stats = meta.statistics().orElse(null);

            ColumnMapping mapping = computeMapping(unifiedSchema, fileSchema);
            perFileInfo.put(filePath, new FileSchemaInfo(new ExternalSchema(fileSchema), mapping, stats));
        }

        return new Result(new ExternalSchema(unifiedSchema), Map.copyOf(perFileInfo));
    }

    /**
     * Detects and resolves esql-planning#1050: a field that some files infer as a scalar leaf and
     * others infer as a nested object (a dotted-prefix parent, e.g. {@code user} vs
     * {@code user.id}/{@code user.tier}) is a schema conflict across files, exactly like a
     * within-file shape conflict is for a single file (elastic/esql-planning#1028).
     * {@code UNION_BY_NAME} merges purely by exact name, so the two shapes never collide there and
     * both get fabricated into the unified schema — this pass collapses each such family to a
     * single shape: the first file (in {@code fileMetadata} iteration order) to contribute the
     * family at all wins, mirroring both #1028's first-observed-shape rule and
     * {@code FIRST_FILE_WINS}'s anchor semantics.
     * <p>
     * Mutates {@code unified} in place, removing the losing shape's entries so they never reach
     * the unified schema. Returns a per-file override of {@link SourceMetadata#schema()} for the
     * losing files: their own inferred sub-schema for the family is replaced by the winning
     * attribute(s) taken straight from the (possibly widened) {@code unified} entries. That
     * override becomes, via the caller, the losing file's {@link FileSchemaInfo#fileSchema()} —
     * which is exactly what {@code FileSplitProvider} pins the reader's {@code readSchema} to. So
     * when the reader for a losing file later hits that file's real, differently-shaped JSON
     * value on the now-pinned winning attribute, the existing per-file shape-conflict handling
     * (e.g. {@code NdJsonPageDecoder}'s {@code shapeConflict}, added for #1028) fires
     * automatically and routes it through {@link org.elasticsearch.xpack.esql.datasources.spi.ErrorPolicy}
     * — no format-specific code needed here.
     * <p>
     * A file that contributes <em>both</em> the bare name and a dotted child for the same root
     * (a literal flat key such as {@code "user.tag"} coexisting with scalar {@code "user"} in one
     * file) still fully participates in the vote above as a leaf-shaped contributor — presence of
     * the bare name means its dotted column(s) are literal flat keys, not nested children (see
     * {@code NdJsonPageDecoder#hasDottedPrefixConflict}), so only those unrelated dotted columns
     * are excluded from the family; see {@link #resolveFamily} for why exempting the file's leaf
     * contribution too would silently reopen the conflict.
     * <p>
     * Only files whose {@link SourceMetadata#sourceType()} is {@link #supportsShapeConflictResolution
     * shape-conflict-capable} ever enter this family vote — see that method for why. A file from
     * any other format that happens to carry a name matching {@code root} or {@code root.*} is
     * therefore never touched here, however it looks lexically: e.g. a CSV file with a literal
     * {@code user.tag} header alongside another (CSV or NDJSON) file's scalar {@code user} column
     * is not a shape conflict — both are ordinary, independent columns and both survive in the
     * unified schema, one NULL-filled in whichever file lacks it, exactly like any other pair of
     * unrelated column names would.
     */
    private static Map<StoragePath, List<Attribute>> resolveShapeConflicts(
        LinkedHashMap<String, MergeEntry> unified,
        Map<StoragePath, SourceMetadata> fileMetadata
    ) {
        Set<String> namesFromCapableFiles = new LinkedHashSet<>();
        for (Map.Entry<StoragePath, SourceMetadata> entry : fileMetadata.entrySet()) {
            if (supportsShapeConflictResolution(entry.getValue().sourceType()) == false) {
                continue;
            }
            for (Attribute attr : entry.getValue().schema()) {
                namesFromCapableFiles.add(attr.name());
            }
        }
        List<String> familyRoots = findFamilyRoots(namesFromCapableFiles);
        if (familyRoots.isEmpty()) {
            return Map.of();
        }
        // Shallowest roots first: once a shallower family is resolved its losing names are
        // removed from `unified`, so a deeper candidate root re-derives its family membership
        // from what's actually still there rather than from a stale upfront snapshot.
        familyRoots.sort(Comparator.comparingInt(SchemaReconciliation::dotDepth));

        Map<StoragePath, List<Attribute>> overrides = new LinkedHashMap<>();
        List<FamilyConflict> conflicts = new ArrayList<>();
        for (String root : familyRoots) {
            FamilyConflict conflict = resolveFamily(root, unified, fileMetadata);
            if (conflict == null) {
                continue;
            }
            conflicts.add(conflict);
            for (String droppedName : conflict.droppedNames()) {
                unified.remove(droppedName);
            }
            for (Map.Entry<StoragePath, List<String>> losing : conflict.losingFileNames().entrySet()) {
                StoragePath losingFile = losing.getKey();
                List<String> ownFamilyNames = losing.getValue();
                List<Attribute> override = overrides.computeIfAbsent(losingFile, f -> new ArrayList<>(fileMetadata.get(f).schema()));
                override.removeIf(a -> ownFamilyNames.contains(a.name()));
                override.addAll(conflict.winningAttributes());
            }
        }
        emitShapeConflictWarnings(conflicts);
        return overrides;
    }

    /**
     * Whether {@code sourceType} may participate in {@link #resolveShapeConflicts}: its reader
     * must both (a) genuinely flatten nested objects into dotted attribute names, so a
     * {@code root}/{@code root.*} pair can actually mean "scalar in one file, object in another"
     * rather than two unrelated literal names, and (b) have a per-file, read-time mechanism that
     * routes a pinned-shape mismatch through {@link org.elasticsearch.xpack.esql.datasources.spi.ErrorPolicy}
     * once this pass overrides a losing file's {@code readSchema} — see {@link #resolveShapeConflicts}'s
     * javadoc for how that override is used downstream.
     * <p>
     * Today only NDJSON satisfies both: {@code NdJsonSchemaInferrer} flattens
     * {@code {"user": {"id": ...}}} into {@code user.id}, and {@code NdJsonPageDecoder}'s
     * {@code shapeConflict} handling (elastic/esql-planning#1028) is exactly the read-time fallback
     * (a) needs. Every other format fails at least one requirement, and enabling it there would
     * silently drop <em>valid</em> UBN columns instead of resolving a real conflict:
     * <ul>
     *   <li>CSV/TSV headers are always literal — a {@code "."} never means nesting, so
     *       {@code user} and {@code user.tag} in two CSV files are simply two unrelated columns,
     *       not a shape conflict.</li>
     *   <li>Iceberg never flattens structs (they're skipped as {@code UNSUPPORTED}), so any dotted
     *       name it does surface is, like CSV, always a literal top-level column name.</li>
     *   <li>Parquet and ORC <em>do</em> flatten nested structs into dotted names (satisfying (a)),
     *       but neither reader has an equivalent of NDJSON's {@code shapeConflict} fallback for a
     *       column pinned to a shape that disagrees with the file's actual footer-declared type
     *       (failing (b)) — so overriding a losing Parquet/ORC file's {@code readSchema} here would
     *       misfire (e.g. a read-time type error) instead of gracefully degrading through
     *       {@code ErrorPolicy}.</li>
     * </ul>
     */
    private static boolean supportsShapeConflictResolution(String sourceType) {
        return "ndjson".equals(sourceType);
    }

    private static int dotDepth(String name) {
        int depth = 0;
        for (int i = 0; i < name.length(); i++) {
            if (name.charAt(i) == '.') {
                depth++;
            }
        }
        return depth;
    }

    /**
     * Returns every name in {@code names} that is a "family root": some other name in the set is
     * {@code root + "." + suffix}. Quadratic in the (typically small, per-query-bounded) column
     * count — the same trade-off {@link #validateNoDuplicateColumns} and the rest of this class
     * already make for per-name work at reconciliation time.
     */
    private static List<String> findFamilyRoots(Set<String> names) {
        List<String> roots = new ArrayList<>();
        for (String candidate : names) {
            String prefix = candidate + ".";
            for (String other : names) {
                if (other.startsWith(prefix)) {
                    roots.add(candidate);
                    break;
                }
            }
        }
        return roots;
    }

    /**
     * Classifies every file's contribution to the {@code root} family as leaf-shaped (has the
     * bare {@code root} name — even if it also carries unrelated {@code root.*} flat keys, see
     * below), dotted-shaped (has some {@code root.*} name but not {@code root} itself) or absent,
     * then resolves a conflict when both shapes are contributed by different files. Returns
     * {@code null} when there is no actual cross-file conflict for this family (a single
     * contributor, or every contributor agrees).
     * <p>
     * A file that has the bare {@code root} name always classifies as leaf-shaped for this
     * family, full stop, regardless of whether it also happens to carry some unrelated
     * {@code root.*} column: presence of {@code root} itself means any {@code root.*} names in
     * that <em>same</em> file are literal flat keys, not nested children of {@code root} (see
     * {@code NdJsonPageDecoder#hasDottedPrefixConflict}), so they take no part in this family
     * either way — they are simply excluded from {@code familyNamesInFile} below and therefore
     * never touched by the winning/losing overrides. The file's actual {@code root} value,
     * though, is a real, ordinary leaf contribution and must fully participate in the cross-file
     * win/loss vote like any other file's bare column — exempting it entirely (as an earlier
     * version of this method did) let such a file's scalar {@code root} silently keep coexisting
     * with a winning nested shape from another file, reopening the exact ambiguity this method
     * exists to close.
     * <p>
     * A file whose format is not {@link #supportsShapeConflictResolution shape-conflict-capable}
     * (e.g. CSV, Iceberg, Parquet, ORC) is skipped outright, regardless of whether it happens to
     * carry {@code root} or a {@code root.*} name — see {@link #resolveShapeConflicts} for why
     * such names there are always literal/independent, never a genuine shape conflict.
     */
    @Nullable
    private static FamilyConflict resolveFamily(
        String root,
        LinkedHashMap<String, MergeEntry> unified,
        Map<StoragePath, SourceMetadata> fileMetadata
    ) {
        String dottedPrefix = root + ".";
        Boolean winningShapeIsLeaf = null;
        StoragePath winningFile = null;
        // Every family-member name (the bare root, or a genuinely nested root.* child) mapped to
        // the set of files whose own schema contributes it — used below so a name is only ever
        // dropped from the unified schema when *every* one of its contributors is on the losing
        // side; a name a kept (non-losing) file also relies on must survive untouched. Note this
        // never contains a root.* name from a file that also has the bare root itself — see the
        // class javadoc above for why those are excluded from the family entirely.
        Map<String, Set<StoragePath>> contributorsByName = new LinkedHashMap<>();
        // Losing files mapped to exactly the family-member names *they* contribute, so each
        // file's override only ever removes its own columns, never another file's.
        LinkedHashMap<StoragePath, List<String>> losingFileNames = new LinkedHashMap<>();

        for (Map.Entry<StoragePath, SourceMetadata> entry : fileMetadata.entrySet()) {
            if (supportsShapeConflictResolution(entry.getValue().sourceType()) == false) {
                continue;
            }
            boolean hasLeaf = false;
            List<String> dottedNames = new ArrayList<>();
            for (Attribute attr : entry.getValue().schema()) {
                String name = attr.name();
                if (name.equals(root)) {
                    hasLeaf = true;
                } else if (name.startsWith(dottedPrefix)) {
                    dottedNames.add(name);
                }
            }

            List<String> familyNamesInFile;
            boolean fileShapeIsLeaf;
            if (hasLeaf) {
                familyNamesInFile = List.of(root);
                fileShapeIsLeaf = true;
            } else if (dottedNames.isEmpty() == false) {
                familyNamesInFile = dottedNames;
                fileShapeIsLeaf = false;
            } else {
                continue; // file doesn't touch this family at all
            }

            for (String name : familyNamesInFile) {
                contributorsByName.computeIfAbsent(name, n -> new LinkedHashSet<>()).add(entry.getKey());
            }
            if (winningShapeIsLeaf == null) {
                winningShapeIsLeaf = fileShapeIsLeaf;
                winningFile = entry.getKey();
            } else if (fileShapeIsLeaf != winningShapeIsLeaf) {
                losingFileNames.put(entry.getKey(), familyNamesInFile);
            }
        }

        if (losingFileNames.isEmpty()) {
            return null;
        }

        // Restricted to contributorsByName's keys (true family members only) rather than a plain
        // name/dottedPrefix match against `unified`: an unrelated root.* flat key owned by some
        // other leaf-shaped file (excluded above) must never be pulled into the winning shape.
        List<Attribute> winningAttributes = new ArrayList<>();
        for (String name : unified.keySet()) {
            if (contributorsByName.containsKey(name) == false) {
                continue;
            }
            boolean isLeafName = name.equals(root);
            if (isLeafName == winningShapeIsLeaf) {
                MergeEntry me = unified.get(name);
                Nullability nullability = me.nullable ? Nullability.TRUE : Nullability.FALSE;
                winningAttributes.add(new ReferenceAttribute(Source.EMPTY, null, name, me.type, nullability, null, false));
            }
        }

        Set<StoragePath> losingFiles = losingFileNames.keySet();
        LinkedHashSet<String> droppedNames = new LinkedHashSet<>();
        for (List<String> ownNames : losingFileNames.values()) {
            for (String name : ownNames) {
                if (losingFiles.containsAll(contributorsByName.get(name))) {
                    droppedNames.add(name);
                }
            }
        }

        return new FamilyConflict(root, winningFile, winningShapeIsLeaf, winningAttributes, droppedNames, losingFileNames);
    }

    /**
     * A resolved esql-planning#1050 conflict for one field family: {@code winningFile} kept its
     * shape ({@code winningAttributes}, family root {@code root}); every file key in
     * {@code losingFileNames} contributed the other shape and had its own listed family names
     * removed from the unified schema (to the extent {@code droppedNames} allows — see
     * {@link #resolveFamily}) and overridden to {@code winningAttributes} in its own
     * {@code fileSchema()} pin.
     */
    private record FamilyConflict(
        String root,
        StoragePath winningFile,
        boolean winningShapeIsLeaf,
        List<Attribute> winningAttributes,
        Set<String> droppedNames,
        Map<StoragePath, List<String>> losingFileNames
    ) {
        String buildDetail() {
            StoragePath losingExample = losingFileNames.keySet().iterator().next();
            String winningShape = winningShapeIsLeaf ? "a scalar" : "an object";
            String losingShape = winningShapeIsLeaf ? "an object" : "a scalar";
            StringBuilder sb = new StringBuilder("Field [").append(root)
                .append("] is ")
                .append(winningShape)
                .append(" in [")
                .append(winningFile)
                .append("] but ")
                .append(losingShape)
                .append(" in [")
                .append(losingExample)
                .append("]");
            if (losingFileNames.size() > 1) {
                sb.append(" (+").append(losingFileNames.size() - 1).append(" more)");
            }
            sb.append("; kept the [")
                .append(winningFile)
                .append("] shape [")
                .append(String.join(", ", winningAttributes.stream().map(Attribute::name).toList()))
                .append("], dropped [")
                .append(String.join(", ", droppedNames))
                .append("] from the unified schema. The conflicting file(s)' values for [")
                .append(root)
                .append("] are handled per the configured error policy at read time.");
            return sb.toString();
        }
    }

    /**
     * Fire-and-forget emit of one response {@code Warning} per resolved shape conflict, via the
     * same {@link SkipWarnings} pattern as {@link #emitKeywordFallbackWarnings}.
     */
    private static void emitShapeConflictWarnings(List<FamilyConflict> conflicts) {
        if (conflicts.isEmpty()) {
            return;
        }
        SkipWarnings warnings = new SkipWarnings(
            "Schema reconciliation resolved cross-file scalar/object shape conflicts (esql-planning#1050) by"
                + " keeping the first file's shape; make the field's shape consistent across files, or declare it"
                + " explicitly, to avoid this."
        );
        for (FamilyConflict conflict : conflicts) {
            warnings.add(conflict.buildDetail());
        }
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

    private static boolean isStringType(DataType type) {
        return type == DataType.KEYWORD || type == DataType.TEXT;
    }

    /**
     * Maximum number of contributing file paths quoted in a single per-column warning detail.
     * Keeps the warning header from blowing up on glob-of-thousands queries; the "+N more" suffix
     * preserves the cardinality so users know the warning applies to more files than shown.
     */
    private static final int MAX_FILES_IN_WARNING_DETAIL = 3;

    private static void emitKeywordFallbackWarnings(
        LinkedHashMap<String, MergeEntry> unified,
        LinkedHashMap<String, KeywordFallback> contributions
    ) {
        // Decide which columns warrant a warning: column degraded to KEYWORD *and* at least one
        // contributing file inferred a non-string type. A column that was KEYWORD in every file
        // (and stayed KEYWORD) is not a degradation — the user-visible type matches the on-disk
        // inferences and nothing was stringified.
        List<KeywordFallback> warned = new ArrayList<>();
        for (Map.Entry<String, MergeEntry> e : unified.entrySet()) {
            if (e.getValue().type != DataType.KEYWORD) {
                continue;
            }
            KeywordFallback fb = contributions.get(e.getKey());
            if (fb != null && fb.hasNonStringContributor()) {
                warned.add(fb);
            }
        }
        if (warned.isEmpty()) {
            return;
        }
        // Fire-and-forget: SkipWarnings#add deposits headers on the current thread context via
        // HeaderWarning.addWarning. The local is not stored anywhere — the side effect *is* the
        // emit. Same pattern as other SkipWarnings callers (e.g. format readers under non-strict
        // error policy).
        SkipWarnings warnings = new SkipWarnings(
            "Schema reconciliation widened columns to keyword due to cross-file type disagreement;"
                + " values are returned as strings. Hint: use schema_resolution = \"strict\" to fail instead."
        );
        for (KeywordFallback fb : warned) {
            warnings.add(fb.buildDetail());
        }
    }

    /**
     * Per-column accumulator: every file that contributed a value for the column, together with
     * that file's inferred type. Insertion-ordered so the emitted message reflects the user's
     * glob order. Recording is unconditional during merge; the emit step decides whether the
     * column actually degraded to {@code KEYWORD} and only then turns this into a warning.
     */
    private static final class KeywordFallback {
        private final String columnName;
        private final LinkedHashMap<StoragePath, DataType> contributions = new LinkedHashMap<>();

        KeywordFallback(String columnName) {
            this.columnName = columnName;
        }

        void add(StoragePath file, DataType inferredType) {
            // First inference wins per (column, file). A single file can't contribute two
            // different types for the same column (validateNoDuplicateColumns guarantees
            // unique names within a file), so putIfAbsent and put are equivalent here — use
            // putIfAbsent for clarity-of-intent.
            contributions.putIfAbsent(file, inferredType);
        }

        boolean hasNonStringContributor() {
            for (DataType type : contributions.values()) {
                if (isStringType(type) == false) {
                    return true;
                }
            }
            return false;
        }

        String buildDetail() {
            // Pair each file with its inferred type — "file (type), file (type), …" — so users
            // can tell at a glance which file disagreed instead of cross-referencing two lists.
            // Long file lists are truncated with a "+N more" suffix; the distinct-type roll-up
            // at the end preserves the legacy summary so users get an at-a-glance type picture
            // even when files are truncated.
            StringBuilder sb = new StringBuilder("Column [").append(columnName).append("] widened to keyword: ");
            int shown = 0;
            int total = contributions.size();
            for (Map.Entry<StoragePath, DataType> e : contributions.entrySet()) {
                if (shown == MAX_FILES_IN_WARNING_DETAIL && total > MAX_FILES_IN_WARNING_DETAIL) {
                    sb.append(", +").append(total - shown).append(" more");
                    break;
                }
                if (shown > 0) {
                    sb.append(", ");
                }
                sb.append(e.getKey()).append(" (").append(e.getValue().typeName()).append(")");
                shown++;
            }
            LinkedHashSet<DataType> distinctTypes = new LinkedHashSet<>(contributions.values());
            if (distinctTypes.size() > 1) {
                sb.append("; distinct types: [");
                int t = 0;
                for (DataType type : distinctTypes) {
                    if (t > 0) {
                        sb.append(", ");
                    }
                    sb.append(type.typeName());
                    t++;
                }
                sb.append("]");
            }
            return sb.toString();
        }
    }

}
