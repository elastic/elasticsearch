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
import org.elasticsearch.xpack.esql.datasources.spi.TypeWidening;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

/**
 * Schema reconciliation algorithms for multi-file external sources.
 * <p>
 * Supports three strategies:
 * <ul>
 *   <li>{@code FIRST_FILE_WINS}: use the first file's schema (existing behavior, no reconciliation)</li>
 *   <li>{@code STRICT}: validate all files share the exact same schema (NDJSON column order is ignored)</li>
 *   <li>{@code UNION_BY_NAME}: merge schemas by column name with type widening</li>
 * </ul>
 * <p>
 * {@link TypeWidening#join} is the total answer: lossless promotions ({@code INTEGER → LONG},
 * {@code INTEGER → DOUBLE}, {@code DATETIME → DATE_NANOS}), the lossy {@code LONG → DOUBLE}
 * promotion (integers above {@code 2^53} are not exact), and {@link DataType#KEYWORD} as the
 * cross-type top. {@link #schemaWiden} is the lossless form and stays {@code null} for
 * {@code LONG + DOUBLE}. {@code schema_resolution = "strict"} still rejects on exact type
 * inequality ({@link #validateStrictTypeMatch}); it does not call {@link #schemaWiden}.
 * <p>
 * Under {@code UNION_BY_NAME}, a {@code LONG + DOUBLE} column becomes {@code DOUBLE} with a
 * precision-loss warning. Any other pair with no closer supertype falls back to
 * {@link DataType#KEYWORD} with a stringification warning. How the wider type is produced depends
 * on the format: columnar files (Parquet, ORC) read the physically-typed value and cast it via
 * {@code ColumnMapping}, while text files (CSV, TSV, NDJSON) are pinned to the reconciled type
 * and read it at that type directly (see {@code readsColumnsAtReconciledType}). Sampler
 * disagreement is the normal steady state for sampling-based readers; users who want the
 * mismatch to fail can opt into {@code schema_resolution = "strict"}.
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
 *       inferred from a byte sample. Carried per-file on {@link FileSplit#readSchema()} as the
 *       reader's read pin. Under UNION_BY_NAME this pin is the effective read schema after
 *       reconciliation overrides: for text formats a widened column is pinned to its reconciled
 *       type so the reader reads at that type directly rather than the narrower sampled type.</dd>
 *
 *   <dt><b>Unified schema</b> (one for the whole table)</dt>
 *   <dd>The cross-file harmonized schema. Produced here as {@link Result#unifiedSchema()}:
 *       FFW takes the anchor file's schema, STRICT validates a common schema, UBN takes the
 *       column-name union with type widening. NDJSON STRICT preserves the anchor's inferred
 *       order while mapping other files' columns by name. Becomes {@code ExternalSourceExec.attributes}
 *       at first, before the optimizer's projection pruning rewrites that field.</dd>
 *
 *   <dt><b>Query schema</b> (unified shape; same for every file in the query)</dt>
 *   <dd>The subset of unified schema the query actually materializes after projection pruning.
 *       Lives on {@code ExternalSourceExec.attributes} on the wire. Drives the per-file
 *       {@link ColumnMapping} after {@link ColumnMapping#pruneToPerFileQuery}.</dd>
 *
 *   <dt><b>Per-file query schema</b> (per-file, file shape — what the reader actually produces)</dt>
 *   <dd>{@code Query schema} ∩ this file's columns, ordered to match the file's natural layout.
 *       Derived per file at split-construction time and at read time. Under FFW and ordered-format
 *       STRICT it collapses to the Query schema because every file has every projected column;
 *       NDJSON STRICT may retain a different inferred order and is mapped by name.</dd>
 * </dl>
 *
 * <h3>Worked example (UNION_BY_NAME)</h3>
 *
 * <pre>
 *   a.csv = [name:keyword, age:int]
 *   b.csv = [age:long, name:keyword, city:keyword]
 *   dataset "my_dataset" over *.csv, configured with schema_resolution=union_by_name
 *   query: FROM my_dataset
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
     * @param fileSchema the effective read schema the reader is pinned to for this file: the file's
     *                   inferred/footer schema, with a text format's columns pinned to their reconciled types
     * @param mapping column mapping from unified schema to file schema, null for identity mapping
     * @param statistics optional statistics from file metadata
     */
    public record FileSchemaInfo(
        ExternalSchema fileSchema,
        @Nullable ColumnMapping mapping,
        @Nullable SourceStatistics statistics,
        // PRE-retype file types, physical-keyed; null means fileSchema IS the inferred schema (nothing retyped this file),
        // so callers fall back to the fileSchema attributes' types (today's behavior). Three populating sites:
        // - the UNION_BY_NAME pin (reconcileUnionByName / pinToReconciledTypes), with the full pre-pin type map;
        // - the declared overlay (ExternalSourceResolver.applyNonStrictOverlay), which preserves an upstream pin's
        // snapshot when present and otherwise snapshots its own pre-overlay types;
        // - FIRST_FILE_WINS (ExternalSourceResolver.finishFirstFileWins), which pins EVERY file's fileSchema to the
        // anchor and so snapshots each file's own footer types here. Unlike the other two this is populated for
        // every file in the listing, including files that agree with the anchor -- those simply yield no retyped
        // columns, so pinnedColumnsOf returns empty for them.
        // It lets stats boundaries recover the file's real inferred types: the split-level boundary normalizes footer
        // range stats with them instead of the retyped types, and the resolve/commit boundaries identify the retyped
        // (pinned) column set to safe-miss its read-schema-blind cached stats.
        @Nullable Map<String, DataType> inferredTypes
    ) {
        public FileSchemaInfo(ExternalSchema fileSchema, @Nullable ColumnMapping mapping, @Nullable SourceStatistics statistics) {
            this(fileSchema, mapping, statistics, null);
        }
    }

    /**
     * Safe type widening for schema reconciliation: the common supertype when one exists without
     * loss, else {@code null}.
     * <p>
     * The rules live in {@link TypeWidening#widenLossless}. Text inferrers fold with
     * {@link TypeWidening#join}, which agrees with this method wherever the lossless form answers.
     * {@code LONG + DOUBLE} is the one {@code join} promotion this rejects.
     *
     * @return the widened type, or null if no lossless supertype exists
     */
    @Nullable
    public static DataType schemaWiden(DataType a, DataType b) {
        return TypeWidening.widenLossless(a, b);
    }

    /**
     * UNION_BY_NAME widening: the total form. {@link DataType#KEYWORD} is the answer for any pair
     * with no closer supertype. {@code LONG + DOUBLE} is {@code DOUBLE}, a {@code join} promotion
     * that is not lossless.
     * <p>
     * The keyword fallback is the lattice's own top rather than a local default here. A call site
     * that invents its own "and otherwise, keyword" is how the subsystem ended up with four
     * different answers to the same question. {@link #schemaWiden} stays as the separate
     * {@code @Nullable} entry point for callers that need to tell "no lossless supertype" apart from
     * "the answer is keyword".
     */
    private static DataType widenToCommonOrKeyword(DataType a, DataType b) {
        return TypeWidening.join(a, b);
    }

    /**
     * STRICT reconciliation: validate all files share the exact same schema.
     * Nullability differences are tolerated. NDJSON column order is also ignored because its
     * inferred order only records when each field first appeared in the sample; ordered formats
     * retain positional schema identity.
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
        boolean compareByName = fileMetadata.values().stream().allMatch(meta -> "ndjson".equals(meta.sourceType()));

        Map<StoragePath, FileSchemaInfo> perFileInfo = new LinkedHashMap<>();

        for (Map.Entry<StoragePath, SourceMetadata> entry : fileMetadata.entrySet()) {
            StoragePath filePath = entry.getKey();
            SourceMetadata meta = entry.getValue();
            List<Attribute> fileSchema = meta.schema();
            SourceStatistics stats = SourceStatisticsSerializer.fromSource(meta);

            validateNoDuplicateColumns(filePath, fileSchema);

            if (filePath.equals(referenceFile) == false) {
                validateStrictMatch(referenceFile, refSchema, filePath, fileSchema, compareByName);
            }

            ColumnMapping mapping;
            if (compareByName) {
                mapping = computeMapping(refSchema, fileSchema);
            } else {
                int[] identity = new int[refSchema.size()];
                for (int i = 0; i < identity.length; i++) {
                    identity[i] = i;
                }
                mapping = new ColumnMapping(identity, null);
            }
            perFileInfo.put(filePath, new FileSchemaInfo(new ExternalSchema(fileSchema), mapping, stats));
        }

        return new Result(new ExternalSchema(refSchema), Map.copyOf(perFileInfo));
    }

    private static void validateStrictMatch(
        StoragePath refPath,
        List<Attribute> refSchema,
        StoragePath filePath,
        List<Attribute> fileSchema,
        boolean compareByName
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
        if (compareByName) {
            validateStrictMatchByName(refPath, refSchema, filePath, fileSchema);
            return;
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
            validateStrictTypeMatch(refPath, refAttr, filePath, fileAttr);
        }
    }

    private static void validateStrictMatchByName(
        StoragePath refPath,
        List<Attribute> refSchema,
        StoragePath filePath,
        List<Attribute> fileSchema
    ) {
        Map<String, Attribute> fileAttributes = new HashMap<>();
        for (Attribute fileAttr : fileSchema) {
            fileAttributes.put(fileAttr.name(), fileAttr);
        }
        for (Attribute refAttr : refSchema) {
            Attribute fileAttr = fileAttributes.get(refAttr.name());
            if (fileAttr == null) {
                throw new IllegalArgumentException(
                    "Schema mismatch in ["
                        + filePath
                        + "]: column ["
                        + refAttr.name()
                        + "] from reference file ["
                        + refPath
                        + "] is missing."
                        + " Hint: use schema_resolution = \"union_by_name\" to automatically merge different schemas."
                );
            }
            validateStrictTypeMatch(refPath, refAttr, filePath, fileAttr);
        }
    }

    private static void validateStrictTypeMatch(StoragePath refPath, Attribute refAttr, StoragePath filePath, Attribute fileAttr) {
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

    /**
     * UNION_BY_NAME reconciliation: merge schemas from all files into a superset.
     * Missing columns are NULL-filled. Type differences use {@link TypeWidening#join}: lossless
     * promotions, {@code LONG + DOUBLE → DOUBLE} with a precision-loss warning, or
     * {@link DataType#KEYWORD} with a stringification warning when there is no closer supertype.
     * <p>
     * The merge is by exact name for every format, so a scalar {@code user} in one file and dotted
     * {@code user.id}/{@code user.tier} in another are simply different columns: all three names survive and
     * are NULL-filled in whichever file lacks them. A dot is an ordinary character in a column name, which
     * holds even for the formats that flatten nested data into dotted names, since their readers resolve
     * both spellings of one name to one column and take no position on a name that only prefixes others.
     *
     * @param fileMetadata ordered map of file path → metadata (insertion order = file sort order)
     * @return reconciliation result with unified schema and per-file mappings
     */
    public static Result reconcileUnionByName(Map<StoragePath, SourceMetadata> fileMetadata) {
        return reconcileUnionByName(fileMetadata, null);
    }

    /**
     * Same as {@link #reconcileUnionByName(Map)}, routing warning text through {@code warningSink}
     * instead of writing {@link org.elasticsearch.common.logging.HeaderWarning} on this thread.
     * Multi-file resolve runs on the metadata-read executor, so a direct header write would land
     * on that pool's {@code ThreadContext} and never reach the client. Callers that are not on the
     * request thread pass {@code ExternalSourceResolution}'s warning buffer so {@code EsqlSession}
     * can merge the messages into {@code DriverCompletionInfo} for emit on the response thread.
     *
     * @param warningSink destination for summary and per-column detail lines; {@code null} writes
     *                    {@link org.elasticsearch.common.logging.HeaderWarning} on the current thread
     */
    public static Result reconcileUnionByName(Map<StoragePath, SourceMetadata> fileMetadata, @Nullable Consumer<String> warningSink) {
        LinkedHashMap<String, MergeEntry> unified = new LinkedHashMap<>();
        // Per-column accumulator. We record *every* file's inferred type for every column up
        // front (it's cheap and gives the warning emitters a complete contributor list), then
        // decide at the end which warning, if any, the unified type warrants. Building this
        // lazily inside the merge branch would lose pre-merge files when a column finally
        // changes type on its third or later file.
        LinkedHashMap<String, ColumnContributions> contributions = new LinkedHashMap<>();

        for (Map.Entry<StoragePath, SourceMetadata> entry : fileMetadata.entrySet()) {
            StoragePath filePath = entry.getKey();
            List<Attribute> fileSchema = entry.getValue().schema();

            validateNoDuplicateColumns(filePath, fileSchema);

            for (Attribute attr : fileSchema) {
                String name = attr.name();
                contributions.computeIfAbsent(name, ColumnContributions::new).add(filePath, attr.dataType());
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

        emitKeywordFallbackWarnings(unified, contributions, warningSink);
        emitPrecisionLossWarnings(unified, contributions, warningSink);

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
            Map<String, DataType> inferredTypes = null;
            if (readsColumnsAtReconciledType(meta.sourceType())) {
                // Text readers parse each token at the pinned read type, so pin every widened column
                // to its reconciled type. The reader then reads at the wider type directly (raw text
                // for KEYWORD) instead of parsing at the narrower sampled type and failing on a value
                // the sample never saw, which would otherwise null-fill or abort the read before the
                // ColumnMapping cast could run. See readsColumnsAtReconciledType for why this is
                // scoped to sample-inferring text formats.
                List<Attribute> prePin = fileSchema;
                fileSchema = pinToReconciledTypes(fileSchema, unified);
                if (fileSchema != prePin) {
                    // At least one column was pinned above its inferred type. Carry the pre-pin (inferred) types
                    // so the resolve-side stats boundary can identify the pinned columns: their per-file stats were
                    // harvested at the narrower read type but the cache identity is read-schema-blind, so they must
                    // safe-miss rather than fold a stale count/extremum.
                    inferredTypes = typeMap(prePin);
                }
            }
            SourceStatistics stats = SourceStatisticsSerializer.fromSource(meta);

            ColumnMapping mapping = computeMapping(unifiedSchema, fileSchema);
            perFileInfo.put(filePath, new FileSchemaInfo(new ExternalSchema(fileSchema), mapping, stats, inferredTypes));
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

    /**
     * Whether a format's reader parses each value at the pinned read type, so pinning a widened
     * column to its reconciled type makes the reader read at the wider type directly. This holds
     * for the sample-inferring text formats (CSV, TSV, NDJSON): their readers convert each token to
     * the read schema's type (a KEYWORD read type yields the raw token), so an out-of-sample value
     * that does not fit the narrower sampled type survives at the wider reconciled type instead of
     * being destroyed at parse time. Columnar formats (Parquet, ORC) instead read the
     * physically-typed value and rely on {@link ColumnMapping}'s post-read cast to widen it, so
     * they must not be pinned here (there is no parse-time loss to avoid, and their readers do not
     * honor a read type that disagrees with the footer-declared type).
     */
    private static boolean readsColumnsAtReconciledType(String sourceType) {
        return READS_AT_RECONCILED_TYPE_FORMATS.contains(sourceType);
    }

    /**
     * The sample-inferring text formats whose readers parse each token at the pinned read type, so a
     * widened column can be pinned to its reconciled type (see {@link #readsColumnsAtReconciledType}).
     * The columnar-vs-text axis lives here as a single documented constant, mirroring
     * {@code ExternalSourceResolver.FILE_TYPED_FORMATS}, rather than on the {@code FormatReader} SPI.
     * A new sample-inferring text reader must be added here to receive the pin.
     */
    private static final Set<String> READS_AT_RECONCILED_TYPE_FORMATS = Set.of("csv", "tsv", "ndjson");

    /**
     * Returns {@code fileSchema} with each column that {@link #shouldPinAtReconciledType safely reads
     * at its reconciled type} replaced by an attribute of that reconciled type, preserving the column
     * name and nullability. Returns the input list unchanged when no column needs pinning.
     */
    private static List<Attribute> pinToReconciledTypes(List<Attribute> fileSchema, Map<String, MergeEntry> unified) {
        List<Attribute> pinned = null;
        for (int i = 0; i < fileSchema.size(); i++) {
            Attribute attr = fileSchema.get(i);
            MergeEntry unifiedEntry = unified.get(attr.name());
            if (unifiedEntry != null && shouldPinAtReconciledType(attr.dataType(), unifiedEntry.type)) {
                if (pinned == null) {
                    pinned = new ArrayList<>(fileSchema);
                }
                pinned.set(i, new ReferenceAttribute(Source.EMPTY, null, attr.name(), unifiedEntry.type, attr.nullable(), null, false));
            }
        }
        return pinned != null ? pinned : fileSchema;
    }

    /**
     * Physical-name-keyed type map of the given schema attributes. Used to snapshot a file's pre-pin
     * (inferred) column types before {@link #pinToReconciledTypes} retypes them, so downstream code can
     * recover which columns were pinned.
     */
    private static Map<String, DataType> typeMap(List<Attribute> schema) {
        Map<String, DataType> types = new HashMap<>(schema.size());
        for (Attribute attr : schema) {
            types.put(attr.name(), attr.dataType());
        }
        return types;
    }

    /**
     * Whether a text reader parsing a column directly at {@code reconciled} is equivalent to (or, for
     * KEYWORD, the intended replacement of) reading it at the sampled {@code inferred} type and then
     * applying {@link ColumnMapping}'s post-read cast.
     * <ul>
     *   <li>KEYWORD: the widened column is a string, so the reader returns the raw token. This is the
     *       point of the pin: an out-of-sample non-numeric value survives verbatim instead of being
     *       destroyed by a numeric parse before the cast can run.</li>
     *   <li>LONG / DOUBLE: the reader's typed parse at the wider numeric type is exactly the sampled
     *       type widened, so an out-of-sample value that overflows the narrower type (e.g. a value
     *       above {@code Integer.MAX_VALUE} in an INTEGER-sampled column reconciled to LONG) still
     *       parses instead of failing.</li>
     * </ul>
     * DATE_NANOS is deliberately excluded: a text reader parsing an epoch number at DATE_NANOS reads
     * it as epoch-nanos, not the epoch-millis a DATETIME column holds, so a DATETIME to DATE_NANOS
     * widening stays on the post-read cast that rescales the unit rather than a raw parse. That holds
     * whatever the reconciled type's origin — a declared schema, or, since text inference learned to
     * produce DATE_NANOS for sub-millisecond timestamps, an inferred one.
     */
    private static boolean shouldPinAtReconciledType(DataType inferred, DataType reconciled) {
        if (inferred == reconciled) {
            return false;
        }
        return reconciled == DataType.KEYWORD || reconciled == DataType.LONG || reconciled == DataType.DOUBLE;
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
        LinkedHashMap<String, ColumnContributions> contributions,
        @Nullable Consumer<String> warningSink
    ) {
        // Column unified to KEYWORD and at least one contributing file inferred a non-string type.
        // A column that was KEYWORD in every file (and stayed KEYWORD) is not a degradation: the
        // user-visible type matches the on-disk inferences and nothing was stringified.
        List<ColumnContributions> warned = new ArrayList<>();
        for (Map.Entry<String, MergeEntry> e : unified.entrySet()) {
            if (e.getValue().type != DataType.KEYWORD) {
                continue;
            }
            ColumnContributions fb = contributions.get(e.getKey());
            if (fb != null && fb.hasNonStringContributor()) {
                warned.add(fb);
            }
        }
        if (warned.isEmpty()) {
            return;
        }
        // SkipWarnings emits through warningSink when one is provided, otherwise HeaderWarning
        // on this thread. The local is not stored; the side effect is the emit.
        SkipWarnings warnings = new SkipWarnings(
            "Schema reconciliation widened columns to keyword due to cross-file type disagreement;"
                + " values are returned as strings. Hint: use schema_resolution = \"strict\" to fail instead.",
            warningSink
        );
        for (ColumnContributions fb : warned) {
            warnings.add(fb.buildDetail(DataType.KEYWORD));
        }
    }

    private static void emitPrecisionLossWarnings(
        LinkedHashMap<String, MergeEntry> unified,
        LinkedHashMap<String, ColumnContributions> contributions,
        @Nullable Consumer<String> warningSink
    ) {
        // Unified DOUBLE and both LONG and DOUBLE contributed. INTEGER + DOUBLE is a lossless
        // promotion and stays silent. LONG + DOUBLE + KEYWORD unifies to KEYWORD, so this gate
        // does not fire and the keyword warning is the only one.
        List<ColumnContributions> warned = new ArrayList<>();
        for (Map.Entry<String, MergeEntry> e : unified.entrySet()) {
            if (e.getValue().type != DataType.DOUBLE) {
                continue;
            }
            ColumnContributions fb = contributions.get(e.getKey());
            if (fb != null && fb.hasLongAndDoubleContributor()) {
                warned.add(fb);
            }
        }
        if (warned.isEmpty()) {
            return;
        }
        SkipWarnings warnings = new SkipWarnings(
            "Schema reconciliation widened long and double columns to double; integers above 2^53"
                + " are not exact. Hint: use schema_resolution = \"strict\" to fail instead.",
            warningSink
        );
        for (ColumnContributions fb : warned) {
            warnings.add(fb.buildDetail(DataType.DOUBLE));
        }
    }

    /**
     * Per-column accumulator: every file that contributed a value for the column, together with
     * that file's inferred type. Insertion-ordered so the emitted message reflects the user's
     * glob order. Recording is unconditional during merge; the emit step decides whether the
     * unified type warrants a warning.
     */
    private static final class ColumnContributions {
        private final String columnName;
        private final LinkedHashMap<StoragePath, DataType> contributions = new LinkedHashMap<>();

        ColumnContributions(String columnName) {
            this.columnName = columnName;
        }

        void add(StoragePath file, DataType inferredType) {
            // First inference wins per (column, file). A single file can't contribute two
            // different types for the same column (validateNoDuplicateColumns guarantees
            // unique names within a file), so putIfAbsent and put are equivalent here.
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

        boolean hasLongAndDoubleContributor() {
            boolean sawLong = false;
            boolean sawDouble = false;
            for (DataType type : contributions.values()) {
                if (type == DataType.LONG) {
                    sawLong = true;
                } else if (type == DataType.DOUBLE) {
                    sawDouble = true;
                }
            }
            return sawLong && sawDouble;
        }

        String buildDetail(DataType unifiedType) {
            // Pair each file with its inferred type so users can tell which file disagreed.
            // Long file lists are truncated with a "+N more" suffix; the distinct-type roll-up
            // at the end keeps an at-a-glance type picture even when files are truncated.
            StringBuilder sb = new StringBuilder("Column [").append(columnName)
                .append("] widened to ")
                .append(unifiedType.typeName())
                .append(": ");
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
