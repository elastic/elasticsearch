/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.xpack.esql.core.expression.Attribute;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;

/**
 * Unified interface for reading data formats.
 * <p>
 * Simple formats: implement only {@link #read(StorageObject, FormatReadContext)} (sync) -
 * async wrapping is automatic.
 * Async-capable formats: override {@link #readAsync(StorageObject, FormatReadContext, Executor, ActionListener)}
 * for native async behavior.
 * <p>
 * The output is ESQL's native Page format rather than Arrow to avoid
 * mandating Arrow as a dependency for all format implementations.
 * <p>
 * Implementations should provide metadata discovery via {@link #metadata(StorageObject)}
 * which returns a unified {@link SourceMetadata} containing schema and source information.
 * <p>
 * Per-query format configuration (delimiter, encoding, etc.) is set on the reader instance
 * via {@link #withConfig(Map)}. Per-query optimizer hints (pushed filters for row-group
 * or stripe skipping) are set via {@link #withPushedFilter(Object)}. Per-read execution
 * parameters (projection, batch size, limit, error policy, split config) are bundled in
 * {@link FormatReadContext}.
 */
public interface FormatReader extends Closeable {

    int NO_LIMIT = -1;

    /**
     * Strategy for resolving schemas across multiple files in a glob/multi-file query.
     */
    enum SchemaResolution {
        /** Use the schema from the first file; ignore differences in subsequent files. */
        FIRST_FILE_WINS,
        /** Require all files to share the exact same schema (modulo nullability). */
        STRICT,
        /** Merge schemas from all files by column name, with safe type widening. */
        UNION_BY_NAME;

        /**
         * Case-insensitive parse of a {@code schema_resolution} option value. This is the single
         * definition of valid strategy names, shared by the query path
         * ({@code ExternalSourceResolver.parseSchemaResolution}) and the dataset CRUD validator so
         * the two cannot diverge.
         *
         * @throws IllegalArgumentException if {@code value} is not a recognised strategy
         */
        public static SchemaResolution parse(String value) {
            return switch (value.toLowerCase(Locale.ROOT)) {
                case "first_file_wins" -> FIRST_FILE_WINS;
                case "strict" -> STRICT;
                case "union_by_name" -> UNION_BY_NAME;
                default -> throw new IllegalArgumentException(
                    "Unknown schema_resolution value [" + value + "]. Valid values are: first_file_wins, strict, union_by_name"
                );
            };
        }
    }

    /**
     * Cluster-wide default schema resolution strategy when a query does not specify one.
     * <p>
     * This is the single source of truth: it is consulted both by this SPI's
     * {@link #defaultSchemaResolution()} and by {@code ExternalSourceResolver.parseSchemaResolution}
     * when no {@code schema_resolution} key is present in the per-query config. The format
     * detected at glob-expansion time is not yet known when the resolver decides whether to
     * take the read-all-and-reconcile path versus the FFW fast path, so there is no format
     * dispatch here today; if per-format defaults become desirable in the future the resolver
     * will need to peek at the lex-smallest file's format first, and this constant becomes the
     * fallback only.
     */
    SchemaResolution DEFAULT_SCHEMA_RESOLUTION = SchemaResolution.UNION_BY_NAME;

    /**
     * Returns the cluster-wide default schema resolution for this reader. Format implementations
     * may override this to advertise a different preferred default, but the resolver does not
     * consult it today (see {@link #DEFAULT_SCHEMA_RESOLUTION} for the rationale). Override is
     * effectively informational until that wiring exists.
     */
    default SchemaResolution defaultSchemaResolution() {
        return DEFAULT_SCHEMA_RESOLUTION;
    }

    /**
     * Returns the default error policy for this format. The base default is {@link ErrorPolicy#STRICT}
     * (fail_fast) and every format inherits it, so a bad per-value coercion fails the read across all
     * formats unless the user opts into {@code error_mode: null_field}. Pinned per reader by a
     * {@code testDefaultErrorPolicyIsStrict} guard; do not override to a lenient default (that would
     * silently diverge one format from the others).
     */
    default ErrorPolicy defaultErrorPolicy() {
        return ErrorPolicy.STRICT;
    }

    // === METADATA ===

    SourceMetadata metadata(StorageObject object) throws IOException;

    /**
     * Asynchronously resolves metadata for the given storage object.
     * <p>
     * The default wraps the synchronous {@link #metadata(StorageObject)} in the provided executor,
     * mirroring {@link #readAsync}. Formats whose footer/metadata read can be issued without holding
     * an executor thread across the network round-trip (e.g. Parquet via
     * {@link StorageObject#readBytesAsync}) should override this so that a wide discovery fan-out is
     * bounded by an in-flight permit rather than by the number of executor threads it pins.
     */
    default void metadataAsync(StorageObject object, Executor executor, ActionListener<SourceMetadata> listener) {
        executor.execute(() -> {
            try {
                listener.onResponse(metadata(object));
            } catch (Exception e) {
                listener.onFailure(e);
            }
        });
    }

    default List<Attribute> schema(StorageObject object) throws IOException {
        return metadata(object).schema();
    }

    // === READ API ===

    /**
     * Reads data from the given storage object using the provided context.
     * <p>
     * This is the primary read method. All implementations must override this method.
     */
    CloseableIterator<Page> read(StorageObject object, FormatReadContext context) throws IOException;

    /**
     * Convenience overload that delegates to {@link #read(StorageObject, FormatReadContext)}.
     * Keeps test code and simple call sites working without constructing a context.
     */
    default CloseableIterator<Page> read(StorageObject object, List<String> projectedColumns, int batchSize) throws IOException {
        return read(object, FormatReadContext.of(projectedColumns, batchSize));
    }

    /**
     * Asynchronously reads data from the given storage object using the provided context.
     * <p>
     * The default wraps the synchronous {@link #read(StorageObject, FormatReadContext)} in the
     * provided executor. Formats with native async support should override this.
     */
    default void readAsync(
        StorageObject object,
        FormatReadContext context,
        Executor executor,
        ActionListener<CloseableIterator<Page>> listener
    ) {
        executor.execute(() -> {
            try {
                listener.onResponse(read(object, context));
            } catch (Exception e) {
                listener.onFailure(e);
            }
        });
    }

    // === CONFIGURATION ===

    String formatName();

    List<String> fileExtensions();

    /**
     * Returns a reader configured from the input config map.
     * Default delegates to {@link #withConfigTrackingConsumedKeys(Map)} and discards the consumed-keys set;
     * use this overload when the caller does not need to validate against the consumed keys.
     * <p>
     * <b>Override target:</b> implementations must override {@link #withConfigTrackingConsumedKeys(Map)},
     * NOT this method. The default {@code withConfig} delegates through the tracking variant, so an
     * override here alone would be silently bypassed by every caller. The tracking variant is the
     * single configuration entry point for the SPI.
     */
    default FormatReader withConfig(Map<String, Object> config) {
        return withConfigTrackingConsumedKeys(config).value();
    }

    /**
     * Returns a reader configured from the input config map, paired with the keys consumed from it.
     * <p>
     * <b>Required override.</b> Every reader must explicitly declare which keys it claims, even if
     * the answer is "none" (return {@code Configured.empty(this)}). The previous {@code default}
     * silently dropped any unknown keys; that footgun is the reason this is no longer optional.
     * Implementations that read configuration from the map should override this method (not
     * {@link #withConfig(Map)}); the consumed-keys set is required by {@link ConfigKeyValidator}
     * for unknown-key rejection at planning time.
     */
    Configured<FormatReader> withConfigTrackingConsumedKeys(Map<String, Object> config);

    /**
     * Returns a format reader configured with the given pushed filter from the optimizer.
     * <p>
     * The pushed filter is an opaque object produced by {@code FilterPushdownSupport} during
     * local physical optimization. Only format readers that support predicate pushdown
     * (e.g., Parquet row-group skipping, ORC stripe-level predicates) need to override this.
     * <p>
     * The filter is per-query: it applies identically to every file/split in the query.
     * Implementations should cast the filter to their expected type and return a new reader
     * instance with the filter stored as an instance field.
     *
     * @param pushedFilter opaque filter object, or null if no filter was pushed
     * @return a new reader with the filter applied, or {@code this} if the filter is not applicable
     */
    default FormatReader withPushedFilter(Object pushedFilter) {
        return this;
    }

    /**
     * Returns the aggregate pushdown support for this format.
     * Only format readers with column statistics in their metadata (Parquet, ORC) override this.
     */
    default AggregatePushdownSupport aggregatePushdownSupport() {
        return AggregatePushdownSupport.UNSUPPORTED;
    }

    /**
     * Returns a format reader configured with the schema attributes, letting the reader skip
     * re-reading/inferring the schema from the file header on every read — especially important for
     * split-based reads where the split may start mid-file (no header available).
     * <p>
     * <b>This carries no authority.</b> Callers use it for two different things: the planning-phase
     * schema, constant for the query ({@code FileSourceFactory}), and a schema just inferred from one
     * file or even one chunk ({@code AsyncExternalSourceOperatorFactory},
     * {@code ParallelParsingCoordinator}, {@code StreamingParallelParsingCoordinator}). A reader cannot
     * tell the two apart from this call. So where a reader also receives
     * {@link FormatReadContext#readSchema()} — the planner's per-file read contract — <b>that is the
     * authority and must win on type</b>; whatever arrives here may be a guess. Letting the guess win
     * is what produced the compressed-read type-cast crashes.
     * <p>
     * Formats with embedded schemas (Parquet, ORC) may ignore this since they always read
     * the schema from the file metadata.
     *
     * @param schema the schema attributes, or null to clear. NOT necessarily planning-phase — see above.
     * @return a new reader with the schema set, or {@code this} if the schema is not needed
     */
    default FormatReader withSchema(List<Attribute> schema) {
        return this;
    }

    /**
     * Returns a format reader that parses the given columns' dates with the given patterns instead of the ISO default
     * / file-level {@code datetime_format}. Keyed by <b>physical</b> (file) column name — the caller
     * ({@code FileSourceFactory}) has already applied any declared {@code path} rename. The patterns are ES
     * {@code DateFormatter} patterns (named formats and {@code ||} chains included), matching the dataset-put validation.
     * <p>
     * Only the text formats (CSV/TSV, NDJSON) parse dates from text and override this. Columnar formats (Parquet, ORC)
     * carry native typed values and keep the no-op default — a declared {@code format} on a columnar column is rejected
     * upstream at query resolution, so it never reaches here.
     *
     * @param physicalNameToPattern per-column date patterns keyed by physical column name; empty for no declared formats
     * @return a new reader applying the per-column formats, or {@code this} when none apply
     */
    default FormatReader withDeclaredDateFormats(Map<String, String> physicalNameToPattern) {
        return this;
    }

    /**
     * Returns a format reader that treats the given columns as <b>declared-type</b> columns: their target type came from
     * an explicit declaration rather than inference, which licenses a lossy read-time coercion toward it (e.g. a declared
     * {@code integer} over an {@code int64} file column narrows per value, null on overflow). An inferred target must
     * never narrow — a cross-file clash widens-or-nulls. Keyed by <b>physical</b> (file) column name; the caller
     * ({@code FileSourceFactory}) has already applied any declared {@code path} rename.
     * <p>
     * Only the by-name columnar formats (Parquet, ORC) make a whole-column incompatibility null-fill decision and
     * override this — a declared column keeps the coercion escape, an inferred column null-fills whenever the file type
     * is not widening-compatible. The text formats (CSV/TSV, NDJSON) parse straight into the target and keep the no-op
     * default (their per-field failures are governed by the {@code ErrorPolicy}, not a whole-column type check).
     *
     * @param physicalDeclaredColumns physical names of the declared-type columns; empty when no column type was declared
     * @return a new reader honoring the declared-type set, or {@code this} when none apply
     */
    default FormatReader withDeclaredTypeColumns(Set<String> physicalDeclaredColumns) {
        return this;
    }

    /**
     * Whether the pinned schema this reader was handed is a DECLARED claim (bind its columns to the file BY NAME) as
     * opposed to an INFERRED description (bind by position). Keyed on the schema's provenance, not on whether any
     * column declared a {@code path}: a declaration whose order merely differs from the file, with no {@code path} at
     * all, must still bind by name.
     * <p>
     * {@code dynamic} controls only whether a schema is inferred; it must not leak into how columns bind. Under
     * {@code dynamic:true} the schema is inferred from the file, so its positions already are the file's — bind by
     * position. Under {@code dynamic:false} the declaration itself is pinned as the schema; a reader that consumed it
     * positionally would never look at the physical names it was handed, so the same mapping could read a different
     * column. This bit makes such a reader bind by name, so the two modes agree (esql-planning#1307).
     * <p>
     * Only the text readers need it: they alone bind a pinned schema positionally. Parquet/ORC bind by footer name and
     * NDJSON by object key, so they bind a declared schema by name under either mode already and keep the no-op default.
     * A declared name the file does not supply reads null with a warning, never a silent positional fallback.
     *
     * @param declaredPathBinding true when the pinned schema is a DECLARED claim (provenance DECLARED)
     * @return a new reader honoring the binding mode, or {@code this} when it does not apply
     */
    default FormatReader withDeclaredPathBinding(boolean declaredPathBinding) {
        return this;
    }

    /**
     * Whether this reader can only bind its declared columns when it sees the start of the file, which makes the file
     * unsplittable: every split past the first would have no way to resolve the binding.
     *
     * <p>True only for a headered text reader binding a DECLARED schema by name: the binding is resolved against the
     * file's header line, and only the first split carries it. A headerless file's physical names encode their own
     * positions ({@code col4} -> field 4), so it binds on any split and stays fully splittable — which is the shape the
     * throughput-sensitive reads actually use.
     */
    default boolean declaredNameBindingNeedsFileStart() {
        return false;
    }

    /**
     * Returns the filter pushdown support for this format, or null if not supported.
     * <p>
     * When non-null, the optimizer can translate ESQL filter expressions into format-specific
     * predicates (e.g., Parquet FilterPredicate) that enable row-group skipping via statistics,
     * dictionary, and bloom filter checks.
     *
     * @return FilterPushdownSupport for this format, or null if not supported
     */
    default FilterPushdownSupport filterPushdownSupport() {
        return null;
    }

    default boolean supportsNativeAsync() {
        return false;
    }

    /**
     * Whether this format supports being wrapped in a whole-file, stream-only decompressor
     * (e.g. {@code .parquet.zst} or {@code .orc.gz}). Sequential formats (CSV, NDJSON) return
     * the default {@code true}. Tail/footer-based formats (Parquet, ORC) must override to
     * {@code false} because they require random access and a known decompressed length. This
     * flag does NOT affect a format's own internal compression (e.g. Parquet column-chunk zstd).
     */
    default boolean supportsWholeFileCompression() {
        return true;
    }

    /**
     * Returns a typed snapshot of format-reader I/O counters, or {@code null} when the reader
     * tracks none. The snapshot is folded into the {@code format_reader} field of the
     * external-source operator status.
     */
    default FormatReaderStatus statusSnapshot() {
        return null;
    }

    /**
     * Returns this reader's {@link RowPositionStrategy} — the dispatcher applies it polymorphically
     * to wrap (or pass through) the reader's emitted page iterator so each page has the
     * {@code _rowPosition} slot populated. Every reader must explicitly declare a strategy:
     * a {@link PassThroughRowPositionStrategy} when the reader natively fills the slot in its own
     * iterator (parquet-mr, ORC, CSV, NDJSON), a {@link NullSpliceRowPositionStrategy} when the
     * reader has no row-position channel and the slot must surface NULL (parquet-rs), or a future
     * strategy that injects the column from per-page reader state. There is no default — readers
     * that "don't care" still participate, by returning {@link PassThroughRowPositionStrategy}.
     */
    RowPositionStrategy rowPositionStrategy();

}
