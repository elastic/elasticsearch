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
import java.util.Map;
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
        // TODO: implement strict schema validation across files
        STRICT,
        // TODO: implement union-by-name schema merging across files
        UNION_BY_NAME
    }

    default SchemaResolution defaultSchemaResolution() {
        return SchemaResolution.FIRST_FILE_WINS;
    }

    /**
     * Returns the default error policy for this format.
     * Override to change the default behavior for a specific format (e.g. NDJSON
     * defaults to lenient because skipping malformed lines is its natural behavior).
     */
    default ErrorPolicy defaultErrorPolicy() {
        return ErrorPolicy.STRICT;
    }

    // === METADATA ===

    SourceMetadata metadata(StorageObject object) throws IOException;

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
     * Returns a format reader configured with the schema attributes.
     * <p>
     * The schema is determined during the planning phase (via {@link #metadata(StorageObject)})
     * and is constant for all files/splits in a query. Passing it here allows the reader to skip
     * re-reading/inferring the schema from the file header on every read, which is especially
     * important for split-based reads where the split may start mid-file (no header available).
     * <p>
     * Formats with embedded schemas (Parquet, ORC) may ignore this since they always read
     * the schema from the file metadata.
     *
     * @param schema the planning-phase schema attributes, or null to clear
     * @return a new reader with the schema set, or {@code this} if the schema is not needed
     */
    default FormatReader withSchema(List<Attribute> schema) {
        return this;
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

}
