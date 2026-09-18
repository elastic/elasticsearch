/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.core.Nullable;

import java.util.Map;
import java.util.concurrent.Executor;
import java.util.function.Consumer;

/**
 * Common interface for complete external data source factories.
 * Both API-based connectors (Flight, JDBC) and table-based catalogs (Iceberg)
 * implement this interface, enabling unified resolution and dispatch.
 *
 * Building-block factories (StorageProviderFactory, FormatReaderFactory) are NOT
 * part of this hierarchy — they are composed by the framework for file-based sources.
 */
public interface ExternalSourceFactory {

    String type();

    boolean canHandle(String location);

    /**
     * Config-aware variant of {@link #canHandle(String)} used by the resolver when selecting a factory.
     * The default delegates to the path-only form; a factory overrides this when query configuration can
     * supply information the path alone lacks. The file factory uses it to claim an extensionless resource
     * when an explicit {@code format} is configured (the read-path counterpart to the CRUD validator
     * accepting an explicit format on an extensionless resource).
     */
    default boolean canHandle(String location, Map<String, Object> config) {
        return canHandle(location);
    }

    SourceMetadata resolveMetadata(String location, Map<String, Object> config);

    /**
     * Asynchronously resolves metadata for the given location.
     * <p>
     * The default wraps the synchronous {@link #resolveMetadata(String, Map)} in the provided
     * executor. File-based factories that can issue the footer/metadata read without pinning an
     * executor thread across the network round-trip should override this to route through the
     * format reader's {@link FormatReader#metadataAsync} path, so a multi-file discovery fan-out
     * is bounded by an in-flight permit rather than by the executor's thread count.
     * <p>
     * When {@code hint} is non-null the caller already knows the object's length/mtime from a
     * directory listing; overrides must build the storage object from it and skip any existence/HEAD
     * probe, since that probe is a synchronous round-trip (e.g. an S3 HEAD) that would pin the
     * executor thread before the async read and defeat the in-flight bound. A {@code null} hint means
     * nothing is known (a single, explicitly-referenced path) and the override must verify existence
     * itself.
     * <p>
     * {@code warningSink} receives non-fatal warnings (see
     * {@link #validateConfig(String, Map, Consumer)}): this method runs on a metadata-read executor
     * whose thread context is not the originating request's, so an implementation must route warnings
     * through the sink rather than calling {@code HeaderWarning.addWarning} directly.
     */
    default void resolveMetadataAsync(
        String location,
        @Nullable ListingHint hint,
        Map<String, Object> config,
        Executor executor,
        Consumer<String> warningSink,
        ActionListener<SourceMetadata> listener
    ) {
        executor.execute(() -> {
            try {
                listener.onResponse(resolveMetadata(location, config));
            } catch (Exception e) {
                listener.onFailure(e);
            }
        });
    }

    /**
     * Reject configuration keys this factory doesn't recognize at the given location. Implementations
     * compose their claimed-key sets and call {@link ConfigKeyValidator#check}.
     * <p>
     * <b>Required override.</b> Every factory must explicitly state its validation contract — either
     * by composing claimed-key sets and delegating to {@link ConfigKeyValidator#check}, or, for
     * factories with no per-query config keys today, by calling
     * {@code ConfigKeyValidator.check(config, List.of())} to reject any non-empty config map. An
     * empty method body would silently accept typo'd configurations — exactly the footgun this
     * abstract contract exists to prevent — so do not write one.
     */
    void validateConfig(String location, Map<String, Object> config);

    /**
     * Variant of {@link #validateConfig(String, Map)} that routes non-fatal warnings through
     * {@code warningSink} instead of {@code HeaderWarning}. The resolver always calls this form:
     * validation runs on its metadata-read executor, whose thread context is not the originating
     * request's, so a direct {@code HeaderWarning.addWarning} from inside it would never reach the
     * client. The sink buffers messages until the resolver flushes them under the restored request
     * context (see {@code ExternalSourceResolver#pendingShadowWarnings}).
     * <p>
     * The default drops the sink and delegates, which is correct for factories that only ever fail or
     * pass. A factory that emits warnings must override this form with the real implementation and
     * implement the two-argument form by delegating here with a direct {@code HeaderWarning} sink
     * (for callers on a request thread).
     */
    default void validateConfig(String location, Map<String, Object> config, Consumer<String> warningSink) {
        validateConfig(location, config);
    }

    default FilterPushdownSupport filterPushdownSupport() {
        return null;
    }

    default SourceOperatorFactoryProvider operatorFactory() {
        return null;
    }

    default SplitProvider splitProvider() {
        return SplitProvider.SINGLE;
    }
}
