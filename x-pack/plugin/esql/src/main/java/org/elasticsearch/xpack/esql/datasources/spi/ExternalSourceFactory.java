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
     */
    default void resolveMetadataAsync(
        String location,
        @Nullable ListingHint hint,
        Map<String, Object> config,
        Executor executor,
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
