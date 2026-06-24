/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.index.analysis;

import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.index.IndexSettings;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

import static org.elasticsearch.index.analysis.AnalysisRegistry.DEFAULT_ANALYZER_NAME;
import static org.elasticsearch.index.analysis.AnalysisRegistry.DEFAULT_SEARCH_ANALYZER_NAME;

/**
 * IndexAnalyzers contains a name to analyzer mapping for a specific index.
 * This class only holds analyzers that are explicitly configured for an index and doesn't allow
 * access to individual tokenizers, char or token filter.
 *
 * @see AnalysisRegistry
 */
public interface IndexAnalyzers extends Closeable {

    enum AnalyzerType {
        ANALYZER,
        NORMALIZER,
        WHITESPACE
    }

    /**
     * Returns an analyzer of the given type mapped to the given name, or {@code null} if
     * no such analyzer exists.
     */
    NamedAnalyzer getAnalyzer(AnalyzerType type, String name);

    /**
     * Returns an analyzer mapped to the given name or {@code null} if not present
     */
    default NamedAnalyzer get(String name) {
        return getAnalyzer(AnalyzerType.ANALYZER, name);
    }

    /**
     * Returns a normalizer mapped to the given name or {@code null} if not present
     */
    default NamedAnalyzer getNormalizer(String name) {
        return getAnalyzer(AnalyzerType.NORMALIZER, name);
    }

    /**
     * Returns a normalizer that splits on whitespace mapped to the given name or {@code null} if not present
     */
    default NamedAnalyzer getWhitespaceNormalizer(String name) {
        return getAnalyzer(AnalyzerType.WHITESPACE, name);
    }

    /**
     * Returns the default index analyzer for this index
     */
    default NamedAnalyzer getDefaultIndexAnalyzer() {
        return getAnalyzer(AnalyzerType.ANALYZER, DEFAULT_ANALYZER_NAME);
    }

    /**
     * Returns the default search analyzer for this index. If not set, this will return the default analyzer
     */
    default NamedAnalyzer getDefaultSearchAnalyzer() {
        NamedAnalyzer analyzer = getAnalyzer(AnalyzerType.ANALYZER, DEFAULT_SEARCH_ANALYZER_NAME);
        if (analyzer != null) {
            return analyzer;
        }
        return getDefaultIndexAnalyzer();
    }

    /**
     * Returns the default search quote analyzer for this index. If not set, this will return the default
     * search analyzer
     */
    default NamedAnalyzer getDefaultSearchQuoteAnalyzer() {
        NamedAnalyzer analyzer = getAnalyzer(AnalyzerType.ANALYZER, AnalysisRegistry.DEFAULT_SEARCH_QUOTED_ANALYZER_NAME);
        if (analyzer != null) {
            return analyzer;
        }
        return getDefaultSearchAnalyzer();
    }

    /**
     * Reload any analyzers that have reloadable components. {@code reloadToken} identifies the reload
     * request so that an analyzer shared by several indices is rebuilt once per request rather than
     * once per index; pass {@code null} to disable that deduplication (always reload).
     */
    default List<String> reload(
        AnalysisRegistry analysisRegistry,
        IndexSettings indexSettings,
        String resource,
        boolean preview,
        Object reloadToken
    ) throws IOException {
        return List.of();
    }

    default void close() throws IOException {}

    static IndexAnalyzers of(Map<String, NamedAnalyzer> analyzers) {
        return of(analyzers, Map.of(), Map.of());
    }

    static IndexAnalyzers of(Map<String, NamedAnalyzer> analyzers, Map<String, NamedAnalyzer> tokenizers) {
        return of(analyzers, tokenizers, Map.of());
    }

    static IndexAnalyzers of(
        Map<String, NamedAnalyzer> analyzers,
        Map<String, NamedAnalyzer> normalizers,
        Map<String, NamedAnalyzer> whitespaceNormalizers
    ) {
        // No shared-cache references — the simple overload used by tests and code paths that
        // construct analyzers directly without going through the registry's cache.
        return of(analyzers, normalizers, whitespaceNormalizers, new HashMap<>(), List.of());
    }

    /**
     * Construct an {@link IndexAnalyzers} that owns refcount handles on shared analyzer cache
     * entries. {@code analyzerReleasables} is keyed by analyzer name (reload mutates a shared
     * analyzer in place rather than swapping cache entries, so the handle for a name is stable);
     * {@code staticReleasables} covers normalizer/whitespace handles.
     *
     * <p>On {@link #close} every releasable is invoked exactly once; the last release on any
     * given cache entry drives the actual underlying {@code analyzer.close()} and evicts it from
     * the registry's cache.
     */
    static IndexAnalyzers of(
        Map<String, NamedAnalyzer> analyzers,
        Map<String, NamedAnalyzer> normalizers,
        Map<String, NamedAnalyzer> whitespaceNormalizers,
        Map<String, Releasable> analyzerReleasables,
        List<Releasable> staticReleasables
    ) {
        return new IndexAnalyzers() {
            // Idempotent close guard: each release handle must fire exactly once across the
            // lifetime of this IndexAnalyzers. Double-close on the same instance otherwise
            // re-releases every shared reference, dropping refcounts a second time and falsely
            // evicting cache entries that other indices still reference.
            private final AtomicBoolean closed = new AtomicBoolean();

            @Override
            public NamedAnalyzer getAnalyzer(AnalyzerType type, String name) {
                return switch (type) {
                    case ANALYZER -> analyzers.get(name);
                    case NORMALIZER -> normalizers.get(name);
                    case WHITESPACE -> whitespaceNormalizers.get(name);
                };
            }

            @Override
            public void close() throws IOException {
                if (closed.compareAndSet(false, true) == false) {
                    return;
                }
                // Two groups: (1) INDEX-scoped analyzers built outside the shared cache; cache
                // entries are re-tagged GLOBAL so they're skipped here, (2) refcount release
                // handles for shared cache entries — the last release on each entry drives the
                // underlying close + eviction. IOUtils.close gathers exceptions so a failure on
                // one closeable doesn't skip the rest, and the first exception (if any) is
                // rethrown.
                List<Closeable> closeables = new ArrayList<>();
                Stream.of(analyzers.values().stream(), normalizers.values().stream(), whitespaceNormalizers.values().stream())
                    .flatMap(s -> s)
                    .filter(a -> a.scope() == AnalyzerScope.INDEX)
                    .forEach(closeables::add);
                closeables.addAll(analyzerReleasables.values());
                closeables.addAll(staticReleasables);
                IOUtils.close(closeables);
            }

            @Override
            public List<String> reload(
                AnalysisRegistry registry,
                IndexSettings indexSettings,
                String resource,
                boolean preview,
                Object reloadToken
            ) throws IOException {

                // Keep the local analyzer name (the map key): a shared ReloadableCustomAnalyzer keeps
                // the name of whichever index built it first, so reloadAnalyzerInPlace must look the
                // recipe up under THIS index's name, not the shared instance's embedded name.
                List<Map.Entry<String, NamedAnalyzer>> reloadableAnalyzers = analyzers.entrySet()
                    .stream()
                    .filter(e -> e.getValue().analyzer() instanceof ReloadableCustomAnalyzer ra && ra.usesResource(resource))
                    .toList();

                if (reloadableAnalyzers.isEmpty()) {
                    return List.of();
                }

                if (preview == false) {
                    // In-place reload: mutate the shared {@link ReloadableCustomAnalyzer}'s
                    // components so existing mapping references (TextSearchInfo captures a direct
                    // NamedAnalyzer reference at mapping-build time) observe the refreshed state.
                    //
                    // Design rationale: reloadable filters force {@link AnalysisMode#SEARCH_TIME}
                    // (see {@link SynonymTokenFilterFactory#analysisMode}), and the mapping layer
                    // enforces that SEARCH_TIME-only analyzers can only be used as
                    // {@code search_analyzer} / {@code search_quote_analyzer} — NOT as the
                    // index-time {@code analyzer} (validated in {@code TextParams}). So reload
                    // can only ever change how user query input is tokenized; it can never
                    // change how data was indexed, and so cannot corrupt the index.
                    //
                    // Because the underlying resource (a synonym set in {@code .synonyms}, a file
                    // on disk) is inherently cluster-global, refreshing it for one sharer and not
                    // another would produce confusing query-time inconsistencies for no benefit.
                    // In-place mutation makes every sharer converge to the same up-to-date state,
                    // which matches user expectations for resource-driven reloads. The reloadToken
                    // makes a shared instance rebuild once per request, not once per sharing index.
                    for (Map.Entry<String, NamedAnalyzer> entry : reloadableAnalyzers) {
                        registry.reloadAnalyzerInPlace(indexSettings, entry.getKey(), entry.getValue(), reloadToken);
                    }
                }

                // Report every matching analyzer as reloaded, including those whose rebuild was deduped
                // because a sibling shard sharing the same instance rebuilt it first for this request.
                // That is accurate, not over-reporting: reload mutates the shared instance in place, so a
                // "coasting" sharer's analyzer reflects the refreshed state just the same. We deliberately
                // do not distinguish "this shard did the I/O" — a coasted sharer is still reloaded, and a
                // rebuilt=false flag would wrongly read as "your reload did not take effect".
                //
                // Conversely, the returned list (surfaced as the response's reload_details) is NOT an
                // exhaustive list of every analyzer affected on the node. Because the rebuilt instance is
                // shared, indices that were not part of this reload request but reference the same
                // instance also observe the refreshed state — they simply are not enumerated here, since
                // reload_details reports the analyzers of the requested index, not the full set of
                // sharers.
                return reloadableAnalyzers.stream().map(Map.Entry::getKey).toList();
            }
        };
    }

}
