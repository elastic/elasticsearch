/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.index.analysis;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.KeywordTokenizer;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.FeatureFlag;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexService.IndexCreationContext;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.TextFieldMapper;
import org.elasticsearch.indices.analysis.AnalysisModule;
import org.elasticsearch.indices.analysis.AnalysisModule.AnalysisProvider;
import org.elasticsearch.indices.analysis.PreBuiltAnalyzers;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static java.util.Collections.unmodifiableMap;

/**
 * An internal registry for tokenizer, token filter, char filter and analyzer.
 * This class exists per node and allows to create per-index {@link IndexAnalyzers} via {@link #build}
 */
public final class AnalysisRegistry implements Closeable {
    private static final Logger logger = LogManager.getLogger(AnalysisRegistry.class);

    public static final String INDEX_ANALYSIS_CHAR_FILTER = "index.analysis.char_filter";
    public static final String INDEX_ANALYSIS_FILTER = "index.analysis.filter";
    public static final String INDEX_ANALYSIS_ANALYZER = "index.analysis.analyzer";
    public static final String INDEX_ANALYSIS_TOKENIZER = "index.analysis.tokenizer";

    public static final String DEFAULT_ANALYZER_NAME = "default";
    public static final String DEFAULT_SEARCH_ANALYZER_NAME = "default_search";
    public static final String DEFAULT_SEARCH_QUOTED_ANALYZER_NAME = "default_search_quoted";

    /**
     * Gates cross-index analyzer sharing while the feature is stabilized. Enabled automatically in
     * snapshot builds (CI, QA, dev) and off in release builds unless
     * {@code -Des.shared_analyzers_feature_flag_enabled=true} is set. When disabled, the recipe
     * cache is never consulted and every index builds its own fresh analyzer — exactly the
     * pre-sharing behavior.
     */
    public static final FeatureFlag SHARED_ANALYZERS_FEATURE_FLAG = new FeatureFlag("shared_analyzers");
    private static final boolean SHARED_ANALYZERS_ENABLED = SHARED_ANALYZERS_FEATURE_FLAG.isEnabled();

    private final PrebuiltAnalysis prebuiltAnalysis;
    private final Map<String, Analyzer> cachedAnalyzer = new ConcurrentHashMap<>();

    /**
     * Node-level cache of analyzers and normalizers keyed on their structural recipe so that
     * indices with identical analyzer definitions share the same {@link NamedAnalyzer} instance —
     * including its Lucene {@code CloseableThreadLocal<TokenStreamComponents>} graph, which is
     * where the per-analyzer per-thread memory cost actually lives.
     *
     * <p>Each value is a {@link CacheEntry} carrying a reference count. Every {@link IndexAnalyzers}
     * built off a cache entry holds one reference; on {@link IndexAnalyzers#close} the reference is
     * released. When the count drops to zero the analyzer is closed and the entry is evicted from
     * the cache. This preserves the close-on-no-references guarantee that holds for non-shared
     * analyzers today, while still letting indices share live instances.
     */
    private final Map<AnalyzerKey, CacheEntry> analyzerCache = new ConcurrentHashMap<>();
    private final Map<AnalyzerKey, CacheEntry> normalizerCache = new ConcurrentHashMap<>();
    private final Map<AnalyzerKey, CacheEntry> whitespaceNormalizerCache = new ConcurrentHashMap<>();

    /**
     * Refcounted slot for a shared analyzer with single-flight build coalescing. The cache slot
     * is reserved by the first thread to miss; concurrent threads with the same recipe see the
     * reserved slot, bump the refcount, and join the future on the result —
     * exactly one thread actually builds the analyzer (a synonym chain may read from disk or the
     * .synonyms system index, which can take many seconds and must not multiply by N concurrent
     * indices opening together).
     *
     * <p>The entry is removed when the last user releases. Build failures complete the future
     * exceptionally and remove the entry so the next attempt can build fresh.
     */
    static final class CacheEntry {
        // Future is completed by the builder thread. tryAcquire may return true before the future
        // is complete (the slot is reserved); callers MUST join() the future to materialize the
        // analyzer. Identity comparison on this object is the right "is it me?" check during
        // displacement, regardless of whether the build has finished.
        final CompletableFuture<NamedAnalyzer> futureAnalyzer = new CompletableFuture<>();
        // Invariant: refCount >= 1 while the entry is reachable to acquire; once it reaches 0
        // the entry is closed and {@link #tryAcquire} refuses further acquires on it.
        final AtomicInteger refCount = new AtomicInteger(1);
        // The analyzer to close on eviction. This is the ORIGINAL wrapper as built, NOT the
        // GLOBAL-retagged wrapper handed to IndexAnalyzers via {@link #futureAnalyzer}. The
        // distinction matters: {@link NamedAnalyzer#close} only cascades to the underlying Lucene
        // analyzer (which owns the heavy CloseableThreadLocal / SynonymMap) when the wrapper's
        // scope is INDEX. We re-tag the handed-out wrapper to GLOBAL so per-index close paths skip
        // it, but that same re-tag would suppress the underlying close on eviction — so eviction
        // closes this original-scoped wrapper instead. For prebuilt / GLOBAL analyzers the two are
        // the same instance and close() correctly leaves the shared underlying alone.
        // Written once by the builder thread before completing the future, so it is safely visible
        // to every thread that joins the future (and to shutdown, which runs after all builds).
        volatile NamedAnalyzer evictable;

        /**
         * Acquire one additional reference if the entry is still live. Returns false if it has
         * already been retired, in which case the caller must build a fresh entry instead.
         */
        boolean tryAcquire() {
            while (true) {
                int n = refCount.get();
                if (n == 0) {
                    return false;
                }
                if (refCount.compareAndSet(n, n + 1)) {
                    return true;
                }
            }
        }
    }

    /**
     * Result of a cache intern: the shared analyzer plus a release handle that the calling
     * {@link IndexAnalyzers} invokes on close. Each successful intern produces exactly one
     * release handle; double-release on the same handle is a bug and will under-count.
     */
    record InternedAnalyzer(NamedAnalyzer analyzer, Releasable release) {}

    private final LongAdder cacheHits = new LongAdder();
    private final LongAdder cacheMisses = new LongAdder();

    private final Environment environment;
    private final Map<String, AnalysisProvider<CharFilterFactory>> charFilters;
    private final Map<String, AnalysisProvider<TokenFilterFactory>> tokenFilters;
    private final Map<String, AnalysisProvider<TokenizerFactory>> tokenizers;
    private final Map<String, AnalysisProvider<AnalyzerProvider<?>>> analyzers;
    private final Map<String, AnalysisProvider<AnalyzerProvider<?>>> normalizers;

    public AnalysisRegistry(
        Environment environment,
        Map<String, AnalysisProvider<CharFilterFactory>> charFilters,
        Map<String, AnalysisProvider<TokenFilterFactory>> tokenFilters,
        Map<String, AnalysisProvider<TokenizerFactory>> tokenizers,
        Map<String, AnalysisProvider<AnalyzerProvider<?>>> analyzers,
        Map<String, AnalysisProvider<AnalyzerProvider<?>>> normalizers,
        Map<String, PreConfiguredCharFilter> preConfiguredCharFilters,
        Map<String, PreConfiguredTokenFilter> preConfiguredTokenFilters,
        Map<String, PreConfiguredTokenizer> preConfiguredTokenizers,
        Map<String, PreBuiltAnalyzerProviderFactory> preConfiguredAnalyzers
    ) {
        this.environment = environment;
        this.charFilters = unmodifiableMap(charFilters);
        this.tokenFilters = unmodifiableMap(tokenFilters);
        this.tokenizers = unmodifiableMap(tokenizers);
        this.analyzers = unmodifiableMap(analyzers);
        this.normalizers = unmodifiableMap(normalizers);
        prebuiltAnalysis = new PrebuiltAnalysis(
            preConfiguredCharFilters,
            preConfiguredTokenFilters,
            preConfiguredTokenizers,
            preConfiguredAnalyzers
        );
    }

    private static Settings getSettingsFromIndexSettings(IndexSettings indexSettings, String groupName) {
        Settings settings = indexSettings.getSettings().getAsSettings(groupName);
        if (settings.isEmpty()) {
            settings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, indexSettings.getIndexVersionCreated()).build();
        }
        return settings;
    }

    private static final IndexSettings NO_INDEX_SETTINGS = new IndexSettings(
        IndexMetadata.builder(IndexMetadata.INDEX_UUID_NA_VALUE)
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()))
            .numberOfReplicas(0)
            .numberOfShards(1)
            .build(),
        Settings.EMPTY
    );

    private <T> T getComponentFactory(
        IndexSettings settings,
        NameOrDefinition nod,
        String componentType,
        Function<String, AnalysisProvider<T>> globalComponentProvider,
        Function<String, AnalysisProvider<T>> prebuiltComponentProvider,
        BiFunction<String, IndexSettings, AnalysisProvider<T>> indexComponentProvider
    ) throws IOException {
        if (nod.definition != null) {
            // custom component, so we build it from scratch
            String type = nod.definition.get("type");
            if (type == null) {
                throw new IllegalArgumentException("Missing [type] setting for anonymous " + componentType + ": " + nod.definition);
            }
            AnalysisProvider<T> factory = globalComponentProvider.apply(type);
            if (factory == null) {
                throw new IllegalArgumentException("failed to find global " + componentType + " under [" + type + "]");
            }
            if (settings == null) {
                settings = NO_INDEX_SETTINGS;
            }
            return factory.get(settings, environment, "__anonymous__" + type, nod.definition);
        }
        if (settings == null) {
            // no index provided, so we use prebuilt analysis components
            AnalysisProvider<T> factory = prebuiltComponentProvider.apply(nod.name);
            if (factory == null) {
                // if there's no prebuilt component, try loading a global one to build with no settings
                factory = globalComponentProvider.apply(nod.name);
                if (factory == null) {
                    throw new IllegalArgumentException("failed to find global " + componentType + " under [" + nod.name + "]");
                }
            }
            return factory.get(environment, nod.name);
        } else {
            // get the component from index settings
            AnalysisProvider<T> factory = indexComponentProvider.apply(nod.name, settings);
            if (factory == null) {
                throw new IllegalArgumentException("failed to find " + componentType + " under [" + nod.name + "]");
            }
            Settings s = getSettingsFromIndexSettings(settings, "index.analysis." + componentType + "." + nod.name);
            return factory.get(settings, environment, nod.name, s);
        }
    }

    /**
     * Returns a registered {@link TokenizerFactory} provider by name or <code>null</code> if the tokenizer was not registered
     */
    private AnalysisModule.AnalysisProvider<TokenizerFactory> getTokenizerProvider(String tokenizer) {
        return tokenizers.getOrDefault(tokenizer, this.prebuiltAnalysis.getTokenizerFactory(tokenizer));
    }

    /**
     * Returns a registered {@link TokenFilterFactory} provider by name or <code>null</code> if the token filter was not registered
     */
    private AnalysisModule.AnalysisProvider<TokenFilterFactory> getTokenFilterProvider(String tokenFilter) {
        return tokenFilters.getOrDefault(tokenFilter, this.prebuiltAnalysis.getTokenFilterFactory(tokenFilter));
    }

    /**
     * Returns a registered {@link CharFilterFactory} provider by name or <code>null</code> if the char filter was not registered
     */
    private AnalysisModule.AnalysisProvider<CharFilterFactory> getCharFilterProvider(String charFilter) {
        return charFilters.getOrDefault(charFilter, this.prebuiltAnalysis.getCharFilterFactory(charFilter));
    }

    /**
     * Returns a registered {@link Analyzer} provider by name or <code>null</code> if the analyzer was not registered
     */
    public Analyzer getAnalyzer(String analyzer) throws IOException {
        AnalysisModule.AnalysisProvider<AnalyzerProvider<?>> analyzerProvider = this.prebuiltAnalysis.getAnalyzerProvider(analyzer);
        if (analyzerProvider == null) {
            AnalysisModule.AnalysisProvider<AnalyzerProvider<?>> provider = analyzers.get(analyzer);
            return provider == null ? null : cachedAnalyzer.computeIfAbsent(analyzer, (key) -> {
                try {
                    return provider.get(environment, key).get();
                } catch (IOException ex) {
                    throw new ElasticsearchException("failed to load analyzer for name " + key, ex);
                }
            });
        }

        return overridePositionIncrementGap(
            (NamedAnalyzer) analyzerProvider.get(environment, analyzer).get(),
            TextFieldMapper.Defaults.POSITION_INCREMENT_GAP
        );
    }

    /**
     * @return number of unique cached analyzer instances (sharing target). Together with the total
     *         live references across {@code IndexAnalyzers} this gives the per-node sharing factor
     *         (references / unique).
     */
    public int analyzerCacheSize() {
        return analyzerCache.size();
    }

    /** Test-only: the live reference count of each entry in the analyzer cache, one element per entry. */
    List<Integer> analyzerCacheRefCounts() {
        return analyzerCache.values().stream().map(e -> e.refCount.get()).toList();
    }

    /** @return number of unique cached normalizers across both keyword and whitespace variants. */
    public int normalizerCacheSize() {
        return normalizerCache.size() + whitespaceNormalizerCache.size();
    }

    /**
     * @return the total number of live references held across the cached analyzer entries (the
     *         {@link #analyzerCache} only — not the normalizer caches, whose unique count is reported
     *         separately by {@link #normalizerCacheSize()}). Each {@link IndexAnalyzers} holds one
     *         reference per analyzer cache entry it uses, so this is the numerator of the analyzer
     *         sharing factor (total references / {@link #analyzerCacheSize() unique entries}); a ratio
     *         of 1.0 means no sharing, higher means more indices share each instance. Keeping the
     *         numerator and denominator over the same cache makes that ratio meaningful.
     */
    public long totalReferences() {
        long total = 0;
        for (CacheEntry entry : analyzerCache.values()) {
            total += entry.refCount.get();
        }
        return total;
    }

    /** @return cumulative count of cache hits since the node started. */
    public long cacheHits() {
        return cacheHits.sum();
    }

    /** @return cumulative count of cache misses (analyzers actually built) since the node started. */
    public long cacheMisses() {
        return cacheMisses.sum();
    }

    /**
     * Opt-in leak detector for analyzer-focused tests. Fails fast if an {@link IndexAnalyzers}
     * was constructed but never closed — surfacing the leak at the point of the offending test
     * rather than at process shutdown. Production code never calls this (the safety net in
     * {@link #close()} closes any leftovers); but tests that exercise the shared-cache lifecycle
     * should invoke it in {@code @After} to guard against regressions in their own setup or in
     * the code under test.
     *
     * @throws AssertionError if any of the three caches still contains entries, with a message
     *         enumerating the leaked recipes.
     */
    public void assertNoCachedEntries() {
        if (analyzerCache.isEmpty() && normalizerCache.isEmpty() && whitespaceNormalizerCache.isEmpty()) {
            return;
        }
        StringBuilder msg = new StringBuilder("AnalysisRegistry has cached entries that were never released:\n");
        leakedEntries(msg, "  analyzer", analyzerCache);
        leakedEntries(msg, "  normalizer", normalizerCache);
        leakedEntries(msg, "  whitespace-normalizer", whitespaceNormalizerCache);
        throw new AssertionError(msg.toString());
    }

    private static void leakedEntries(StringBuilder out, String label, Map<AnalyzerKey, CacheEntry> cache) {
        if (cache.isEmpty()) {
            return;
        }
        for (Map.Entry<AnalyzerKey, CacheEntry> e : cache.entrySet()) {
            out.append(label).append(" cache[").append(e.getKey()).append("] refCount=").append(e.getValue().refCount.get()).append('\n');
        }
    }

    @Override
    public void close() throws IOException {
        try {
            prebuiltAnalysis.close();
        } finally {
            // Close cached analyzer wrappers. In normal shutdown each IndexAnalyzers has already
            // released its references and the caches should be empty — anything left here is the
            // safety net for shutdown paths that bypass per-index close. We close the original-scoped
            // wrapper (CacheEntry#evictable): {@link NamedAnalyzer#close} respects scope and only
            // closes the underlying Lucene Analyzer when scope == INDEX, so custom analyzers release
            // their underlying while prebuilt entries (AnalyzerScope.INDICES) correctly leave the
            // shared underlying alone.
            List<Closeable> toClose = new ArrayList<>(cachedAnalyzer.values());
            for (Map<AnalyzerKey, CacheEntry> cache : List.of(analyzerCache, normalizerCache, whitespaceNormalizerCache)) {
                for (CacheEntry entry : cache.values()) {
                    // Use the eagerly-stored evictable rather than join()ing the future, to avoid
                    // blocking on an in-flight build during shutdown — if a builder is mid-flight
                    // here something is misbehaving and we'd rather not wedge the shutdown. A slot
                    // reserved but not yet built has a null evictable and is GC-collectible anyway.
                    NamedAnalyzer a = entry.evictable;
                    if (a != null) {
                        toClose.add(a);
                    }
                }
            }
            IOUtils.close(toClose);
        }
    }

    /**
     * Creates an index-level {@link IndexAnalyzers} from this registry using the given index settings
     * and {@link IndexCreationContext}.
     */
    public IndexAnalyzers build(IndexCreationContext context, IndexSettings indexSettings) throws IOException {
        final Map<String, CharFilterFactory> charFilterFactories = buildCharFilterFactories(indexSettings);
        final Map<String, TokenizerFactory> tokenizerFactories = buildTokenizerFactories(indexSettings);
        final Map<String, TokenFilterFactory> tokenFilterFactories = buildTokenFilterFactories(indexSettings);
        final Map<String, AnalyzerProvider<?>> analyzerFactories = buildAnalyzerFactories(indexSettings);
        final Map<String, AnalyzerProvider<?>> normalizerFactories = buildNormalizerFactories(indexSettings);
        return build(
            context,
            indexSettings,
            analyzerFactories,
            normalizerFactories,
            tokenizerFactories,
            charFilterFactories,
            tokenFilterFactories
        );
    }

    /**
     * Creates a custom analyzer from a collection of {@link NameOrDefinition} specifications for each component
     *
     * Callers are responsible for closing the returned Analyzer
     */
    public NamedAnalyzer buildCustomAnalyzer(
        IndexCreationContext context,
        IndexSettings indexSettings,
        boolean normalizer,
        NameOrDefinition tokenizer,
        List<NameOrDefinition> charFilters,
        List<NameOrDefinition> tokenFilters
    ) throws IOException {
        TokenizerFactory tokenizerFactory = getComponentFactory(
            indexSettings,
            tokenizer,
            "tokenizer",
            this::getTokenizerProvider,
            prebuiltAnalysis::getTokenizerFactory,
            this::getTokenizerProvider
        );

        List<CharFilterFactory> charFilterFactories = new ArrayList<>();
        for (NameOrDefinition nod : charFilters) {
            charFilterFactories.add(
                getComponentFactory(
                    indexSettings,
                    nod,
                    "char_filter",
                    this::getCharFilterProvider,
                    prebuiltAnalysis::getCharFilterFactory,
                    this::getCharFilterProvider
                )
            );
        }

        List<TokenFilterFactory> tokenFilterFactories = new ArrayList<>();
        for (NameOrDefinition nod : tokenFilters) {
            TokenFilterFactory tff = getComponentFactory(
                indexSettings,
                nod,
                "filter",
                this::getTokenFilterProvider,
                prebuiltAnalysis::getTokenFilterFactory,
                this::getTokenFilterProvider
            );
            if (normalizer && tff instanceof NormalizingTokenFilterFactory == false) {
                throw new IllegalArgumentException("Custom normalizer may not use filter [" + tff.name() + "]");
            }
            tff = tff.getChainAwareTokenFilterFactory(context, tokenizerFactory, charFilterFactories, tokenFilterFactories, name -> {
                try {
                    return getComponentFactory(
                        indexSettings,
                        new NameOrDefinition(name),
                        "filter",
                        this::getTokenFilterProvider,
                        prebuiltAnalysis::getTokenFilterFactory,
                        this::getTokenFilterProvider
                    );
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
            tokenFilterFactories.add(tff);
        }

        Analyzer analyzer = new CustomAnalyzer(
            tokenizerFactory,
            charFilterFactories.toArray(new CharFilterFactory[] {}),
            tokenFilterFactories.toArray(new TokenFilterFactory[] {})
        );
        return produceAnalyzer(context, "__custom__", new AnalyzerProvider<>() {
            @Override
            public String name() {
                return "__custom__";
            }

            @Override
            public AnalyzerScope scope() {
                return AnalyzerScope.GLOBAL;
            }

            @Override
            public Analyzer get() {
                return analyzer;
            }

            @Override
            public Object sharingKey() {
                // Inline _analyze path bypasses the cache (indexSettings == null below), so
                // this key is never consulted; return identity as a safe placeholder.
                return this;
            }
        }, Map.of(), Map.of(), Map.of(), null, null, rel -> {});

    }

    public Map<String, TokenFilterFactory> buildTokenFilterFactories(IndexSettings indexSettings) throws IOException {
        final Map<String, Settings> tokenFiltersSettings = indexSettings.getSettings().getGroups(INDEX_ANALYSIS_FILTER);
        return buildMapping(
            Component.FILTER,
            indexSettings,
            tokenFiltersSettings,
            this.tokenFilters,
            prebuiltAnalysis.preConfiguredTokenFilters
        );
    }

    public Map<String, TokenizerFactory> buildTokenizerFactories(IndexSettings indexSettings) throws IOException {
        final Map<String, Settings> tokenizersSettings = indexSettings.getSettings().getGroups(INDEX_ANALYSIS_TOKENIZER);
        return buildMapping(Component.TOKENIZER, indexSettings, tokenizersSettings, tokenizers, prebuiltAnalysis.preConfiguredTokenizers);
    }

    public Map<String, CharFilterFactory> buildCharFilterFactories(IndexSettings indexSettings) throws IOException {
        final Map<String, Settings> charFiltersSettings = indexSettings.getSettings().getGroups(INDEX_ANALYSIS_CHAR_FILTER);
        return buildMapping(
            Component.CHAR_FILTER,
            indexSettings,
            charFiltersSettings,
            charFilters,
            prebuiltAnalysis.preConfiguredCharFilterFactories
        );
    }

    private Map<String, AnalyzerProvider<?>> buildAnalyzerFactories(IndexSettings indexSettings) throws IOException {
        final Map<String, Settings> analyzersSettings = indexSettings.getSettings().getGroups("index.analysis.analyzer");
        return buildMapping(Component.ANALYZER, indexSettings, analyzersSettings, analyzers, prebuiltAnalysis.analyzerProviderFactories);
    }

    private Map<String, AnalyzerProvider<?>> buildNormalizerFactories(IndexSettings indexSettings) throws IOException {
        final Map<String, Settings> normalizersSettings = indexSettings.getSettings().getGroups("index.analysis.normalizer");
        return buildMapping(Component.NORMALIZER, indexSettings, normalizersSettings, normalizers, Collections.emptyMap());
    }

    /**
     * Returns a registered {@link TokenizerFactory} provider by {@link IndexSettings}
     *  or a registered {@link TokenizerFactory} provider by predefined name
     *  or <code>null</code> if the tokenizer was not registered
     * @param tokenizer global or defined tokenizer name
     * @param indexSettings an index settings
     * @return {@link TokenizerFactory} provider or <code>null</code>
     */
    private AnalysisProvider<TokenizerFactory> getTokenizerProvider(String tokenizer, IndexSettings indexSettings) {
        return getProvider(
            Component.TOKENIZER,
            tokenizer,
            indexSettings,
            "index.analysis.tokenizer",
            tokenizers,
            this::getTokenizerProvider
        );
    }

    /**
     * Returns a registered {@link TokenFilterFactory} provider by {@link IndexSettings}
     *  or a registered {@link TokenFilterFactory} provider by predefined name
     *  or <code>null</code> if the tokenFilter was not registered
     * @param tokenFilter global or defined tokenFilter name
     * @param indexSettings an index settings
     * @return {@link TokenFilterFactory} provider or <code>null</code>
     */
    private AnalysisProvider<TokenFilterFactory> getTokenFilterProvider(String tokenFilter, IndexSettings indexSettings) {
        return getProvider(
            Component.FILTER,
            tokenFilter,
            indexSettings,
            "index.analysis.filter",
            tokenFilters,
            this::getTokenFilterProvider
        );
    }

    /**
     * Returns a registered {@link CharFilterFactory} provider by {@link IndexSettings}
     *  or a registered {@link CharFilterFactory} provider by predefined name
     *  or <code>null</code> if the charFilter was not registered
     * @param charFilter global or defined charFilter name
     * @param indexSettings an index settings
     * @return {@link CharFilterFactory} provider or <code>null</code>
     */
    private AnalysisProvider<CharFilterFactory> getCharFilterProvider(String charFilter, IndexSettings indexSettings) {
        return getProvider(
            Component.CHAR_FILTER,
            charFilter,
            indexSettings,
            "index.analysis.char_filter",
            charFilters,
            this::getCharFilterProvider
        );
    }

    private static <T> AnalysisProvider<T> getProvider(
        Component componentType,
        String componentName,
        IndexSettings indexSettings,
        String componentSettings,
        Map<String, AnalysisProvider<T>> providers,
        Function<String, AnalysisProvider<T>> providerFunction
    ) {
        final Map<String, Settings> subSettings = indexSettings.getSettings().getGroups(componentSettings);
        if (subSettings.containsKey(componentName)) {
            Settings currentSettings = subSettings.get(componentName);
            return getAnalysisProvider(componentType, providers, componentName, currentSettings.get("type"));
        } else {
            return providerFunction.apply(componentName);
        }
    }

    /**
     * Structural identity of an analyzer for node-level caching. Local analyzer names are
     * intentionally absent — sharing is by structural identity, not by name. Each component slot
     * holds the corresponding factory's {@link TokenFilterFactory#sharingKey()} (or the analogous
     * method on {@link TokenizerFactory} / {@link CharFilterFactory}); factories that opt out of
     * sharing return {@code this}, which falls back to identity-equality — those components don't
     * share across indices.
     *
     * <p>{@code providerIdentity} is either the literal string {@code "custom"} (for
     * {@link CustomAnalyzerProvider}, where the chain factories below carry the real identity)
     * or the {@link AnalyzerProvider}'s {@link AnalyzerProvider#sharingKey() sharingKey()}.
     *
     * <p>Version-sensitivity is the provider's responsibility: a factory whose behavior depends
     * on {@code indexVersionCreated} (e.g. Persian, Romanian, Unique) must fold the version-derived
     * state into its own {@code sharingKey} — version is NOT carried at the composition level so
     * version-invariant analyzers can still share across mixed-version nodes.
     */
    record AnalyzerKey(
        Object providerIdentity,
        Object tokenizerKey,
        List<Object> charFilterKeys,
        List<Object> tokenFilterKeys,
        int positionIncrementGap,
        int offsetGap
    ) {}

    /**
     * Wraps a factory's {@link TokenFilterFactory#sharingKey() sharingKey()} together with the
     * factory's concrete class. Folding the class in centrally guarantees that two <em>different</em>
     * factory types can never collide on an equal key — even when their keys are bare values of the
     * same shape (e.g. two filters that both key on a single {@code boolean}, or the ICU filters that
     * both key on a {@code Normalizer2}). Individual factories therefore only need their key to be
     * value-correct <em>within</em> their own type; cross-type isolation is handled here.
     */
    record FactoryKey(Class<?> factoryType, Object key) {}

    enum Component {
        ANALYZER {
            @Override
            public String toString() {
                return "analyzer";
            }
        },
        NORMALIZER {
            @Override
            public String toString() {
                return "normalizer";
            }
        },
        CHAR_FILTER {
            @Override
            public String toString() {
                return "char_filter";
            }
        },
        TOKENIZER {
            @Override
            public String toString() {
                return "tokenizer";
            }
        },
        FILTER {
            @Override
            public String toString() {
                return "filter";
            }
        };
    }

    @SuppressWarnings("unchecked")
    private <T> Map<String, T> buildMapping(
        Component component,
        IndexSettings settings,
        Map<String, Settings> settingsMap,
        Map<String, ? extends AnalysisModule.AnalysisProvider<T>> providerMap,
        Map<String, ? extends AnalysisModule.AnalysisProvider<T>> defaultInstance
    ) throws IOException {
        Settings defaultSettings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, settings.getIndexVersionCreated()).build();
        Map<String, T> factories = new HashMap<>();
        for (Map.Entry<String, Settings> entry : settingsMap.entrySet()) {
            String name = entry.getKey();
            Settings currentSettings = entry.getValue();
            String typeName = currentSettings.get("type");
            if (component == Component.ANALYZER) {
                T factory = null;
                if (typeName == null) {
                    if (currentSettings.get("tokenizer") != null) {
                        factory = (T) new CustomAnalyzerProvider(settings, name, currentSettings);
                    } else {
                        throw new IllegalArgumentException(
                            component + " [" + name + "] " + "must specify either an analyzer type, or a tokenizer"
                        );
                    }
                } else if (typeName.equals("custom")) {
                    factory = (T) new CustomAnalyzerProvider(settings, name, currentSettings);
                }
                if (factory != null) {
                    factories.put(name, factory);
                    continue;
                }
            } else if (component == Component.NORMALIZER) {
                if (typeName == null || typeName.equals("custom")) {
                    T factory = (T) new CustomNormalizerProvider(settings, name, currentSettings);
                    factories.put(name, factory);
                    continue;
                }
            }
            AnalysisProvider<T> type = getAnalysisProvider(component, providerMap, name, typeName);
            if (type == null) {
                throw new IllegalArgumentException("Unknown " + component + " type [" + typeName + "] for [" + name + "]");
            }
            final T factory = type.get(settings, environment, name, currentSettings);
            factories.put(name, factory);

        }
        // go over the char filters in the bindings and register the ones that are not configured
        for (Map.Entry<String, ? extends AnalysisProvider<T>> entry : providerMap.entrySet()) {
            String name = entry.getKey();
            AnalysisProvider<T> provider = entry.getValue();
            // we don't want to re-register one that already exists
            if (settingsMap.containsKey(name)) {
                continue;
            }
            // check, if it requires settings, then don't register it, we know default has no settings...
            if (provider.requiresAnalysisSettings()) {
                continue;
            }
            AnalysisProvider<T> defaultProvider = defaultInstance.get(name);
            final T instance;
            if (defaultProvider == null) {
                instance = provider.get(settings, environment, name, defaultSettings);
            } else {
                instance = defaultProvider.get(settings, environment, name, defaultSettings);
            }
            factories.put(name, instance);
        }

        for (Map.Entry<String, ? extends AnalysisProvider<T>> entry : defaultInstance.entrySet()) {
            final String name = entry.getKey();
            final AnalysisProvider<T> provider = entry.getValue();
            factories.putIfAbsent(name, provider.get(settings, environment, name, defaultSettings));
        }
        return factories;
    }

    private static <T> AnalysisProvider<T> getAnalysisProvider(
        Component component,
        Map<String, ? extends AnalysisProvider<T>> providerMap,
        String name,
        String typeName
    ) {
        if (typeName == null) {
            throw new IllegalArgumentException(component + " [" + name + "] must specify either an analyzer type, or a tokenizer");
        }
        AnalysisProvider<T> type = providerMap.get(typeName);
        if (type == null) {
            throw new IllegalArgumentException("Unknown " + component + " type [" + typeName + "] for [" + name + "]");
        }
        return type;
    }

    private static class PrebuiltAnalysis implements Closeable {

        final Map<String, AnalysisProvider<AnalyzerProvider<?>>> analyzerProviderFactories;
        final Map<String, ? extends AnalysisProvider<TokenFilterFactory>> preConfiguredTokenFilters;
        final Map<String, ? extends AnalysisProvider<TokenizerFactory>> preConfiguredTokenizers;
        final Map<String, ? extends AnalysisProvider<CharFilterFactory>> preConfiguredCharFilterFactories;

        private PrebuiltAnalysis(
            Map<String, PreConfiguredCharFilter> preConfiguredCharFilters,
            Map<String, PreConfiguredTokenFilter> preConfiguredTokenFilters,
            Map<String, PreConfiguredTokenizer> preConfiguredTokenizers,
            Map<String, PreBuiltAnalyzerProviderFactory> preConfiguredAnalyzers
        ) {

            Map<String, PreBuiltAnalyzerProviderFactory> analyzerProviderFactories = new HashMap<>();
            analyzerProviderFactories.putAll(preConfiguredAnalyzers);
            // Pre-build analyzers
            for (PreBuiltAnalyzers preBuiltAnalyzerEnum : PreBuiltAnalyzers.values()) {
                String name = preBuiltAnalyzerEnum.name().toLowerCase(Locale.ROOT);
                analyzerProviderFactories.put(name, new PreBuiltAnalyzerProviderFactory(name, preBuiltAnalyzerEnum));
            }

            this.analyzerProviderFactories = Collections.unmodifiableMap(analyzerProviderFactories);
            this.preConfiguredCharFilterFactories = preConfiguredCharFilters;
            this.preConfiguredTokenFilters = preConfiguredTokenFilters;
            this.preConfiguredTokenizers = preConfiguredTokenizers;
        }

        AnalysisProvider<CharFilterFactory> getCharFilterFactory(String name) {
            return preConfiguredCharFilterFactories.get(name);
        }

        AnalysisProvider<TokenFilterFactory> getTokenFilterFactory(String name) {
            return preConfiguredTokenFilters.get(name);
        }

        AnalysisProvider<TokenizerFactory> getTokenizerFactory(String name) {
            return preConfiguredTokenizers.get(name);
        }

        AnalysisProvider<AnalyzerProvider<?>> getAnalyzerProvider(String name) {
            return analyzerProviderFactories.get(name);
        }

        @Override
        public void close() throws IOException {
            IOUtils.close(analyzerProviderFactories.values().stream().map((a) -> ((PreBuiltAnalyzerProviderFactory) a)).toList());
        }
    }

    public IndexAnalyzers build(
        IndexCreationContext context,
        IndexSettings indexSettings,
        Map<String, AnalyzerProvider<?>> analyzerProviders,
        Map<String, AnalyzerProvider<?>> normalizerProviders,
        Map<String, TokenizerFactory> tokenizerFactoryFactories,
        Map<String, CharFilterFactory> charFilterFactoryFactories,
        Map<String, TokenFilterFactory> tokenFilterFactoryFactories
    ) {
        final Map<String, Settings> analyzersSettings = indexSettings.getSettings().getGroups(INDEX_ANALYSIS_ANALYZER);
        final Map<String, Settings> normalizersSettings = indexSettings.getSettings().getGroups("index.analysis.normalizer");
        Map<String, NamedAnalyzer> analyzers = new HashMap<>();
        Map<String, NamedAnalyzer> normalizers = new HashMap<>();
        Map<String, NamedAnalyzer> whitespaceNormalizers = new HashMap<>();
        // Refcount handles on the shared cache entries this index acquires during build (analyzers,
        // normalizers and whitespace normalizers alike); handed to IndexAnalyzers so close() releases
        // each exactly once — the last release on an entry drives the underlying analyzer.close() and
        // its eviction. A plain list suffices: reload() mutates shared analyzers in place and never
        // touches these handles, and close() only iterates them, so they need no keying.
        List<Releasable> releasables = new ArrayList<>();
        // Exception safety: any failure between here and the {@link IndexAnalyzers#of} call must
        // release every reference we have already interned. Without this rollback, a single
        // failing factory iteration would leak refcounts for the analyzers already built earlier
        // in the loop, pinning their cache entries until node shutdown.
        boolean handedOff = false;
        try {
            for (Map.Entry<String, AnalyzerProvider<?>> entry : analyzerProviders.entrySet()) {
                String analyzerName = entry.getKey();
                analyzers.merge(
                    analyzerName,
                    produceAnalyzer(
                        context,
                        analyzerName,
                        entry.getValue(),
                        tokenFilterFactoryFactories,
                        charFilterFactoryFactories,
                        tokenizerFactoryFactories,
                        indexSettings,
                        analyzersSettings.get(analyzerName),
                        releasables::add
                    ),
                    (k, v) -> {
                        throw new IllegalStateException("already registered analyzer with name: " + analyzerName);
                    }
                );
            }
            for (Map.Entry<String, AnalyzerProvider<?>> entry : normalizerProviders.entrySet()) {
                processNormalizerFactory(
                    entry.getKey(),
                    entry.getValue(),
                    normalizers,
                    TokenizerFactory.newFactory("keyword", KeywordTokenizer::new),
                    tokenFilterFactoryFactories,
                    charFilterFactoryFactories,
                    indexSettings,
                    normalizersSettings.get(entry.getKey()),
                    normalizerCache,
                    releasables::add
                );
                processNormalizerFactory(
                    entry.getKey(),
                    entry.getValue(),
                    whitespaceNormalizers,
                    TokenizerFactory.newFactory("whitespace", WhitespaceTokenizer::new),
                    tokenFilterFactoryFactories,
                    charFilterFactoryFactories,
                    indexSettings,
                    normalizersSettings.get(entry.getKey()),
                    whitespaceNormalizerCache,
                    releasables::add
                );
            }

            for (Analyzer analyzer : normalizers.values()) {
                analyzer.normalize("", ""); // check for deprecations
            }

            if (analyzers.containsKey(DEFAULT_ANALYZER_NAME) == false) {
                analyzers.put(
                    DEFAULT_ANALYZER_NAME,
                    produceAnalyzer(
                        context,
                        DEFAULT_ANALYZER_NAME,
                        new StandardAnalyzerProvider(indexSettings, null, DEFAULT_ANALYZER_NAME, Settings.EMPTY),
                        tokenFilterFactoryFactories,
                        charFilterFactoryFactories,
                        tokenizerFactoryFactories,
                        indexSettings,
                        null,
                        releasables::add
                    )
                );
            }
            NamedAnalyzer defaultAnalyzer = analyzers.get(DEFAULT_ANALYZER_NAME);
            if (defaultAnalyzer == null) {
                throw new IllegalArgumentException("no default analyzer configured");
            }
            defaultAnalyzer.checkAllowedInMode(AnalysisMode.ALL);
            assert Objects.equals(defaultAnalyzer.name(), DEFAULT_ANALYZER_NAME);
            if (Objects.equals(defaultAnalyzer.name(), DEFAULT_ANALYZER_NAME) == false) {
                throw new IllegalStateException("default analyzer must have the name [default] but was: [" + defaultAnalyzer.name() + "]");
            }

            if (analyzers.containsKey("default_index")) {
                throw new IllegalArgumentException(
                    "setting [index.analysis.analyzer.default_index] is not supported anymore, use "
                        + "[index.analysis.analyzer.default] instead for index ["
                        + indexSettings.getIndex().getName()
                        + "]"
                );
            }

            for (Map.Entry<String, NamedAnalyzer> analyzer : analyzers.entrySet()) {
                if (analyzer.getKey().startsWith("_")) {
                    throw new IllegalArgumentException("analyzer name must not start with '_'. got \"" + analyzer.getKey() + "\"");
                }
            }
            IndexAnalyzers result = IndexAnalyzers.of(analyzers, normalizers, whitespaceNormalizers, releasables);
            handedOff = true;
            return result;
        } finally {
            if (handedOff == false) {
                // Release every reference we already acquired so a single failure mid-build
                // does not pin cache entries. IOUtils.closeWhileHandlingException collects
                // and swallows individual close failures so we never mask the original throw
                // and never skip remaining handles after one fails.
                IOUtils.closeWhileHandlingException(releasables);
            }
        }
    }

    private NamedAnalyzer produceAnalyzer(
        IndexCreationContext context,
        String name,
        AnalyzerProvider<?> analyzerFactory,
        Map<String, TokenFilterFactory> tokenFilters,
        Map<String, CharFilterFactory> charFilters,
        Map<String, TokenizerFactory> tokenizers,
        IndexSettings indexSettings,
        Settings analyzerSettings,
        Consumer<Releasable> releasableSink
    ) {
        // Check-then-build: compute the cache key from the (already-built) factory map plus the
        // analyzer's settings, BEFORE calling provider.build(). When the cache hits, we return the
        // shared analyzer without running provider.build() — which for synonym chains is the
        // expensive step that reads from the .synonyms system index or from the synonyms file.
        // For inline _analyze calls (indexSettings == null), or when the sharing feature flag is
        // disabled (release builds without the flag set), we bypass the cache entirely and build a
        // fresh per-index analyzer.
        if (indexSettings != null && SHARED_ANALYZERS_ENABLED) {
            AnalyzerKey key = computeAnalyzerKey(analyzerFactory, analyzerSettings, tokenizers, charFilters, tokenFilters);
            CacheEntry cached = analyzerCache.get(key);
            if (cached != null) {
                InternedAnalyzer acquired = acquireIfLive(key, cached, analyzerCache);
                if (acquired != null) {
                    cacheHits.increment();
                    return handOff(name, acquired, releasableSink);
                }
                // Entry was concurrently retired; fall through to single-flight build.
            }
            // Single-flight: lookupOrIntern reserves the slot atomically, then runs the builder
            // OUTSIDE the bin lock so concurrent callers can join the same future instead of
            // each re-reading the synonyms file / .synonyms index. The hit/miss counters are
            // updated inside lookupOrIntern based on whether we built or coalesced.
            InternedAnalyzer interned = lookupOrIntern(
                key,
                () -> buildNamedAnalyzer(context, name, analyzerFactory, tokenFilters, charFilters, tokenizers),
                analyzerCache
            );
            return handOff(name, interned, releasableSink);
        }
        return buildNamedAnalyzer(context, name, analyzerFactory, tokenFilters, charFilters, tokenizers);
    }

    /**
     * Hand the release handle off to the caller's sink and return the shared analyzer under the
     * requesting index's local {@code name}. If the sink throws (e.g. an unexpected map exception
     * while we already hold an additional reference), release the freshly-acquired reference before
     * re-throwing so the cache entry never gets pinned.
     *
     * <p>The cached instance carries the local name of whichever index built it first. Sharing keys
     * on the analysis recipe, not the name, so a different index may reuse it under a different local
     * name. In that case re-wrap the shared underlying analyzer in a NamedAnalyzer bearing THIS
     * index's name: the underlying analyzer and its refcount stay shared, but the name is correct so
     * field-mapper serialization emits this index's analyzer name and round-trips through mapping
     * (de)serialization rather than leaking the original builder's name.
     */
    private static NamedAnalyzer handOff(String name, InternedAnalyzer interned, Consumer<Releasable> sink) {
        Releasable release = interned.release();
        boolean accepted = false;
        try {
            sink.accept(release);
            accepted = true;
        } finally {
            if (accepted == false) {
                IOUtils.closeWhileHandlingException(release);
            }
        }
        NamedAnalyzer shared = interned.analyzer();
        // Cache entries are re-tagged INDICES in lookupOrIntern so that the per-index wrapper handed out
        // here (and on release) does not cascade-close the shared underlying analyzer. The thin re-wrap
        // below copies the source scope, so an INDEX-scoped cache entry would reintroduce that cascade —
        // guard the invariant rather than rely on every future caller preserving it.
        assert shared.scope() != AnalyzerScope.INDEX
            : "cached shared analyzer [" + shared.name() + "] must not be INDEX-scoped; its per-index wrapper would cascade-close it";
        return shared.name().equals(name) ? shared : new NamedAnalyzer(name, shared);
    }

    private NamedAnalyzer buildNamedAnalyzer(
        IndexCreationContext context,
        String name,
        AnalyzerProvider<?> analyzerFactory,
        Map<String, TokenFilterFactory> tokenFilters,
        Map<String, CharFilterFactory> charFilters,
        Map<String, TokenizerFactory> tokenizers
    ) {
        /*
         * Lucene defaults positionIncrementGap to 0 in all analyzers but
         * Elasticsearch defaults it to 100 (see TextFieldMapper.Defaults) so we
         * override the positionIncrementGap if it doesn't match here. Prebuilt
         * analyzers already bake in this default (see PreBuiltAnalyzerProvider),
         * so for those this override is a no-op and the shared instance is
         * returned unchanged; it still applies to analyzers that arrive here
         * without the Elasticsearch default set.
         */
        int overridePositionIncrementGap = TextFieldMapper.Defaults.POSITION_INCREMENT_GAP;
        if (analyzerFactory instanceof CustomAnalyzerProvider) {
            ((CustomAnalyzerProvider) analyzerFactory).build(context, tokenizers, charFilters, tokenFilters);
            /*
             * Custom analyzers already default to the correct, version
             * dependent positionIncrementGap and the user is be able to
             * configure the positionIncrementGap directly on the analyzer so
             * we disable overriding the positionIncrementGap to preserve the
             * user's setting.
             */
            overridePositionIncrementGap = Integer.MIN_VALUE;
        }
        Analyzer analyzerF = analyzerFactory.get();
        if (analyzerF == null) {
            throw new IllegalArgumentException("analyzer [" + analyzerFactory.name() + "] created null analyzer");
        }
        NamedAnalyzer analyzer;
        if (analyzerF instanceof NamedAnalyzer namedAnalyzer) {
            analyzer = overridePositionIncrementGap(namedAnalyzer, overridePositionIncrementGap);
        } else {
            analyzer = new NamedAnalyzer(name, analyzerFactory.scope(), analyzerF, overridePositionIncrementGap);
        }
        checkVersions(analyzer);
        return analyzer;
    }

    /**
     * Look up {@code key}; if present and live, acquire one additional reference and join the
     * builder's future. Otherwise reserve a fresh slot, run {@code builder} (which may be slow —
     * synonym chains read from disk or the .synonyms index), complete the future, and return.
     * Single-flight: when N threads concurrently hit the same key, exactly one runs the builder
     * and the rest await the result.
     *
     * <p>INDEX-scoped wrappers are re-tagged GLOBAL inside the builder thread so per-index close
     * paths skip them. Build failures complete the future exceptionally and drop the entry from
     * the cache so the next attempt retries fresh.
     */
    private InternedAnalyzer lookupOrIntern(AnalyzerKey key, Supplier<NamedAnalyzer> builder, Map<AnalyzerKey, CacheEntry> cache) {
        boolean[] createdFresh = { false };
        CacheEntry entry = cache.compute(key, (k, existing) -> {
            if (existing != null && existing.tryAcquire()) {
                return existing;
            }
            // Either no existing entry, or it has just been retired by a concurrent release.
            // Install a fresh entry. refCount=1 belongs to this caller; the builder will run
            // outside the bin lock so other threads' concurrent compute() calls don't block on
            // it (they'll see the entry, tryAcquire, and join the future).
            createdFresh[0] = true;
            return new CacheEntry();
        });

        if (createdFresh[0]) {
            // Builder runs here, outside the bin lock. Concurrent callers see our entry and join.
            cacheMisses.increment();
            NamedAnalyzer raw;
            try {
                raw = builder.get();
            } catch (Throwable t) {
                // Surface to siblings, retire the slot, drop our reference.
                entry.futureAnalyzer.completeExceptionally(t);
                cache.remove(key, entry);
                entry.refCount.decrementAndGet();
                throw t;
            }
            // Re-tag INDICES (shared across indices, torn down when the last index releases it) rather
            // than INDEX: only INDEX scope cascades close() to the underlying analyzer, so the per-index
            // wrappers handed out from a shared entry must not be INDEX-scoped. INDICES also matches how
            // prebuilt shared analyzers are already scoped (PreBuiltAnalyzerProviderFactory).
            NamedAnalyzer scoped = raw.scope() == AnalyzerScope.INDEX ? new NamedAnalyzer(raw, AnalyzerScope.INDICES) : raw;
            // Close the original-scoped wrapper on eviction (see CacheEntry#evictable): for custom
            // analyzers `raw` is INDEX-scoped so its close() cascades to the underlying analyzer,
            // whereas the INDICES `scoped` wrapper we hand out would not.
            entry.evictable = raw;
            entry.futureAnalyzer.complete(scoped);
        } else {
            // We adopted a sibling's entry — count as a coalesced hit, not a build.
            cacheHits.increment();
        }

        return materialize(key, entry, cache);
    }

    /**
     * Acquire an additional reference on an existing cache entry, returning {@code null} if the
     * entry has been retired. Used by the fast-path cache hit where the entry is already
     * (typically) materialized.
     */
    private static InternedAnalyzer acquireIfLive(AnalyzerKey key, CacheEntry entry, Map<AnalyzerKey, CacheEntry> cache) {
        if (entry.tryAcquire() == false) {
            return null;
        }
        return materialize(key, entry, cache);
    }

    /**
     * Block until the entry's future completes, then wrap the analyzer in an
     * {@link InternedAnalyzer} with its release callback. If the build failed (or fails after
     * we've acquired our reference), release the placeholder ref and surface the failure as a
     * RuntimeException — the cache entry has already been removed by the failing builder, so a
     * retry will rebuild.
     */
    private static InternedAnalyzer materialize(AnalyzerKey key, CacheEntry entry, Map<AnalyzerKey, CacheEntry> cache) {
        NamedAnalyzer analyzer;
        try {
            analyzer = entry.futureAnalyzer.join();
        } catch (CompletionException ce) {
            // Build failed. We never owned a closeable analyzer, so just drop our refcount; the
            // failing builder already removed the slot from the cache.
            entry.refCount.decrementAndGet();
            Throwable cause = ce.getCause();
            // Preserve Error semantics: an OutOfMemoryError (or other Error) from the builder must
            // not be downgraded to a RuntimeException, or JVM-level error handling is suppressed.
            if (cause instanceof Error err) {
                throw err;
            }
            if (cause instanceof RuntimeException r) {
                throw r;
            }
            throw new RuntimeException(cause);
        }
        return new InternedAnalyzer(analyzer, () -> releaseFromCache(key, entry, cache));
    }

    /**
     * Decrement an entry's reference count under the cache bin lock. On reaching zero the analyzer
     * is closed and, if the entry is still the current value for {@code key}, removed from the
     * cache. If a different entry now occupies {@code key} — a concurrent release retired this one
     * and a fresh build re-interned the same recipe — we still close this (now superseded) entry
     * but leave the current mapping untouched.
     */
    private static void releaseFromCache(AnalyzerKey key, CacheEntry entry, Map<AnalyzerKey, CacheEntry> cache) {
        cache.compute(key, (k, current) -> {
            int after = entry.refCount.decrementAndGet();
            if (after > 0) {
                return current;
            }
            // The future MUST be complete-with-value here: a refcount can only reach zero through
            // releases of materialize()-returned handles, which only exist after a successful
            // join(). Close the original-scoped wrapper (CacheEntry#evictable) so the underlying
            // analyzer — and its CloseableThreadLocal / SynonymMap — is actually released, rather
            // than the GLOBAL-retagged wrapper we handed out (whose close() would not cascade).
            try {
                entry.evictable.close();
            } catch (Exception e) {
                // Best-effort close: an evicted analyzer's resources are CPU-only (CloseableThreadLocal,
                // SynonymMap), so a close failure leaks at most a little memory and is not actionable —
                // log at debug rather than failing the release.
                logger.debug(() -> "failed to close evicted analyzer [" + key + "]", e);
            }
            return current == entry ? null : current;
        });
    }

    private static AnalyzerKey computeAnalyzerKey(
        AnalyzerProvider<?> provider,
        Settings analyzerSettings,
        Map<String, TokenizerFactory> tokenizerFactories,
        Map<String, CharFilterFactory> charFilterFactories,
        Map<String, TokenFilterFactory> tokenFilterFactories
    ) {
        Settings recipeSettings = analyzerSettings == null ? Settings.EMPTY : analyzerSettings;
        if (provider instanceof CustomAnalyzerProvider || provider instanceof CustomNormalizerProvider) {
            // Custom analyzers and normalizers share by chain identity regardless of the local name
            // they were given. Each chain slot holds the corresponding factory's {@link #sharingKey()}.
            // Normalizers have no tokenizer (the keyword/whitespace tokenizer is fixed and lives in a
            // separate cache), so the tokenizer slot resolves to null for them.
            String tokenizerName = recipeSettings.get("tokenizer");
            Object tokenizerKey = tokenizerName == null ? null : sharingKey(tokenizerFactories.get(tokenizerName));
            List<Object> charFilterKeys = recipeSettings.getAsList("char_filter")
                .stream()
                .map(n -> sharingKey(charFilterFactories.get(n)))
                .toList();
            List<Object> tokenFilterKeys = recipeSettings.getAsList("filter")
                .stream()
                .map(n -> sharingKey(tokenFilterFactories.get(n)))
                .toList();
            int gap = recipeSettings.getAsInt("position_increment_gap", TextFieldMapper.Defaults.POSITION_INCREMENT_GAP);
            // offset_gap is baked per-index into the (Reloadable)CustomAnalyzer just like the
            // position gap, so it must be part of the key or indices differing only in offset_gap
            // would wrongly share. -1 is the "use Lucene default" sentinel (CustomAnalyzerProvider).
            int offsetGap = recipeSettings.getAsInt("offset_gap", -1);
            return new AnalyzerKey("custom", tokenizerKey, charFilterKeys, tokenFilterKeys, gap, offsetGap);
        }
        // For prebuilt / built-in / plugin analyzer providers we delegate to
        // {@link AnalyzerProvider#sharingKey()}, folding in the provider class so two different
        // provider types can never collide. Version-sensitive providers fold version-derived state
        // into their own key; version is not carried here. Prebuilt providers have no offset_gap
        // setting, so -1 (the custom default) keeps the literal consistent.
        return new AnalyzerKey(
            new FactoryKey(provider.getClass(), provider.sharingKey()),
            null,
            List.of(),
            List.of(),
            TextFieldMapper.Defaults.POSITION_INCREMENT_GAP,
            -1
        );
    }

    private static Object sharingKey(TokenizerFactory factory) {
        return factory == null ? null : new FactoryKey(factory.getClass(), factory.sharingKey());
    }

    private static Object sharingKey(CharFilterFactory factory) {
        return factory == null ? null : new FactoryKey(factory.getClass(), factory.sharingKey());
    }

    private static Object sharingKey(TokenFilterFactory factory) {
        return factory == null ? null : new FactoryKey(factory.getClass(), factory.sharingKey());
    }

    /**
     * Reload the synonyms (and any other reloadable resources) of {@code currentReference} in place,
     * rebuilding its {@link ReloadableCustomAnalyzer} components from the current resource state. The
     * mutation is observed by existing mapping references and by every index sharing this instance; see
     * {@link ReloadableCustomAnalyzer#reload} for why that is safe and how {@code reloadToken} dedups a
     * shared instance to a single rebuild per request.
     */
    public void reloadAnalyzerInPlace(IndexSettings indexSettings, String name, NamedAnalyzer currentReference, ReloadToken reloadToken)
        throws IOException {
        ReloadableCustomAnalyzer reloadable = (ReloadableCustomAnalyzer) currentReference.analyzer();
        // Look up the recipe under {@code name} — the requesting index's LOCAL analyzer name — rather
        // than {@code currentReference.name()}: a shared analyzer keeps the name of whichever index
        // built it first, which need not match this index's name for the same recipe. Using the
        // embedded name would read the wrong (or a missing) settings group for every other sharer.
        Settings analyzerSettings = indexSettings.getSettings().getGroups(INDEX_ANALYSIS_ANALYZER).get(name);
        if (analyzerSettings == null) {
            // No recipe under this index's local name — reloadable analyzers always have one. Bail
            // WITHOUT claiming the token, so a sharer that does carry the recipe can still reload it.
            return;
        }
        if (reloadable.shouldReload(reloadToken) == false) {
            // Already reloaded for this request, or already loaded and this is only a recovery load — skip
            // building the inputs. reload() re-checks authoritatively under its lock.
            return;
        }
        Map<String, TokenizerFactory> tokenizerFactories = buildTokenizerFactories(indexSettings);
        Map<String, CharFilterFactory> charFilterFactories = buildCharFilterFactories(indexSettings);
        Map<String, TokenFilterFactory> tokenFilterFactories = buildTokenFilterFactories(indexSettings);
        // Rebuild the components fresh from settings. This reads from disk / the .synonyms index for
        // synonym chains; lenient handling lives inside the synonym factory.
        reloadable.reload(reloadToken, name, analyzerSettings, tokenizerFactories, charFilterFactories, tokenFilterFactories);
    }

    private static NamedAnalyzer overridePositionIncrementGap(NamedAnalyzer analyzer, int overridePositionIncrementGap) {
        if (overridePositionIncrementGap >= 0 && analyzer.getPositionIncrementGap(analyzer.name()) != overridePositionIncrementGap) {
            analyzer = new NamedAnalyzer(analyzer, overridePositionIncrementGap);
        }
        return analyzer;
    }

    private void processNormalizerFactory(
        String name,
        AnalyzerProvider<?> normalizerFactory,
        Map<String, NamedAnalyzer> normalizers,
        TokenizerFactory tokenizerFactory,
        Map<String, TokenFilterFactory> tokenFilters,
        Map<String, CharFilterFactory> charFilters,
        IndexSettings indexSettings,
        Settings normalizerSettings,
        Map<AnalyzerKey, CacheEntry> cache,
        Consumer<Releasable> releasableSink
    ) {
        if (tokenizerFactory == null) {
            throw new IllegalStateException("keyword tokenizer factory is null, normalizers require analysis-common module");
        }
        if (normalizers.containsKey(name)) {
            throw new IllegalStateException("already registered analyzer with name: " + name);
        }
        // Check-then-build, mirroring {@link #produceAnalyzer}. The tokenizer map is intentionally
        // empty in the key derivation: keyword vs whitespace normalizers use separate caches
        // (passed in), so they cannot collide despite sharing the recipe. When the sharing feature
        // flag is disabled we build a fresh per-index normalizer and never touch the cache.
        if (indexSettings != null && SHARED_ANALYZERS_ENABLED) {
            AnalyzerKey key = computeAnalyzerKey(normalizerFactory, normalizerSettings, Map.of(), charFilters, tokenFilters);
            CacheEntry cached = cache.get(key);
            if (cached != null) {
                InternedAnalyzer acquired = acquireIfLive(key, cached, cache);
                if (acquired != null) {
                    cacheHits.increment();
                    normalizers.put(name, handOff(name, acquired, releasableSink));
                    return;
                }
            }
            InternedAnalyzer interned = lookupOrIntern(
                key,
                () -> buildNamedNormalizer(name, normalizerFactory, tokenizerFactory, tokenFilters, charFilters),
                cache
            );
            normalizers.put(name, handOff(name, interned, releasableSink));
            return;
        }
        normalizers.put(name, buildNamedNormalizer(name, normalizerFactory, tokenizerFactory, tokenFilters, charFilters));
    }

    private static NamedAnalyzer buildNamedNormalizer(
        String name,
        AnalyzerProvider<?> normalizerFactory,
        TokenizerFactory tokenizerFactory,
        Map<String, TokenFilterFactory> tokenFilters,
        Map<String, CharFilterFactory> charFilters
    ) {
        if (normalizerFactory instanceof CustomNormalizerProvider) {
            ((CustomNormalizerProvider) normalizerFactory).build(tokenizerFactory, charFilters, tokenFilters);
        }
        Analyzer normalizerF = normalizerFactory.get();
        if (normalizerF == null) {
            throw new IllegalArgumentException("normalizer [" + normalizerFactory.name() + "] created null normalizer");
        }
        return new NamedAnalyzer(name, normalizerFactory.scope(), normalizerF);
    }

    // Some analysis components emit deprecation warnings or throw exceptions when used
    // with the wrong version of elasticsearch. These exceptions and warnings are
    // normally thrown when tokenstreams are constructed, which unless we build a
    // tokenstream up-front does not happen until a document is indexed. In order to
    // surface these warnings or exceptions as early as possible, we build an empty
    // tokenstream and pull it through an Analyzer at construction time.
    private static void checkVersions(Analyzer analyzer) {
        try (TokenStream ts = analyzer.tokenStream("", "")) {
            ts.reset();
            while (ts.incrementToken()) {
            }
            ts.end();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
