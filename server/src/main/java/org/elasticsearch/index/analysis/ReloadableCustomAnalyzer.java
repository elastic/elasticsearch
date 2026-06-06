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
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.util.CloseableThreadLocal;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.index.IndexService.IndexCreationContext;

import java.io.Reader;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public final class ReloadableCustomAnalyzer extends Analyzer implements AnalyzerComponentsProvider {

    private volatile AnalyzerComponents components;

    private CloseableThreadLocal<AnalyzerComponents> storedComponents = new CloseableThreadLocal<>();

    // external resources that this analyzer is based on
    private final Set<String> resources;

    private final int positionIncrementGap;

    private final int offsetGap;

    /**
     * An alternative {@link ReuseStrategy} that allows swapping the stored analyzer components when they change.
     * This is used to change e.g. token filters in search time analyzers.
     */
    private static final ReuseStrategy UPDATE_STRATEGY = new ReuseStrategy() {
        @Override
        public TokenStreamComponents getReusableComponents(Analyzer analyzer, String fieldName) {
            ReloadableCustomAnalyzer custom = (ReloadableCustomAnalyzer) analyzer;
            AnalyzerComponents components = custom.getComponents();
            AnalyzerComponents storedComponents = custom.getStoredComponents();
            if (storedComponents == null || components != storedComponents) {
                custom.setStoredComponents(components);
                return null;
            }
            TokenStreamComponents tokenStream = (TokenStreamComponents) getStoredValue(analyzer);
            assert tokenStream != null;
            return tokenStream;
        }

        @Override
        public void setReusableComponents(Analyzer analyzer, String fieldName, TokenStreamComponents tokenStream) {
            setStoredValue(analyzer, tokenStream);
        }
    };

    public ReloadableCustomAnalyzer(AnalyzerComponents components, int positionIncrementGap, int offsetGap) {
        super(UPDATE_STRATEGY);
        if (components.analysisMode().equals(AnalysisMode.SEARCH_TIME) == false) {
            throw new IllegalArgumentException(
                "ReloadableCustomAnalyzer must only be initialized with analysis components in AnalysisMode.SEARCH_TIME mode"
            );
        }
        this.components = components;
        this.positionIncrementGap = positionIncrementGap;
        this.offsetGap = offsetGap;

        Set<String> resourcesTemp = new HashSet<>();
        for (TokenFilterFactory tokenFilter : components.getTokenFilters()) {
            resourcesTemp.addAll(tokenFilter.getResourceNames());
        }
        resources = resourcesTemp.isEmpty() ? null : Set.copyOf(resourcesTemp);
    }

    @Override
    public AnalyzerComponents getComponents() {
        return this.components;
    }

    public boolean usesResource(String resourceName) {
        if (resourceName == null) {
            return true;
        }
        if (resources == null) {
            return false;
        }
        return resources.contains(resourceName);
    }

    @Override
    public int getPositionIncrementGap(String fieldName) {
        return this.positionIncrementGap;
    }

    @Override
    public int getOffsetGap(String field) {
        if (this.offsetGap < 0) {
            return super.getOffsetGap(field);
        }
        return this.offsetGap;
    }

    public AnalysisMode getAnalysisMode() {
        return this.components.analysisMode();
    }

    @Override
    protected Reader initReaderForNormalization(String fieldName, Reader reader) {
        final AnalyzerComponents components = getComponents();
        for (CharFilterFactory charFilter : components.getCharFilters()) {
            reader = charFilter.normalize(reader);
        }
        return reader;
    }

    @Override
    protected TokenStream normalize(String fieldName, TokenStream in) {
        final AnalyzerComponents components = getComponents();
        TokenStream result = in;
        for (TokenFilterFactory filter : components.getTokenFilters()) {
            result = filter.normalize(result);
        }
        return result;
    }

    // The reload token (the reload request) this analyzer was last reloaded for. Written only under the
    // monitor in reload(); volatile so the unsynchronized shouldReload() hint can read it without
    // blocking behind an in-flight (slow) reload.
    private volatile Object lastReloadToken;

    // Set once this analyzer has been loaded from its resources (synonyms etc.) at least once. The
    // initial load is deferred from build time to shard recovery (IndicesService#beforeIndexShardRecovery,
    // a null-token reload); because one instance is shared across indices, that initial load only needs
    // to happen once per node. Later shard recoveries — including those of other indices that share this
    // instance — observe this flag (in reload(), under the lock) and skip, instead of rebuilding the
    // analyzer on every shard opening. Volatile so shouldReload() can read it without the lock.
    private volatile boolean loaded;

    // Set by close() once the last sharer has released this instance. reload() (synchronized) observes
    // it and discards its result rather than mutate an analyzer nobody references; getStoredComponents()
    // observes it and fails fast with AlreadyClosedException rather than dereference a closed
    // CloseableThreadLocal. Volatile so close() can set it WITHOUT taking the reload monitor — close()
    // runs while the registry holds its cache lock and must never wait behind a (slow) reload build.
    private volatile boolean closed;

    /**
     * Cheap pre-check the registry uses to skip building reload inputs for a reload that {@link #reload}
     * would skip anyway: {@code false} when the analyzer is closed, when a {@code null} (recovery) token
     * arrives after the instance has already been loaded once, or when a non-null request token has
     * already reloaded this instance. This is only a hint — it does not mutate dedup state, so under
     * concurrency it may return {@code true} for more than one caller; {@link #reload} makes the
     * authoritative, atomic decision under the lock.
     */
    public boolean shouldReload(Object token) {
        if (closed) {
            return false;
        }
        if (token == null) {
            return loaded == false;
        }
        return token != lastReloadToken;
    }

    /**
     * Rebuilds and publishes the analyzer's components from the given inputs — unless this reload is not
     * needed, decided atomically under the lock so concurrent reloads never rebuild the same instance
     * more than once for the same reason:
     * <ul>
     *   <li>a {@code null} token is the deferred initial resource load fired by shard recovery; because
     *       one instance is shared across indices it only needs to load once per node, so it is a no-op
     *       once {@link #loaded};</li>
     *   <li>a non-null token is an explicit {@code _reload_search_analyzers} request; it always rebuilds,
     *       except that the once-per-request token dedups the broadcast to a shared instance.</li>
     * </ul>
     * {@code synchronized} so reloads serialize and never build in parallel; {@link #close} does NOT take
     * this monitor (it only flips the volatile {@link #closed} flag), so it never blocks behind a build.
     */
    public synchronized void reload(
        Object reloadToken,
        String name,
        Settings settings,
        final Map<String, TokenizerFactory> tokenizers,
        final Map<String, CharFilterFactory> charFilters,
        final Map<String, TokenFilterFactory> tokenFilters
    ) {
        if (closed) {
            return;
        }
        if (reloadToken == null) {
            if (loaded) {
                // Initial resource load already done (possibly by a concurrent recovery claim). Skip the
                // rebuild rather than re-read the source on every shard opening.
                return;
            }
        } else if (reloadToken == lastReloadToken) {
            // This broadcast request already reloaded this shared instance.
            return;
        } else {
            lastReloadToken = reloadToken;
        }
        AnalyzerComponents components = AnalyzerComponents.createComponents(
            IndexCreationContext.RELOAD_ANALYZERS,
            name,
            settings,
            tokenizers,
            charFilters,
            tokenFilters
        );
        if (closed) {
            // The last sharer released this instance while we were rebuilding. close() wins: there is no
            // one left to query it, so drop the freshly built components rather than publish them onto a
            // torn-down analyzer (whose only reader, getStoredComponents(), now throws).
            return;
        }
        this.components = components;
        this.loaded = true;
    }

    @Override
    public void close() {
        // Not synchronized on purpose: close() runs while the registry holds its cache lock, so it must
        // never wait behind a reload build. Flagging closed (volatile) is enough — a concurrent reload()
        // drops its result, and any tokenStream() that raced this close fails fast with
        // AlreadyClosedException instead of NPE-ing on the now-closed CloseableThreadLocal.
        closed = true;
        super.close();
        storedComponents.close();
    }

    private void setStoredComponents(AnalyzerComponents components) {
        storedComponents.set(components);
    }

    private AnalyzerComponents getStoredComponents() {
        if (closed) {
            // The instance was released and closed (its CloseableThreadLocal is torn down). Behave like
            // any other closed Lucene analyzer rather than NPE on the nulled thread-local.
            throw new AlreadyClosedException("analyzer is closed");
        }
        return storedComponents.get();
    }

    @Override
    protected TokenStreamComponents createComponents(String fieldName) {
        AnalyzerComponents stored = getStoredComponents();
        final AnalyzerComponents components = stored != null ? stored : getComponents();
        Tokenizer tokenizer = components.getTokenizerFactory().create();
        TokenStream tokenStream = tokenizer;
        for (TokenFilterFactory tokenFilter : components.getTokenFilters()) {
            tokenStream = tokenFilter.create(tokenStream);
        }
        return new TokenStreamComponents(tokenizer, tokenStream);
    }

    @Override
    protected Reader initReader(String fieldName, Reader reader) {
        AnalyzerComponents stored = getStoredComponents();
        // AnalyzerWrapper subclasses that wrap this RCA bypass UPDATE_STRATEGY, so storedComponents
        // may be unset; fall back to the volatile. initReader and createComponents may then see
        // different versions, but char filters are never updateable so the difference is harmless.
        final AnalyzerComponents components = stored != null ? stored : getComponents();
        if (CollectionUtils.isEmpty(components.getCharFilters()) == false) {
            for (CharFilterFactory charFilter : components.getCharFilters()) {
                reader = charFilter.create(reader);
            }
        }
        return reader;
    }
}
