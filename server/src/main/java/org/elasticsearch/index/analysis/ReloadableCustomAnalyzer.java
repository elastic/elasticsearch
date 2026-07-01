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
            try {
                TokenStreamComponents tokenStream = (TokenStreamComponents) getStoredValue(analyzer);
                assert tokenStream != null;
                return tokenStream;
            } catch (NullPointerException e) {
                // close() nulled the analyzer's reuse thread-local between getStoredComponents() and here.
                throw alreadyClosed();
            }
        }

        @Override
        public void setReusableComponents(Analyzer analyzer, String fieldName, TokenStreamComponents tokenStream) {
            try {
                setStoredValue(analyzer, tokenStream);
            } catch (NullPointerException e) {
                // close() nulled the analyzer's reuse thread-local while this stream was being created.
                throw alreadyClosed();
            }
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

    // The reload request token of the last ATTEMPT on this shared instance — success OR failure — or null
    // before any explicit reload. Together with lastReloadFailure it makes a single request act on this
    // instance exactly once even though it fans out across every index sharing it: the first sharer to
    // arrive attempts the rebuild and the rest observe its outcome. Written only under the monitor in
    // reload(); volatile so the unsynchronized shouldReload() hint can read it without blocking behind an
    // in-flight (slow) reload.
    private volatile Object lastReloadToken;

    // The failure thrown by the attempt recorded in lastReloadToken, or null if that attempt succeeded (or
    // none has run). A non-null value is REPLAYED to later sharers carrying the same token: they re-throw
    // it (so their shard reports the failure too) instead of rebuilding. Sharers carry a byte-identical
    // recipe, so a rebuild would only repeat the same failure — and for a resource failure such as a
    // synonym map that trips the circuit breaker, retrying on every sharing shard would turn one failure
    // into a storm. Cleared on the next successful rebuild; a fresh request (new token) ignores it and
    // rebuilds. Written under the monitor, ALWAYS paired with lastReloadToken and ordered so a
    // shouldReload() reader that observes a matching token also observes the matching failure state;
    // volatile for that unsynchronized read.
    private volatile RuntimeException lastReloadFailure;

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
     * already <em>successfully</em> reloaded this instance. It returns {@code true} when that token's
     * attempt failed, so the registry re-enters {@link #reload} to replay the failure to this sharer. This
     * is only a hint — it does not mutate dedup state, so under concurrency it may return {@code true} for
     * more than one caller; {@link #reload} makes the authoritative, atomic decision under the lock.
     */
    public boolean shouldReload(Object token) {
        if (closed) {
            return false;
        }
        if (token == null) {
            return loaded == false;
        }
        if (token == lastReloadToken) {
            // Already attempted for this request: re-enter reload() only to replay a remembered failure.
            // A successful attempt published in place, so a coasting sharer needs no follow-up. (lastReloadFailure
            // is written before lastReloadToken, so observing the matching token here means the failure
            // state is already visible.)
            return lastReloadFailure != null;
        }
        return true;
    }

    /**
     * Rebuilds and publishes the analyzer's components from the given inputs — unless this reload is not
     * needed, decided atomically under the lock so concurrent reloads never rebuild the same instance
     * more than once for the same reason:
     * <ul>
     *   <li>a {@code null} token is the deferred initial resource load fired by shard recovery; because
     *       one instance is shared across indices it only needs to load once per node, so it is a no-op
     *       once {@link #loaded};</li>
     *   <li>a non-null token is an explicit {@code _reload_search_analyzers} request. The first sharer
     *       carrying a given token attempts the rebuild; later sharers carrying the same token observe its
     *       outcome rather than rebuild — they coast if it succeeded (the rebuild published in place, so
     *       they are already up to date) or re-throw its failure if it did not. Sharers carry a
     *       byte-identical recipe, so rebuilding on each would only repeat the same outcome; replaying a
     *       failure in particular avoids turning one circuit-breaker trip (e.g. a large synonym map) into a
     *       storm across every sharing shard. A subsequent request carries a new token and rebuilds.</li>
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
            // This request already attempted this shared instance. Replay that outcome instead of
            // rebuilding: re-throw a remembered failure so this sharer's shard reports it too, or coast
            // (the successful rebuild already published in place, so this sharer is up to date).
            if (lastReloadFailure != null) {
                throw lastReloadFailure;
            }
            return;
        }
        final AnalyzerComponents rebuilt;
        try {
            rebuilt = AnalyzerComponents.createComponents(
                IndexCreationContext.RELOAD_ANALYZERS,
                name,
                settings,
                tokenizers,
                charFilters,
                tokenFilters
            );
        } catch (RuntimeException e) {
            // Remember an explicit reload's failure so the other indices sharing this instance in the same
            // request replay it (see the dedup branch above) instead of each re-running the identically
            // failing build — which for a circuit-breaker trip would be a storm of failures. A null-token
            // recovery load is per-shard and idempotent, so it is left to retry on the next shard opening.
            // If close() already won there is no one left to report to, so skip recording. Write the
            // failure BEFORE the token so a shouldReload() reader that sees the token also sees the failure.
            if (reloadToken != null && closed == false) {
                this.lastReloadFailure = e;
                this.lastReloadToken = reloadToken;
            }
            throw e;
        }
        if (closed) {
            // The last sharer released this instance while we were rebuilding. close() wins: there is no
            // one left to query it, so drop the freshly built components rather than publish them onto a
            // torn-down analyzer (whose only reader, getStoredComponents(), now throws).
            return;
        }
        this.components = rebuilt;
        this.loaded = true;
        // Publish the request token now that the rebuild has succeeded, clearing any failure remembered for
        // an earlier token so a later sharer carrying this token coasts on the fresh state rather than
        // replaying a stale error. Clear the failure BEFORE the token so a shouldReload() reader that sees
        // this token also sees the cleared failure state.
        if (reloadToken != null) {
            this.lastReloadFailure = null;
            this.lastReloadToken = reloadToken;
        }
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
        if (closed) {
            throw alreadyClosed();
        }
        try {
            storedComponents.set(components);
        } catch (NullPointerException e) {
            // close() raced this access and tore down the CloseableThreadLocal between the check and here.
            throw alreadyClosed();
        }
    }

    private AnalyzerComponents getStoredComponents() {
        if (closed) {
            throw alreadyClosed();
        }
        try {
            return storedComponents.get();
        } catch (NullPointerException e) {
            // close() raced this access and tore down the CloseableThreadLocal between the check and here.
            throw alreadyClosed();
        }
    }

    /**
     * close() runs at refcount 0 (the last sharer released this instance) and tears down the
     * {@link CloseableThreadLocal}. A query that was already tokenizing when that happened must fail like
     * any other closed Lucene analyzer rather than NPE on the now-null thread-local. The {@link #closed}
     * flag handles the common case; the {@code NullPointerException} catch above closes the tiny
     * check-then-act window where close() lands mid-access.
     */
    private static AlreadyClosedException alreadyClosed() {
        return new AlreadyClosedException("analyzer is closed");
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
