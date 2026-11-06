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
import org.apache.lucene.analysis.DelegatingAnalyzerWrapper;
import org.elasticsearch.index.mapper.MappingLookup;

import java.util.IdentityHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * The index-time analyzer used by the {@link org.apache.lucene.index.IndexWriter}. Resolves each field's
 * {@link NamedAnalyzer} from the current {@link MappingLookup} and delegates to a {@link TokenCountingAnalyzer}
 * around the analyzer inside it.
 *
 * <p>Lucene caches {@link TokenStreamComponents} per thread on the analyzer that stores them. Wrapping
 * the per-field analyzer as a whole in an {@link org.apache.lucene.analysis.AnalyzerWrapper} would cache
 * a separate analysis chain per field, so instead there is one {@link TokenCountingAnalyzer} per
 * underlying analyzer, shared by all fields that use it. Each wrapper caches its own chain, separate from
 * the one the underlying analyzer caches for other callers, so the number of chains per thread is bounded
 * by the number of analyzers rather than the number of fields.
 *
 * <p>The wrappers are discarded whenever the {@link MappingLookup} changes, and their chains are rebuilt
 * on next use. Some mappers build new underlying analyzers each time they are rebuilt during a mapping
 * update (for example text fields with {@code index_phrases} or {@code index_prefixes},
 * {@code search_as_you_type}, {@code completion} and {@code annotated_text}), so wrappers kept across
 * lookups would retain stale analyzers indefinitely. Rebuilding costs one chain per analyzer and indexing
 * thread after each mapping update, which is small compared to the mapping update itself. Discarded
 * wrappers are not closed; they and their cached chains are garbage collected.
 */
public final class TokenCountingIndexAnalyzer extends DelegatingAnalyzerWrapper {

    private final Supplier<MappingLookup> mappingLookupSupplier;
    private final TokenCountingMetrics metrics;
    private final AtomicReference<Wrappers> wrappers = new AtomicReference<>();

    /**
     * @param mappingLookupSupplier supplies the current mapping lookup used to resolve each field's index analyzer
     * @param metrics               metrics instance for recording token counts per field value
     */
    public TokenCountingIndexAnalyzer(Supplier<MappingLookup> mappingLookupSupplier, TokenCountingMetrics metrics) {
        super(PER_FIELD_REUSE_STRATEGY);
        this.mappingLookupSupplier = mappingLookupSupplier;
        this.metrics = metrics;
    }

    @Override
    protected Analyzer getWrappedAnalyzer(String fieldName) {
        // Wrappers are only published if no other thread has published since they were read, and the lookup is
        // read after the wrappers, so a thread holding an older lookup can't replace wrappers built for a newer one.
        while (true) {
            Wrappers current = wrappers.get();
            MappingLookup lookup = mappingLookupSupplier.get();
            if (current != null && current.lookup == lookup) {
                return current.get(fieldName);
            }
            Wrappers next = new Wrappers(lookup);
            if (wrappers.compareAndSet(current, next)) {
                return next.get(fieldName);
            }
        }
    }

    // The wrappers are shared by fields that may configure different gaps, so the gaps come from the field's NamedAnalyzer.

    @Override
    public int getPositionIncrementGap(String fieldName) {
        return indexAnalyzer(mappingLookupSupplier.get(), fieldName).getPositionIncrementGap(fieldName);
    }

    @Override
    public int getOffsetGap(String fieldName) {
        return indexAnalyzer(mappingLookupSupplier.get(), fieldName).getOffsetGap(fieldName);
    }

    private static NamedAnalyzer indexAnalyzer(MappingLookup lookup, String fieldName) {
        return lookup.indexAnalyzer(
            fieldName,
            f -> { throw new IllegalArgumentException("Field [" + f + "] has no associated analyzer"); }
        );
    }

    /**
     * The token counting wrappers for a single {@link MappingLookup}.
     */
    private final class Wrappers {
        private final MappingLookup lookup;
        private final Map<String, Analyzer> byField = new ConcurrentHashMap<>();
        // keyed by identity: analyzers don't define equality, and NamedAnalyzer compares names only
        private final Map<Analyzer, Analyzer> byAnalyzer = new IdentityHashMap<>();

        private Wrappers(MappingLookup lookup) {
            this.lookup = lookup;
        }

        Analyzer get(String fieldName) {
            Analyzer wrapper = byField.get(fieldName);
            if (wrapper == null) {
                wrapper = byField.computeIfAbsent(fieldName, this::create);
            }
            return wrapper;
        }

        private Analyzer create(String fieldName) {
            Analyzer analyzer = indexAnalyzer(lookup, fieldName).analyzer();
            // Runs inside byField.computeIfAbsent. Locks are always taken in that order, and creating a wrapper
            // doesn't touch byField, so this can't deadlock.
            synchronized (byAnalyzer) {
                return byAnalyzer.computeIfAbsent(analyzer, a -> new TokenCountingAnalyzer(a, metrics));
            }
        }
    }
}
