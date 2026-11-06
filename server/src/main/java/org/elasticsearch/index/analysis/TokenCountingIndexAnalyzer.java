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
 * underlying analyzer, shared by all fields and shards that use it (see {@link TokenCountingMetrics}).
 * Each wrapper caches its own chain, separate from the one the underlying analyzer caches for other callers,
 * so the number of chains per thread is bounded by the number of analyzers on the node rather than the
 * number of fields or shards.
 *
 * <p>The per-field lookups are discarded whenever the {@link MappingLookup} changes, because some mappers
 * build new underlying analyzers during a mapping update (for example text fields with {@code index_phrases}
 * or {@code index_prefixes}, {@code search_as_you_type}, {@code completion} and {@code annotated_text}).
 * Wrappers are held weakly node-wide, so a wrapper and its chains survive while any shard still uses its
 * analyzer, and are garbage collected, not closed, once none does.
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
            // Runs inside byField.computeIfAbsent and then takes the metrics lock, which never touches byField,
            // so this can't deadlock.
            return metrics.wrapperFor(indexAnalyzer(lookup, fieldName).analyzer());
        }
    }
}
