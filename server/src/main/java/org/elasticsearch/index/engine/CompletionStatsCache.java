/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.index.engine;

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Terms;
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.search.suggest.document.CompletionTerms;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.FieldMemoryStats;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.search.suggest.completion.CompletionStats;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

public class CompletionStatsCache implements ReferenceManager.RefreshListener {

    private final Supplier<Engine.Searcher> searcherSupplier;

    /**
     * Contains a future (i.e. non-null) if another thread is already computing stats, in which case wait for this computation to
     * complete. Contains null otherwise, in which case compute the stats ourselves and save them here for other threads to use.
     * Futures are eventually completed with stats that include all fields, requiring further filtering (see
     * {@link CompletionStatsCache#filterCompletionStatsByFieldName}).
     */
    private final AtomicReference<PlainActionFuture<CompletionStats>> completionStatsFutureRef = new AtomicReference<>();

    public CompletionStatsCache(Supplier<Engine.Searcher> searcherSupplier) {
        this.searcherSupplier = searcherSupplier;
    }

    public CompletionStats get(String... fieldNamePatterns) {
        final PlainActionFuture<CompletionStats> newFuture = new PlainActionFuture<>();
        final PlainActionFuture<CompletionStats> oldFuture = completionStatsFutureRef.compareAndExchange(null, newFuture);

        if (oldFuture != null) {
            // we lost the race, someone else is already computing stats, so we wait for that to finish
            return filterCompletionStatsByFieldName(fieldNamePatterns, oldFuture.actionGet());
        }

        // we won the race, nobody else is already computing stats, so it's up to us
        ActionListener.completeWith(newFuture, () -> {
            long sizeInBytes = 0;
            final Map<String, Long> completionFields = new HashMap<>();

            try (Engine.Searcher currentSearcher = searcherSupplier.get()) {
                for (LeafReaderContext atomicReaderContext : currentSearcher.getIndexReader().leaves()) {
                    LeafReader atomicReader = atomicReaderContext.reader();
                    for (FieldInfo info : atomicReader.getFieldInfos()) {
                        Terms terms = atomicReader.terms(info.name);
                        if (terms instanceof CompletionTerms) {
                            // TODO: currently we load up the suggester for reporting its size
                            final long fstSize = ((CompletionTerms) terms).suggester().ramBytesUsed();
                            completionFields.merge(info.name, fstSize, Long::sum);
                            sizeInBytes += fstSize;
                        }
                    }
                }
            }

            return new CompletionStats(sizeInBytes, new FieldMemoryStats(completionFields));
        });

        boolean success = false;
        final CompletionStats completionStats;
        try {
            completionStats = newFuture.actionGet();
            success = true;
        } finally {
            if (success == false) {
                // invalidate the cache (if not already invalidated) so that future calls will retry
                completionStatsFutureRef.compareAndSet(newFuture, null);
            }
        }

        return filterCompletionStatsByFieldName(fieldNamePatterns, completionStats);
    }

    private static CompletionStats filterCompletionStatsByFieldName(String[] fieldNamePatterns, CompletionStats fullCompletionStats) {
        final FieldMemoryStats fieldMemoryStats;
        if (CollectionUtils.isEmpty(fieldNamePatterns) == false) {
            final Map<String, Long> completionFields = new HashMap<>(fieldNamePatterns.length);
            for (var field : fullCompletionStats.getFields()) {
                if (Regex.simpleMatch(fieldNamePatterns, field.getKey())) {
                    completionFields.merge(field.getKey(), field.getValue(), Long::sum);
                }
            }
            fieldMemoryStats = new FieldMemoryStats(completionFields);
        } else {
            fieldMemoryStats = null;
        }
        return new CompletionStats(fullCompletionStats.getSizeInBytes(), fieldMemoryStats);
    }

    @Override
    public void beforeRefresh() {}

    @Override
    public void afterRefresh(boolean didRefresh) {
        if (didRefresh) {
            completionStatsFutureRef.set(null);
        }
    }
}
