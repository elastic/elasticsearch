/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.index.engine;

import com.carrotsearch.hppc.ObjectLongHashMap;
import com.carrotsearch.hppc.cursors.ObjectLongCursor;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Terms;
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.search.suggest.document.CompletionTerms;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.FieldMemoryStats;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.search.suggest.completion.CompletionStats;

import java.util.function.Supplier;

class CompletionStatsCache implements ReferenceManager.RefreshListener {

    private final Supplier<Engine.Searcher> searcherSupplier;

    /**
     * Contains a future (i.e. non-null) if another thread is already computing stats, in which case wait for this computation to
     * complete. Contains null otherwise, in which case compute the stats ourselves and save them here for other threads to use.
     * Futures are eventually completed with stats that include all fields, requiring further filtering (see
     * {@link CompletionStatsCache#filterCompletionStatsByFieldName}).
     */
    @Nullable
    private PlainActionFuture<CompletionStats> completionStatsFuture;

    /**
     * Protects accesses to {@code completionStatsFuture} since we can't use {@link java.util.concurrent.atomic.AtomicReference} in JDK8.
     */
    private final Object completionStatsFutureMutex = new Object();

    CompletionStatsCache(Supplier<Engine.Searcher> searcherSupplier) {
        this.searcherSupplier = searcherSupplier;
    }

    CompletionStats get(String... fieldNamePatterns) {
        final PlainActionFuture<CompletionStats> newFuture = new PlainActionFuture<>();

        // final PlainActionFuture<CompletionStats> oldFuture = completionStatsFutureRef.compareAndExchange(null, newFuture);
        // except JDK8 doesn't have compareAndExchange so we emulate it:
        final PlainActionFuture<CompletionStats> oldFuture;
        synchronized (completionStatsFutureMutex) {
            if (completionStatsFuture == null) {
                completionStatsFuture = newFuture;
                oldFuture = null;
            } else {
                oldFuture = completionStatsFuture;
            }
        }

        if (oldFuture != null) {
            // we lost the race, someone else is already computing stats, so we wait for that to finish
            return filterCompletionStatsByFieldName(fieldNamePatterns, oldFuture.actionGet());
        }

        // we won the race, nobody else is already computing stats, so it's up to us
        ActionListener.completeWith(newFuture, () -> {
            long sizeInBytes = 0;
            final ObjectLongHashMap<String> completionFields = new ObjectLongHashMap<>();

            try (Engine.Searcher currentSearcher = searcherSupplier.get()) {
                for (LeafReaderContext atomicReaderContext : currentSearcher.getIndexReader().leaves()) {
                    LeafReader atomicReader = atomicReaderContext.reader();
                    for (FieldInfo info : atomicReader.getFieldInfos()) {
                        Terms terms = atomicReader.terms(info.name);
                        if (terms instanceof CompletionTerms) {
                            // TODO: currently we load up the suggester for reporting its size
                            final long fstSize = ((CompletionTerms) terms).suggester().ramBytesUsed();
                            completionFields.addTo(info.name, fstSize);
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

                // completionStatsFutureRef.compareAndSet(newFuture, null); except we're not using AtomicReference in JDK8
                synchronized (completionStatsFutureMutex) {
                    if (completionStatsFuture == newFuture) {
                        completionStatsFuture = null;
                    }
                }
            }
        }

        return filterCompletionStatsByFieldName(fieldNamePatterns, completionStats);
    }

    private static CompletionStats filterCompletionStatsByFieldName(String[] fieldNamePatterns, CompletionStats fullCompletionStats) {
        final FieldMemoryStats fieldMemoryStats;
        if (fieldNamePatterns != null && fieldNamePatterns.length > 0) {
            final ObjectLongHashMap<String> completionFields = new ObjectLongHashMap<>(fieldNamePatterns.length);
            for (ObjectLongCursor<String> fieldCursor : fullCompletionStats.getFields()) {
                if (Regex.simpleMatch(fieldNamePatterns, fieldCursor.key)) {
                    completionFields.addTo(fieldCursor.key, fieldCursor.value);
                }
            }
            fieldMemoryStats = new FieldMemoryStats(completionFields);
        } else {
            fieldMemoryStats = null;
        }
        return new CompletionStats(fullCompletionStats.getSizeInBytes(), fieldMemoryStats);
    }

    @Override
    public void beforeRefresh() {
    }

    @Override
    public void afterRefresh(boolean didRefresh) {
        if (didRefresh) {
            // completionStatsFutureRef.set(null); except we're not using AtomicReference in JDK8
            synchronized (completionStatsFutureMutex) {
                completionStatsFuture = null;
            }
        }
    }
}
