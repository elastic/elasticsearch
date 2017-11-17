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

package org.elasticsearch.search.query;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.EarlyTerminatingSortingCollector;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MultiCollector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHitCountCollector;
import org.apache.lucene.search.Weight;
import org.elasticsearch.common.lucene.MinimumScoreCollector;
import org.elasticsearch.common.lucene.search.FilteredCollector;
import org.elasticsearch.search.profile.query.InternalProfileCollector;
import org.elasticsearch.tasks.TaskCancelledException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.BooleanSupplier;
import java.util.function.IntSupplier;

import static org.elasticsearch.search.profile.query.CollectorResult.REASON_SEARCH_CANCELLED;
import static org.elasticsearch.search.profile.query.CollectorResult.REASON_SEARCH_MIN_SCORE;
import static org.elasticsearch.search.profile.query.CollectorResult.REASON_SEARCH_MULTI;
import static org.elasticsearch.search.profile.query.CollectorResult.REASON_SEARCH_POST_FILTER;
import static org.elasticsearch.search.profile.query.CollectorResult.REASON_SEARCH_TERMINATE_AFTER_COUNT;
import static org.elasticsearch.search.query.TopDocsCollectorContext.shortcutTotalHitCount;

abstract class QueryCollectorContext {
    private String profilerName;

    QueryCollectorContext(String profilerName) {
        this.profilerName = profilerName;
    }

    /**
     * Creates a collector that delegates documents to the provided <code>in</code> collector.
     * @param in The delegate collector
     */
    abstract Collector create(Collector in) throws IOException;

    /**
     * Wraps this collector with a profiler
     */
    protected InternalProfileCollector createWithProfiler(InternalProfileCollector in) throws IOException {
        final Collector collector = create(in);
        return new InternalProfileCollector(collector, profilerName, in != null ? Collections.singletonList(in) : Collections.emptyList());
    }

    /**
     * A value of <code>false</code> indicates that the underlying collector can infer
     * its results directly from the context (search is not needed).
     * Default to true (search is needed).
     */
    boolean shouldCollect() {
        return true;
    }

    /**
     * Post-process <code>result</code> after search execution.
     *
     * @param result The query search result to populate
     * @param hasCollected True if search was executed
     */
    void postProcess(QuerySearchResult result, boolean hasCollected) throws IOException {}

    /**
     * Creates the collector tree from the provided <code>collectors</code>
     * @param collectors Ordered list of collector context
     */
    static Collector createQueryCollector(List<QueryCollectorContext> collectors) throws IOException {
        Collector collector = null;
        for (QueryCollectorContext ctx : collectors) {
            collector = ctx.create(collector);
        }
        return collector;
    }

    /**
     * Creates the collector tree from the provided <code>collectors</code> and wraps each collector with a profiler
     * @param collectors Ordered list of collector context
     */
    static InternalProfileCollector createQueryCollectorWithProfiler(List<QueryCollectorContext> collectors) throws IOException {
        InternalProfileCollector collector = null;
        for (QueryCollectorContext ctx : collectors) {
            collector = ctx.createWithProfiler(collector);
        }
        return collector;
    }

    /**
     * Filters documents with a query score greater than <code>minScore</code>
     * @param minScore The minimum score filter
     */
    static QueryCollectorContext createMinScoreCollectorContext(float minScore) {
        return new QueryCollectorContext(REASON_SEARCH_MIN_SCORE) {
            @Override
            Collector create(Collector in) {
                return new MinimumScoreCollector(in, minScore);
            }
        };
    }

    /**
     * Filters documents based on the provided <code>query</code>
     */
    static QueryCollectorContext createFilteredCollectorContext(IndexSearcher searcher, Query query) {
        return new QueryCollectorContext(REASON_SEARCH_POST_FILTER) {
            @Override
            Collector create(Collector in ) throws IOException {
                final Weight filterWeight = searcher.createNormalizedWeight(query, false);
                return new FilteredCollector(in, filterWeight);
            }
        };
    }

    /**
     * Creates a multi collector from the provided <code>subs</code>
     */
    static QueryCollectorContext createMultiCollectorContext(Collection<Collector> subs) {
        return new QueryCollectorContext(REASON_SEARCH_MULTI) {
            @Override
            Collector create(Collector in) throws IOException {
                List<Collector> subCollectors = new ArrayList<> ();
                subCollectors.add(in);
                subCollectors.addAll(subs);
                return MultiCollector.wrap(subCollectors);
            }

            @Override
            protected InternalProfileCollector createWithProfiler(InternalProfileCollector in) throws IOException {
                final List<InternalProfileCollector> subCollectors = new ArrayList<> ();
                subCollectors.add(in);
                if (subs.stream().anyMatch((col) -> col instanceof InternalProfileCollector == false)) {
                    throw new IllegalArgumentException("non-profiling collector");
                }
                for (Collector collector : subs) {
                    subCollectors.add((InternalProfileCollector) collector);
                }
                final Collector collector = MultiCollector.wrap(subCollectors);
                return new InternalProfileCollector(collector, REASON_SEARCH_MULTI, subCollectors);
            }
        };
    }

    /**
     * Creates a collector that throws {@link TaskCancelledException} if the search is cancelled
     */
    static QueryCollectorContext createCancellableCollectorContext(BooleanSupplier cancelled) {
        return new QueryCollectorContext(REASON_SEARCH_CANCELLED) {
            @Override
            Collector create(Collector in) throws IOException {
                return new CancellableCollector(cancelled, in);
            }

            @Override
            boolean shouldCollect() {
                return false;
            }
        };
    }

    /**
     * Creates collector limiting the collection to the first <code>numHits</code> documents
     */
    static QueryCollectorContext createEarlyTerminationCollectorContext(int numHits) {
        return new QueryCollectorContext(REASON_SEARCH_TERMINATE_AFTER_COUNT) {
            private EarlyTerminatingCollector collector;

            @Override
            Collector create(Collector in) throws IOException {
                assert collector == null;
                this.collector = new EarlyTerminatingCollector(in, numHits);
                return collector;
            }

            @Override
            void postProcess(QuerySearchResult result, boolean hasCollected) throws IOException {
                if (hasCollected && collector.terminatedEarly()) {
                    result.terminatedEarly(true);
                }
            }
        };
    }

    /**
     * Creates a sorting termination collector limiting the collection to the first <code>numHits</code> per segment.
     * The total hit count matching the query is also computed if <code>trackTotalHits</code> is true.
     */
    static QueryCollectorContext createEarlySortingTerminationCollectorContext(IndexReader reader,
                                                                               Query query,
                                                                               Sort indexSort,
                                                                               int numHits,
                                                                               boolean trackTotalHits,
                                                                               boolean shouldCollect) {
        return new QueryCollectorContext(REASON_SEARCH_TERMINATE_AFTER_COUNT) {
            private IntSupplier countSupplier = null;

            @Override
            Collector create(Collector in) throws IOException {
                EarlyTerminatingSortingCollector sortingCollector = new EarlyTerminatingSortingCollector(in, indexSort, numHits);
                Collector collector = sortingCollector;
                if (trackTotalHits) {
                    int count = shouldCollect ? -1 : shortcutTotalHitCount(reader, query);
                    if (count == -1) {
                        TotalHitCountCollector countCollector = new TotalHitCountCollector();
                        collector = MultiCollector.wrap(sortingCollector, countCollector);
                        this.countSupplier = countCollector::getTotalHits;
                    } else {
                        this.countSupplier = () -> count;
                    }
                }
                return collector;
            }

            @Override
            void postProcess(QuerySearchResult result, boolean hasCollected) throws IOException {
                if (countSupplier != null) {
                    final TopDocs topDocs = result.topDocs();
                    topDocs.totalHits = countSupplier.getAsInt();
                    result.topDocs(topDocs, result.sortValueFormats());
                }
            }
        };
    }
}
