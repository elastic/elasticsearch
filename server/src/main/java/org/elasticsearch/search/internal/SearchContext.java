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
package org.elasticsearch.search.internal;


import org.apache.lucene.search.Collector;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.Query;
import org.elasticsearch.action.search.SearchShardTask;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.AbstractRefCounted;
import org.elasticsearch.common.util.concurrent.RefCounted;
import org.elasticsearch.common.util.iterable.Iterables;
import org.elasticsearch.index.cache.bitset.BitsetFilterCache;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.ObjectMapper;
import org.elasticsearch.index.query.ParsedQuery;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.similarity.SimilarityService;
import org.elasticsearch.search.SearchExtBuilder;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.aggregations.SearchContextAggregations;
import org.elasticsearch.search.collapse.CollapseContext;
import org.elasticsearch.search.dfs.DfsSearchResult;
import org.elasticsearch.search.fetch.FetchPhase;
import org.elasticsearch.search.fetch.FetchSearchResult;
import org.elasticsearch.search.fetch.StoredFieldsContext;
import org.elasticsearch.search.fetch.subphase.FetchDocValuesContext;
import org.elasticsearch.search.fetch.subphase.FetchFieldsContext;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.fetch.subphase.InnerHitsContext;
import org.elasticsearch.search.fetch.subphase.ScriptFieldsContext;
import org.elasticsearch.search.fetch.subphase.highlight.SearchContextHighlight;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.search.profile.Profilers;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.search.rescore.RescoreContext;
import org.elasticsearch.search.sort.SortAndFormats;
import org.elasticsearch.search.suggest.SuggestionSearchContext;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This class encapsulates the state needed to execute a search. It holds a reference to the
 * shards point in time snapshot (IndexReader / ContextIndexSearcher) and allows passing on
 * state from one query / fetch phase to another.
 *
 * This class also implements {@link RefCounted} since in some situations like in {@link org.elasticsearch.search.SearchService}
 * a SearchContext can be closed concurrently due to independent events ie. when an index gets removed. To prevent accessing closed
 * IndexReader / IndexSearcher instances the SearchContext can be guarded by a reference count and fail if it's been closed by
 * an external event.
 */
// For reference why we use RefCounted here see #20095
public abstract class SearchContext extends AbstractRefCounted implements Releasable {

    public static final int DEFAULT_TERMINATE_AFTER = 0;
    public static final int TRACK_TOTAL_HITS_ACCURATE = Integer.MAX_VALUE;
    public static final int TRACK_TOTAL_HITS_DISABLED = -1;
    public static final int DEFAULT_TRACK_TOTAL_HITS_UP_TO = 10000;

    private Map<Lifetime, List<Releasable>> clearables = null;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private InnerHitsContext innerHitsContext;

    protected SearchContext() {
        super("search_context");
    }

    public abstract void setTask(SearchShardTask task);

    public abstract SearchShardTask getTask();

    public abstract boolean isCancelled();

    @Override
    public final void close() {
        if (closed.compareAndSet(false, true)) { // prevent double closing
            decRef();
        }
    }

    @Override
    protected final void closeInternal() {
        try {
            clearReleasables(Lifetime.CONTEXT);
        } finally {
            doClose();
        }
    }

    @Override
    protected void alreadyClosed() {
        throw new IllegalStateException("search context is already closed can't increment refCount current count [" + refCount() + "]");
    }

    protected abstract void doClose();

    /**
     * Should be called before executing the main query and after all other parameters have been set.
     * @param rewrite if the set query should be rewritten against the searcher returned from {@link #searcher()}
     */
    public abstract void preProcess(boolean rewrite);

    /** Automatically apply all required filters to the given query such as
     *  alias filters, types filters, etc. */
    public abstract Query buildFilteredQuery(Query query);

    public abstract SearchContextId id();

    public abstract String source();

    public abstract ShardSearchRequest request();

    public abstract SearchType searchType();

    public abstract SearchShardTarget shardTarget();

    public abstract int numberOfShards();

    public abstract float queryBoost();

    public abstract long getOriginNanoTime();

    public abstract ScrollContext scrollContext();

    public abstract SearchContext scrollContext(ScrollContext scroll);

    public abstract SearchContextAggregations aggregations();

    public abstract SearchContext aggregations(SearchContextAggregations aggregations);

    public abstract void addSearchExt(SearchExtBuilder searchExtBuilder);

    public abstract SearchExtBuilder getSearchExt(String name);

    public abstract SearchContextHighlight highlight();

    public abstract void highlight(SearchContextHighlight highlight);

    public InnerHitsContext innerHits() {
        if (innerHitsContext == null) {
            innerHitsContext = new InnerHitsContext();
        }
        return innerHitsContext;
    }

    public abstract SuggestionSearchContext suggest();

    public abstract void suggest(SuggestionSearchContext suggest);

    /**
     * @return list of all rescore contexts.  empty if there aren't any.
     */
    public abstract List<RescoreContext> rescore();

    public abstract void addRescore(RescoreContext rescore);

    public abstract boolean hasScriptFields();

    public abstract ScriptFieldsContext scriptFields();

    /**
     * A shortcut function to see whether there is a fetchSourceContext and it says the source is requested.
     */
    public abstract boolean sourceRequested();

    public abstract boolean hasFetchSourceContext();

    public abstract FetchSourceContext fetchSourceContext();

    public abstract SearchContext fetchSourceContext(FetchSourceContext fetchSourceContext);

    public abstract FetchDocValuesContext docValuesContext();

    public abstract SearchContext docValuesContext(FetchDocValuesContext docValuesContext);

    /**
     * The context related to retrieving fields.
     */
    public abstract FetchFieldsContext fetchFieldsContext();

    /**
     * Sets the context related to retrieving fields.
     */
    public abstract SearchContext fetchFieldsContext(FetchFieldsContext fetchFieldsContext);

    public abstract ContextIndexSearcher searcher();

    public abstract IndexShard indexShard();

    public abstract MapperService mapperService();

    public abstract SimilarityService similarityService();

    public abstract BigArrays bigArrays();

    public abstract BitsetFilterCache bitsetFilterCache();

    public abstract <IFD extends IndexFieldData<?>> IFD getForField(MappedFieldType fieldType);

    public abstract TimeValue timeout();

    public abstract void timeout(TimeValue timeout);

    public abstract int terminateAfter();

    public abstract void terminateAfter(int terminateAfter);

    /**
     * Indicates if the current index should perform frequent low level search cancellation check.
     *
     * Enabling low-level checks will make long running searches to react to the cancellation request faster. However,
     * since it will produce more cancellation checks it might slow the search performance down.
     */
    public abstract boolean lowLevelCancellation();

    public abstract SearchContext minimumScore(float minimumScore);

    public abstract Float minimumScore();

    public abstract SearchContext sort(SortAndFormats sort);

    public abstract SortAndFormats sort();

    public abstract SearchContext trackScores(boolean trackScores);

    public abstract boolean trackScores();

    public abstract SearchContext trackTotalHitsUpTo(int trackTotalHits);

    /**
     * Indicates the total number of hits to count accurately.
     * Defaults to {@link #DEFAULT_TRACK_TOTAL_HITS_UP_TO}.
     */
    public abstract int trackTotalHitsUpTo();

    public abstract SearchContext searchAfter(FieldDoc searchAfter);

    public abstract FieldDoc searchAfter();

    public abstract SearchContext collapse(CollapseContext collapse);

    public abstract CollapseContext collapse();

    public abstract SearchContext parsedPostFilter(ParsedQuery postFilter);

    public abstract ParsedQuery parsedPostFilter();

    public abstract Query aliasFilter();

    public abstract SearchContext parsedQuery(ParsedQuery query);

    public abstract ParsedQuery parsedQuery();

    /**
     * The query to execute, might be rewritten.
     */
    public abstract Query query();

    public abstract int from();

    public abstract SearchContext from(int from);

    public abstract int size();

    public abstract SearchContext size(int size);

    public abstract boolean hasStoredFields();

    public abstract boolean hasStoredFieldsContext();

    /**
     * A shortcut function to see whether there is a storedFieldsContext and it says the fields are requested.
     */
    public abstract boolean storedFieldsRequested();

    public abstract StoredFieldsContext storedFieldsContext();

    public abstract SearchContext storedFieldsContext(StoredFieldsContext storedFieldsContext);

    public abstract boolean explain();

    public abstract void explain(boolean explain);

    @Nullable
    public abstract List<String> groupStats();

    public abstract void groupStats(List<String> groupStats);

    public abstract boolean version();

    public abstract void version(boolean version);

    /** indicates whether the sequence number and primary term of the last modification to each hit should be returned */
    public abstract boolean seqNoAndPrimaryTerm();

    /** controls whether the sequence number and primary term of the last modification to each hit should be returned */
    public abstract void seqNoAndPrimaryTerm(boolean seqNoAndPrimaryTerm);

    public abstract int[] docIdsToLoad();

    public abstract int docIdsToLoadFrom();

    public abstract int docIdsToLoadSize();

    public abstract SearchContext docIdsToLoad(int[] docIdsToLoad, int docsIdsToLoadFrom, int docsIdsToLoadSize);

    public abstract void accessed(long accessTime);

    public abstract long lastAccessTime();

    public abstract long keepAlive();

    public abstract void keepAlive(long keepAlive);

    public SearchLookup lookup() {
        return getQueryShardContext().lookup();
    }

    public abstract DfsSearchResult dfsResult();

    public abstract QuerySearchResult queryResult();

    public abstract FetchPhase fetchPhase();

    public abstract FetchSearchResult fetchResult();

    /**
     * Return a handle over the profilers for the current search request, or {@code null} if profiling is not enabled.
     */
    public abstract Profilers getProfilers();

    /**
     * Schedule the release of a resource. The time when {@link Releasable#close()} will be called on this object
     * is function of the provided {@link Lifetime}.
     */
    public void addReleasable(Releasable releasable, Lifetime lifetime) {
        if (clearables == null) {
            clearables = new EnumMap<>(Lifetime.class);
        }
        List<Releasable> releasables = clearables.get(lifetime);
        if (releasables == null) {
            releasables = new ArrayList<>();
            clearables.put(lifetime, releasables);
        }
        releasables.add(releasable);
    }

    public void clearReleasables(Lifetime lifetime) {
        if (clearables != null) {
            List<List<Releasable>>releasables = new ArrayList<>();
            for (Lifetime lc : Lifetime.values()) {
                if (lc.compareTo(lifetime) > 0) {
                    break;
                }
                List<Releasable> remove = clearables.remove(lc);
                if (remove != null) {
                    releasables.add(remove);
                }
            }
            Releasables.close(Iterables.flatten(releasables));
        }
    }

    /**
     * @return true if the request contains only suggest
     */
    public final boolean hasOnlySuggest() {
        return request().source() != null
            && request().source().isSuggestOnly();
    }

    /**
     * Given the full name of a field, returns its {@link MappedFieldType}.
     */
    public abstract MappedFieldType fieldType(String name);

    public abstract ObjectMapper getObjectMapper(String name);

    /**
     * Returns time in milliseconds that can be used for relative time calculations.
     * WARN: This is not the epoch time.
     */
    public abstract long getRelativeTimeInMillis();

    /** Return a view of the additional query collectors that should be run for this context. */
    public abstract Map<Class<?>, Collector> queryCollectors();

    /**
     * The life time of an object that is used during search execution.
     */
    public enum Lifetime {
        /**
         * This life time is for objects that only live during collection time.
         */
        COLLECTION,
        /**
         * This life time is for objects that need to live until the end of the current search phase.
         */
        PHASE,
        /**
         * This life time is for objects that need to live until the search context they are attached to is destroyed.
         */
        CONTEXT
    }

    public abstract QueryShardContext getQueryShardContext();

    @Override
    public String toString() {
        StringBuilder result = new StringBuilder().append(shardTarget());
        if (searchType() != SearchType.DEFAULT) {
            result.append("searchType=[").append(searchType()).append("]");
        }
        if (scrollContext() != null) {
            if (scrollContext().scroll != null) {
                result.append("scroll=[").append(scrollContext().scroll.keepAlive()).append("]");
            } else {
                result.append("scroll=[null]");
            }
        }
        result.append(" query=[").append(query()).append("]");
        return result.toString();
    }
}
