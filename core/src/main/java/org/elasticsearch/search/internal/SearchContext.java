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
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.util.Counter;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.cache.recycler.PageCacheRecycler;
import org.elasticsearch.common.DelegatingHasContextAndHeaders;
import org.elasticsearch.common.HasContextAndHeaders;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.iterable.Iterables;
import org.elasticsearch.index.analysis.AnalysisService;
import org.elasticsearch.index.cache.bitset.BitsetFilterCache;
import org.elasticsearch.index.fielddata.IndexFieldDataService;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.object.ObjectMapper;
import org.elasticsearch.index.query.IndexQueryParserService;
import org.elasticsearch.index.query.ParsedQuery;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.similarity.SimilarityService;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.aggregations.SearchContextAggregations;
import org.elasticsearch.search.dfs.DfsSearchResult;
import org.elasticsearch.search.fetch.FetchSearchResult;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.fetch.FetchSubPhaseContext;
import org.elasticsearch.search.fetch.innerhits.InnerHitsContext;
import org.elasticsearch.search.fetch.script.ScriptFieldsContext;
import org.elasticsearch.search.fetch.source.FetchSourceContext;
import org.elasticsearch.search.highlight.SearchContextHighlight;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.search.rescore.RescoreSearchContext;
import org.elasticsearch.search.suggest.SuggestionSearchContext;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class SearchContext extends DelegatingHasContextAndHeaders implements Releasable {

    private static ThreadLocal<SearchContext> current = new ThreadLocal<>();
    public final static int DEFAULT_TERMINATE_AFTER = 0;

    public static void setCurrent(SearchContext value) {
        current.set(value);
        QueryParseContext.setTypes(value.types());
    }

    public static void removeCurrent() {
        current.remove();
        QueryParseContext.removeTypes();
    }

    public static SearchContext current() {
        return current.get();
    }

    private Map<Lifetime, List<Releasable>> clearables = null;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    protected final ParseFieldMatcher parseFieldMatcher;

    protected SearchContext(ParseFieldMatcher parseFieldMatcher, HasContextAndHeaders contextHeaders) {
        super(contextHeaders);
        this.parseFieldMatcher = parseFieldMatcher;
    }

    public ParseFieldMatcher parseFieldMatcher() {
        return parseFieldMatcher;
    }

    @Override
    public final void close() {
        if (closed.compareAndSet(false, true)) { // prevent double release
            try {
                clearReleasables(Lifetime.CONTEXT);
            } finally {
                doClose();
            }
        }
    }

    private boolean nowInMillisUsed;

    protected abstract void doClose();

    /**
     * Should be called before executing the main query and after all other parameters have been set.
     */
    public abstract void preProcess();

    public abstract Query searchFilter(String[] types);

    public abstract long id();

    public abstract String source();

    public abstract ShardSearchRequest request();

    public abstract SearchType searchType();

    public abstract SearchContext searchType(SearchType searchType);

    public abstract SearchShardTarget shardTarget();

    public abstract int numberOfShards();

    public abstract boolean hasTypes();

    public abstract String[] types();

    public abstract float queryBoost();

    public abstract SearchContext queryBoost(float queryBoost);

    public abstract long getOriginNanoTime();

    public final long nowInMillis() {
        nowInMillisUsed = true;
        return nowInMillisImpl();
    }

    public final boolean nowInMillisUsed() {
        return nowInMillisUsed;
    }

    protected abstract long nowInMillisImpl();

    public abstract ScrollContext scrollContext();

    public abstract SearchContext scrollContext(ScrollContext scroll);

    public abstract SearchContextAggregations aggregations();

    public abstract SearchContext aggregations(SearchContextAggregations aggregations);

    public abstract  <SubPhaseContext extends FetchSubPhaseContext> SubPhaseContext getFetchSubPhaseContext(FetchSubPhase.ContextFactory<SubPhaseContext> contextFactory);

    public abstract SearchContextHighlight highlight();

    public abstract void highlight(SearchContextHighlight highlight);

    public abstract void innerHits(InnerHitsContext innerHitsContext);

    public abstract InnerHitsContext innerHits();

    public abstract SuggestionSearchContext suggest();

    public abstract void suggest(SuggestionSearchContext suggest);

    /**
     * @return list of all rescore contexts.  empty if there aren't any.
     */
    public abstract List<RescoreSearchContext> rescore();

    public abstract void addRescore(RescoreSearchContext rescore);

    public abstract boolean hasScriptFields();

    public abstract ScriptFieldsContext scriptFields();

    /**
     * A shortcut function to see whether there is a fetchSourceContext and it says the source is requested.
     */
    public abstract boolean sourceRequested();

    public abstract boolean hasFetchSourceContext();

    public abstract FetchSourceContext fetchSourceContext();

    public abstract SearchContext fetchSourceContext(FetchSourceContext fetchSourceContext);

    public abstract ContextIndexSearcher searcher();

    public abstract IndexShard indexShard();

    public abstract MapperService mapperService();

    public abstract AnalysisService analysisService();

    public abstract IndexQueryParserService queryParserService();

    public abstract SimilarityService similarityService();

    public abstract ScriptService scriptService();

    public abstract PageCacheRecycler pageCacheRecycler();

    public abstract BigArrays bigArrays();

    public abstract BitsetFilterCache bitsetFilterCache();

    public abstract IndexFieldDataService fieldData();

    public abstract long timeoutInMillis();

    public abstract void timeoutInMillis(long timeoutInMillis);

    public abstract int terminateAfter();

    public abstract void terminateAfter(int terminateAfter);

    public abstract SearchContext minimumScore(float minimumScore);

    public abstract Float minimumScore();

    public abstract SearchContext sort(Sort sort);

    public abstract Sort sort();

    public abstract SearchContext trackScores(boolean trackScores);

    public abstract boolean trackScores();

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

    public abstract boolean hasFieldNames();

    public abstract List<String> fieldNames();

    public abstract void emptyFieldNames();

    public abstract boolean explain();

    public abstract void explain(boolean explain);

    @Nullable
    public abstract List<String> groupStats();

    public abstract void groupStats(List<String> groupStats);

    public abstract boolean version();

    public abstract void version(boolean version);

    public abstract int[] docIdsToLoad();

    public abstract int docIdsToLoadFrom();

    public abstract int docIdsToLoadSize();

    public abstract SearchContext docIdsToLoad(int[] docIdsToLoad, int docsIdsToLoadFrom, int docsIdsToLoadSize);

    public abstract void accessed(long accessTime);

    public abstract long lastAccessTime();

    public abstract long keepAlive();

    public abstract void keepAlive(long keepAlive);

    public abstract SearchLookup lookup();

    public abstract DfsSearchResult dfsResult();

    public abstract QuerySearchResult queryResult();

    public abstract FetchSearchResult fetchResult();

    /**
     * Schedule the release of a resource. The time when {@link Releasable#close()} will be called on this object
     * is function of the provided {@link Lifetime}.
     */
    public void addReleasable(Releasable releasable, Lifetime lifetime) {
        if (clearables == null) {
            clearables = new HashMap<>();
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

    public abstract MappedFieldType smartNameFieldType(String name);

    /**
     * Looks up the given field, but does not restrict to fields in the types set on this context.
     */
    public abstract MappedFieldType smartNameFieldTypeFromAnyType(String name);

    public abstract ObjectMapper getObjectMapper(String name);

    public abstract Counter timeEstimateCounter();

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
}
