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

package org.elasticsearch.search;

import com.carrotsearch.hppc.ObjectOpenHashSet;
import com.carrotsearch.hppc.ObjectSet;
import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.TopDocs;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.cache.recycler.PageCacheRecycler;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.ConcurrentMapLong;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.fielddata.FieldDataType;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldDataService;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.FieldMapper.Loading;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.TemplateQueryParser;
import org.elasticsearch.index.search.stats.StatsGroupsParseElement;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesLifecycle;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.IndicesWarmer;
import org.elasticsearch.indices.IndicesWarmer.TerminationHandle;
import org.elasticsearch.indices.IndicesWarmer.WarmerContext;
import org.elasticsearch.indices.cache.query.IndicesQueryCache;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.mustache.MustacheScriptEngineService;
import org.elasticsearch.search.dfs.CachedDfSource;
import org.elasticsearch.search.dfs.DfsPhase;
import org.elasticsearch.search.dfs.DfsSearchResult;
import org.elasticsearch.search.fetch.*;
import org.elasticsearch.search.internal.*;
import org.elasticsearch.search.internal.SearchContext.Lifetime;
import org.elasticsearch.search.query.*;
import org.elasticsearch.search.warmer.IndexWarmersMetaData;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.common.Strings.hasLength;
import static org.elasticsearch.common.unit.TimeValue.timeValueMinutes;

/**
 *
 */
public class SearchService extends AbstractLifecycleComponent<SearchService> {

    public static final String NORMS_LOADING_KEY = "index.norms.loading";
    public static final String DEFAULT_KEEPALIVE_KEY = "search.default_keep_alive";
    public static final String KEEPALIVE_INTERVAL_KEY = "search.keep_alive_interval";


    private final ThreadPool threadPool;

    private final ClusterService clusterService;

    private final IndicesService indicesService;

    private final IndicesWarmer indicesWarmer;

    private final ScriptService scriptService;

    private final PageCacheRecycler pageCacheRecycler;

    private final BigArrays bigArrays;

    private final DfsPhase dfsPhase;

    private final QueryPhase queryPhase;

    private final FetchPhase fetchPhase;

    private final IndicesQueryCache indicesQueryCache;

    private final long defaultKeepAlive;

    private final ScheduledFuture<?> keepAliveReaper;

    private final AtomicLong idGenerator = new AtomicLong();

    private final ConcurrentMapLong<SearchContext> activeContexts = ConcurrentCollections.newConcurrentMapLongWithAggressiveConcurrency();

    private final ImmutableMap<String, SearchParseElement> elementParsers;

    @Inject
    public SearchService(Settings settings, ClusterService clusterService, IndicesService indicesService,IndicesWarmer indicesWarmer, ThreadPool threadPool,
                         ScriptService scriptService, PageCacheRecycler pageCacheRecycler, BigArrays bigArrays, DfsPhase dfsPhase, QueryPhase queryPhase, FetchPhase fetchPhase,
                         IndicesQueryCache indicesQueryCache) {
        super(settings);
        this.threadPool = threadPool;
        this.clusterService = clusterService;
        this.indicesService = indicesService;
        indicesService.indicesLifecycle().addListener(new IndicesLifecycle.Listener() {

            @Override
            public void afterIndexDeleted(Index index, @IndexSettings Settings indexSettings) {
                // once an index is closed we can just clean up all the pending search context information
                // to release memory and let references to the filesystem go etc.
                freeAllContextForIndex(index);
            }
        });
        this.indicesWarmer = indicesWarmer;
        this.scriptService = scriptService;
        this.pageCacheRecycler = pageCacheRecycler;
        this.bigArrays = bigArrays;
        this.dfsPhase = dfsPhase;
        this.queryPhase = queryPhase;
        this.fetchPhase = fetchPhase;
        this.indicesQueryCache = indicesQueryCache;

        TimeValue keepAliveInterval = settings.getAsTime(KEEPALIVE_INTERVAL_KEY, timeValueMinutes(1));
        // we can have 5 minutes here, since we make sure to clean with search requests and when shard/index closes
        this.defaultKeepAlive = settings.getAsTime(DEFAULT_KEEPALIVE_KEY, timeValueMinutes(5)).millis();

        Map<String, SearchParseElement> elementParsers = new HashMap<>();
        elementParsers.putAll(dfsPhase.parseElements());
        elementParsers.putAll(queryPhase.parseElements());
        elementParsers.putAll(fetchPhase.parseElements());
        elementParsers.put("stats", new StatsGroupsParseElement());
        this.elementParsers = ImmutableMap.copyOf(elementParsers);

        this.keepAliveReaper = threadPool.scheduleWithFixedDelay(new Reaper(), keepAliveInterval);

        this.indicesWarmer.addListener(new NormsWarmer());
        this.indicesWarmer.addListener(new FieldDataWarmer());
        this.indicesWarmer.addListener(new SearchWarmer());
    }

    protected void putContext(SearchContext context) {
        final SearchContext previous = activeContexts.put(context.id(), context);
        assert previous == null;
    }

    protected SearchContext removeContext(long id) {
        return activeContexts.remove(id);
    }

    @Override
    protected void doStart() throws ElasticsearchException {
    }

    @Override
    protected void doStop() throws ElasticsearchException {
        for (final SearchContext context : activeContexts.values()) {
            freeContext(context.id());
        }
    }

    @Override
    protected void doClose() throws ElasticsearchException {
        doStop();
        FutureUtils.cancel(keepAliveReaper);
    }

    public DfsSearchResult executeDfsPhase(ShardSearchRequest request) throws ElasticsearchException {
        final SearchContext context = createAndPutContext(request);
        try {
            contextProcessing(context);
            dfsPhase.execute(context);
            contextProcessedSuccessfully(context);
            return context.dfsResult();
        } catch (Throwable e) {
            logger.trace("Dfs phase failed", e);
            freeContext(context.id());
            throw ExceptionsHelper.convertToRuntime(e);
        } finally {
            cleanContext(context);
        }
    }

    public QuerySearchResult executeScan(ShardSearchRequest request) throws ElasticsearchException {
        final SearchContext context = createAndPutContext(request);
        final int originalSize = context.size();
        try {
            if (context.aggregations() != null) {
                throw new ElasticsearchIllegalArgumentException("aggregations are not supported with search_type=scan");
            }

            if (context.scroll() == null) {
                throw new ElasticsearchException("Scroll must be provided when scanning...");
            }

            assert context.searchType() == SearchType.SCAN;
            context.searchType(SearchType.QUERY_THEN_FETCH); // move to QUERY_THEN_FETCH, and then, when scrolling, move to SCAN
            context.size(0); // set size to 0 so that we only count matches
            assert context.searchType() == SearchType.QUERY_THEN_FETCH;

            contextProcessing(context);
            queryPhase.execute(context);
            contextProcessedSuccessfully(context);
            return context.queryResult();
        } catch (Throwable e) {
            logger.trace("Scan phase failed", e);
            freeContext(context.id());
            throw ExceptionsHelper.convertToRuntime(e);
        } finally {
            context.size(originalSize);
            cleanContext(context);
        }
    }

    public ScrollQueryFetchSearchResult executeScan(InternalScrollSearchRequest request) throws ElasticsearchException {
        final SearchContext context = findContext(request.id());
        contextProcessing(context);
        try {
            processScroll(request, context);
            if (context.searchType() == SearchType.QUERY_THEN_FETCH) {
                // first scanning, reset the from to 0
                context.searchType(SearchType.SCAN);
                context.from(0);
            }
            queryPhase.execute(context);
            shortcutDocIdsToLoadForScanning(context);
            fetchPhase.execute(context);
            if (context.scroll() == null || context.fetchResult().hits().hits().length < context.size()) {
                freeContext(request.id());
            } else {
                contextProcessedSuccessfully(context);
            }
            return new ScrollQueryFetchSearchResult(new QueryFetchSearchResult(context.queryResult(), context.fetchResult()), context.shardTarget());
        } catch (Throwable e) {
            logger.trace("Scan phase failed", e);
            freeContext(context.id());
            throw ExceptionsHelper.convertToRuntime(e);
        } finally {
            cleanContext(context);
        }
    }

    /**
     * Try to load the query results from the cache or execute the query phase directly if the cache cannot be used.
     */
    private void loadOrExecuteQueryPhase(final ShardSearchRequest request, final SearchContext context,
            final QueryPhase queryPhase) throws Exception {
        final boolean canCache = indicesQueryCache.canCache(request, context);
        if (canCache) {
            indicesQueryCache.loadIntoContext(request, context, queryPhase);
        } else {
            queryPhase.execute(context);
        }
    }

    public QuerySearchResultProvider executeQueryPhase(ShardSearchRequest request) throws ElasticsearchException {
        final SearchContext context = createAndPutContext(request);
        try {
            context.indexShard().searchService().onPreQueryPhase(context);
            long time = System.nanoTime();
            contextProcessing(context);

            loadOrExecuteQueryPhase(request, context, queryPhase);

            if (context.queryResult().topDocs().scoreDocs.length == 0 && context.scroll() == null) {
                freeContext(context.id());
            } else {
                contextProcessedSuccessfully(context);
            }
            context.indexShard().searchService().onQueryPhase(context, System.nanoTime() - time);

            return context.queryResult();
        } catch (Throwable e) {
            // execution exception can happen while loading the cache, strip it
            if (e instanceof ExecutionException) {
                e = e.getCause();
            }
            context.indexShard().searchService().onFailedQueryPhase(context);
            logger.trace("Query phase failed", e);
            freeContext(context.id());
            throw ExceptionsHelper.convertToRuntime(e);
        } finally {
            cleanContext(context);
        }
    }

    public ScrollQuerySearchResult executeQueryPhase(InternalScrollSearchRequest request) throws ElasticsearchException {
        final SearchContext context = findContext(request.id());
        try {
            context.indexShard().searchService().onPreQueryPhase(context);
            long time = System.nanoTime();
            contextProcessing(context);
            processScroll(request, context);
            queryPhase.execute(context);
            contextProcessedSuccessfully(context);
            context.indexShard().searchService().onQueryPhase(context, System.nanoTime() - time);
            return new ScrollQuerySearchResult(context.queryResult(), context.shardTarget());
        } catch (Throwable e) {
            context.indexShard().searchService().onFailedQueryPhase(context);
            logger.trace("Query phase failed", e);
            freeContext(context.id());
            throw ExceptionsHelper.convertToRuntime(e);
        } finally {
            cleanContext(context);
        }
    }

    public QuerySearchResult executeQueryPhase(QuerySearchRequest request) throws ElasticsearchException {
        final SearchContext context = findContext(request.id());
        contextProcessing(context);
        try {
            context.searcher().dfSource(new CachedDfSource(context.searcher().getIndexReader(), request.dfs(), context.similarityService().similarity()));
        } catch (Throwable e) {
            freeContext(context.id());
            cleanContext(context);
            throw new QueryPhaseExecutionException(context, "Failed to set aggregated df", e);
        }
        try {
            context.indexShard().searchService().onPreQueryPhase(context);
            long time = System.nanoTime();
            queryPhase.execute(context);
            if (context.queryResult().topDocs().scoreDocs.length == 0 && context.scroll() == null) {
                // no hits, we can release the context since there will be no fetch phase
                freeContext(context.id());
            } else {
                contextProcessedSuccessfully(context);
            }
            context.indexShard().searchService().onQueryPhase(context, System.nanoTime() - time);
            return context.queryResult();
        } catch (Throwable e) {
            context.indexShard().searchService().onFailedQueryPhase(context);
            logger.trace("Query phase failed", e);
            freeContext(context.id());
            throw ExceptionsHelper.convertToRuntime(e);
        } finally {
            cleanContext(context);
        }
    }

    public QueryFetchSearchResult executeFetchPhase(ShardSearchRequest request) throws ElasticsearchException {
        final SearchContext context = createAndPutContext(request);
        contextProcessing(context);
        try {
            context.indexShard().searchService().onPreQueryPhase(context);
            long time = System.nanoTime();
            try {
                loadOrExecuteQueryPhase(request, context, queryPhase);
            } catch (Throwable e) {
                context.indexShard().searchService().onFailedQueryPhase(context);
                throw ExceptionsHelper.convertToRuntime(e);
            }
            long time2 = System.nanoTime();
            context.indexShard().searchService().onQueryPhase(context, time2 - time);
            context.indexShard().searchService().onPreFetchPhase(context);
            try {
                shortcutDocIdsToLoad(context);
                fetchPhase.execute(context);
                if (context.scroll() == null) {
                    freeContext(context.id());
                } else {
                    contextProcessedSuccessfully(context);
                }
            } catch (Throwable e) {
                context.indexShard().searchService().onFailedFetchPhase(context);
                throw ExceptionsHelper.convertToRuntime(e);
            }
            context.indexShard().searchService().onFetchPhase(context, System.nanoTime() - time2);
            return new QueryFetchSearchResult(context.queryResult(), context.fetchResult());
        } catch (Throwable e) {
            logger.trace("Fetch phase failed", e);
            freeContext(context.id());
            throw ExceptionsHelper.convertToRuntime(e);
        } finally {
            cleanContext(context);
        }
    }

    public QueryFetchSearchResult executeFetchPhase(QuerySearchRequest request) throws ElasticsearchException {
        final SearchContext context = findContext(request.id());
        contextProcessing(context);
        try {
            context.searcher().dfSource(new CachedDfSource(context.searcher().getIndexReader(), request.dfs(), context.similarityService().similarity()));
        } catch (Throwable e) {
            freeContext(context.id());
            cleanContext(context);
            throw new QueryPhaseExecutionException(context, "Failed to set aggregated df", e);
        }
        try {
            context.indexShard().searchService().onPreQueryPhase(context);
            long time = System.nanoTime();
            try {
                queryPhase.execute(context);
            } catch (Throwable e) {
                context.indexShard().searchService().onFailedQueryPhase(context);
                throw ExceptionsHelper.convertToRuntime(e);
            }
            long time2 = System.nanoTime();
            context.indexShard().searchService().onQueryPhase(context, time2 - time);
            context.indexShard().searchService().onPreFetchPhase(context);
            try {
                shortcutDocIdsToLoad(context);
                fetchPhase.execute(context);
                if (context.scroll() == null) {
                    freeContext(request.id());
                } else {
                    contextProcessedSuccessfully(context);
                }
            } catch (Throwable e) {
                context.indexShard().searchService().onFailedFetchPhase(context);
                throw ExceptionsHelper.convertToRuntime(e);
            }
            context.indexShard().searchService().onFetchPhase(context, System.nanoTime() - time2);
            return new QueryFetchSearchResult(context.queryResult(), context.fetchResult());
        } catch (Throwable e) {
            logger.trace("Fetch phase failed", e);
            freeContext(context.id());
            throw ExceptionsHelper.convertToRuntime(e);
        } finally {
            cleanContext(context);
        }
    }

    public ScrollQueryFetchSearchResult executeFetchPhase(InternalScrollSearchRequest request) throws ElasticsearchException {
        final SearchContext context = findContext(request.id());
        contextProcessing(context);
        try {
            processScroll(request, context);
            context.indexShard().searchService().onPreQueryPhase(context);
            long time = System.nanoTime();
            try {
                queryPhase.execute(context);
            } catch (Throwable e) {
                context.indexShard().searchService().onFailedQueryPhase(context);
                throw ExceptionsHelper.convertToRuntime(e);
            }
            long time2 = System.nanoTime();
            context.indexShard().searchService().onQueryPhase(context, time2 - time);
            context.indexShard().searchService().onPreFetchPhase(context);
            try {
                shortcutDocIdsToLoad(context);
                fetchPhase.execute(context);
                if (context.scroll() == null) {
                    freeContext(request.id());
                } else {
                    contextProcessedSuccessfully(context);
                }
            } catch (Throwable e) {
                context.indexShard().searchService().onFailedFetchPhase(context);
                throw ExceptionsHelper.convertToRuntime(e);
            }
            context.indexShard().searchService().onFetchPhase(context, System.nanoTime() - time2);
            return new ScrollQueryFetchSearchResult(new QueryFetchSearchResult(context.queryResult(), context.fetchResult()), context.shardTarget());
        } catch (Throwable e) {
            logger.trace("Fetch phase failed", e);
            freeContext(context.id());
            throw ExceptionsHelper.convertToRuntime(e);
        } finally {
            cleanContext(context);
        }
    }

    public FetchSearchResult executeFetchPhase(ShardFetchRequest request) throws ElasticsearchException {
        final SearchContext context = findContext(request.id());
        contextProcessing(context);
        try {
            if (request.lastEmittedDoc() != null) {
                context.lastEmittedDoc(request.lastEmittedDoc());
            }
            context.docIdsToLoad(request.docIds(), 0, request.docIdsSize());
            context.indexShard().searchService().onPreFetchPhase(context);
            long time = System.nanoTime();
            fetchPhase.execute(context);
            if (context.scroll() == null) {
                freeContext(request.id());
            } else {
                contextProcessedSuccessfully(context);
            }
            context.indexShard().searchService().onFetchPhase(context, System.nanoTime() - time);
            return context.fetchResult();
        } catch (Throwable e) {
            context.indexShard().searchService().onFailedFetchPhase(context);
            logger.trace("Fetch phase failed", e);
            freeContext(context.id()); // we just try to make sure this is freed - rethrow orig exception.
            throw ExceptionsHelper.convertToRuntime(e);
        } finally {
            cleanContext(context);
        }
    }

    private SearchContext findContext(long id) throws SearchContextMissingException {
        SearchContext context = activeContexts.get(id);
        if (context == null) {
            throw new SearchContextMissingException(id);
        }
        SearchContext.setCurrent(context);
        return context;
    }

    final SearchContext createAndPutContext(ShardSearchRequest request) throws ElasticsearchException {
        SearchContext context = createContext(request, null);
        boolean success = false;
        try {
            putContext(context);
            context.indexShard().searchService().onNewContext(context);
            success = true;
            return context;
        } finally {
            if (!success) {
                freeContext(context.id());
            }
        }
    }

    final SearchContext createContext(ShardSearchRequest request, @Nullable Engine.Searcher searcher) throws ElasticsearchException {
        IndexService indexService = indicesService.indexServiceSafe(request.index());
        IndexShard indexShard = indexService.shardSafe(request.shardId());

        SearchShardTarget shardTarget = new SearchShardTarget(clusterService.localNode().id(), request.index(), request.shardId());

        Engine.Searcher engineSearcher = searcher == null ? indexShard.acquireSearcher("search") : searcher;
        SearchContext context = new DefaultSearchContext(idGenerator.incrementAndGet(), request, shardTarget, engineSearcher, indexService, indexShard, scriptService, pageCacheRecycler, bigArrays, threadPool.estimatedTimeInMillisCounter());
        SearchContext.setCurrent(context);
        try {
            context.scroll(request.scroll());

            parseTemplate(request);
            parseSource(context, request.source());
            parseSource(context, request.extraSource());

            // if the from and size are still not set, default them
            if (context.from() == -1) {
                context.from(0);
            }
            if (context.searchType() == SearchType.COUNT) {
                // so that the optimizations we apply to size=0 also apply to search_type=COUNT
                // and that we close contexts when done with the query phase
                context.searchType(SearchType.QUERY_THEN_FETCH);
                context.size(0);
            } else if (context.size() == -1) {
                context.size(10);
            }

            // pre process
            dfsPhase.preProcess(context);
            queryPhase.preProcess(context);
            fetchPhase.preProcess(context);

            // compute the context keep alive
            long keepAlive = defaultKeepAlive;
            if (request.scroll() != null && request.scroll().keepAlive() != null) {
                keepAlive = request.scroll().keepAlive().millis();
            }
            context.keepAlive(keepAlive);
        } catch (Throwable e) {
            context.close();
            throw ExceptionsHelper.convertToRuntime(e);
        }

        return context;
    }

    private void freeAllContextForIndex(Index index) {
        assert index != null;
        for (SearchContext ctx : activeContexts.values()) {
            if (index.equals(ctx.indexShard().shardId().index())) {
                freeContext(ctx.id());
            }
        }
    }


    public boolean freeContext(long id) {
        final SearchContext context = removeContext(id);
        if (context != null) {
            try {
                context.indexShard().searchService().onFreeContext(context);
            } finally {
                context.close();
            }
            return true;
        }
        return false;
    }

    public void freeAllScrollContexts() {
        for (SearchContext searchContext : activeContexts.values()) {
            if (searchContext.scroll() != null) {
                freeContext(searchContext.id());
            }
        }
    }

    private void contextProcessing(SearchContext context) {
        // disable timeout while executing a search
        context.accessed(-1);
    }

    private void contextProcessedSuccessfully(SearchContext context) {
        context.accessed(threadPool.estimatedTimeInMillis());
    }

    private void cleanContext(SearchContext context) {
        assert context == SearchContext.current();
        context.clearReleasables(Lifetime.PHASE);
        SearchContext.removeCurrent();
    }

    private void parseTemplate(ShardSearchRequest request) {

        final ExecutableScript executable;
        if (hasLength(request.templateName())) {
            executable = this.scriptService.executable(new Script(MustacheScriptEngineService.NAME, request.templateName(), request.templateType(), request.templateParams()), ScriptContext.Standard.SEARCH);
        } else {
            if (!hasLength(request.templateSource())) {
                return;
            }
            XContentParser parser = null;
            TemplateQueryParser.TemplateContext templateContext = null;

            try {
                parser = XContentFactory.xContent(request.templateSource()).createParser(request.templateSource());
                templateContext = TemplateQueryParser.parse(parser, "params", "template");

                if (templateContext.scriptType() == ScriptService.ScriptType.INLINE) {
                    //Try to double parse for nested template id/file
                    parser = null;
                    try {
                        byte[] templateBytes = templateContext.template().getBytes(Charsets.UTF_8);
                        parser = XContentFactory.xContent(templateBytes).createParser(templateBytes);
                    } catch (ElasticsearchParseException epe) {
                        //This was an non-nested template, the parse failure was due to this, it is safe to assume this refers to a file
                        //for backwards compatibility and keep going
                        templateContext = new TemplateQueryParser.TemplateContext(ScriptService.ScriptType.FILE, templateContext.template(), templateContext.params());
                    }
                    if (parser != null) {
                        TemplateQueryParser.TemplateContext innerContext = TemplateQueryParser.parse(parser, "params");
                        if (hasLength(innerContext.template()) && !innerContext.scriptType().equals(ScriptService.ScriptType.INLINE)) {
                            //An inner template referring to a filename or id
                            templateContext = new TemplateQueryParser.TemplateContext(innerContext.scriptType(), innerContext.template(), templateContext.params());
                        }
                    }
                }
            } catch (IOException e) {
                throw new ElasticsearchParseException("Failed to parse template", e);
            } finally {
                Releasables.closeWhileHandlingException(parser);
            }

            if (!hasLength(templateContext.template())) {
                throw new ElasticsearchParseException("Template must have [template] field configured");
            }
            executable = this.scriptService.executable(new Script(MustacheScriptEngineService.NAME, templateContext.template(), templateContext.scriptType(), templateContext.params()), ScriptContext.Standard.SEARCH);
        }

        BytesReference processedQuery = (BytesReference) executable.run();
        request.source(processedQuery);
    }

    private void parseSource(SearchContext context, BytesReference source) throws SearchParseException {
        // nothing to parse...
        if (source == null || source.length() == 0) {
            return;
        }
        XContentParser parser = null;
        try {
            parser = XContentFactory.xContent(source).createParser(source);
            XContentParser.Token token;
            token = parser.nextToken();
            if (token != XContentParser.Token.START_OBJECT) {
                throw new ElasticsearchParseException("Expected START_OBJECT but got " + token.name() + " " + parser.currentName());
            }
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    String fieldName = parser.currentName();
                    parser.nextToken();
                    SearchParseElement element = elementParsers.get(fieldName);
                    if (element == null) {
                        throw new SearchParseException(context, "No parser for element [" + fieldName + "]");
                    }
                    element.parse(parser, context);
                } else {
                    if (token == null) {
                        throw new ElasticsearchParseException("End of query source reached but query is not complete.");
                    } else {
                        throw new ElasticsearchParseException("Expected field name but got " + token.name() + " \"" + parser.currentName() + "\"");
                    }
                }
            }
        } catch (Throwable e) {
            String sSource = "_na_";
            try {
                sSource = XContentHelper.convertToJson(source, false);
            } catch (Throwable e1) {
                // ignore
            }
            throw new SearchParseException(context, "Failed to parse source [" + sSource + "]", e);
        } finally {
            if (parser != null) {
                parser.close();
            }
        }
    }

    private static final int[] EMPTY_DOC_IDS = new int[0];

    /**
     * Shortcut ids to load, we load only "from" and up to "size". The phase controller
     * handles this as well since the result is always size * shards for Q_A_F
     */
    private void shortcutDocIdsToLoad(SearchContext context) {
        if (context.request().scroll() != null) {
            TopDocs topDocs = context.queryResult().topDocs();
            int[] docIdsToLoad = new int[topDocs.scoreDocs.length];
            for (int i = 0; i < topDocs.scoreDocs.length; i++) {
                docIdsToLoad[i] = topDocs.scoreDocs[i].doc;
            }
            context.docIdsToLoad(docIdsToLoad, 0, docIdsToLoad.length);
        } else {
            TopDocs topDocs = context.queryResult().topDocs();
            if (topDocs.scoreDocs.length < context.from()) {
                // no more docs...
                context.docIdsToLoad(EMPTY_DOC_IDS, 0, 0);
                return;
            }
            int totalSize = context.from() + context.size();
            int[] docIdsToLoad = new int[Math.min(topDocs.scoreDocs.length - context.from(), context.size())];
            int counter = 0;
            for (int i = context.from(); i < totalSize; i++) {
                if (i < topDocs.scoreDocs.length) {
                    docIdsToLoad[counter] = topDocs.scoreDocs[i].doc;
                } else {
                    break;
                }
                counter++;
            }
            context.docIdsToLoad(docIdsToLoad, 0, counter);
        }
    }

    private void shortcutDocIdsToLoadForScanning(SearchContext context) {
        TopDocs topDocs = context.queryResult().topDocs();
        if (topDocs.scoreDocs.length == 0) {
            // no more docs...
            context.docIdsToLoad(EMPTY_DOC_IDS, 0, 0);
            return;
        }
        int[] docIdsToLoad = new int[topDocs.scoreDocs.length];
        for (int i = 0; i < docIdsToLoad.length; i++) {
            docIdsToLoad[i] = topDocs.scoreDocs[i].doc;
        }
        context.docIdsToLoad(docIdsToLoad, 0, docIdsToLoad.length);
    }

    private void processScroll(InternalScrollSearchRequest request, SearchContext context) {
        // process scroll
        context.from(context.from() + context.size());
        context.scroll(request.scroll());
        // update the context keep alive based on the new scroll value
        if (request.scroll() != null && request.scroll().keepAlive() != null) {
            context.keepAlive(request.scroll().keepAlive().millis());
        }
    }

    /**
     * Returns the number of active contexts in this
     * SearchService
     */
    public int getActiveContexts() {
        return this.activeContexts.size();
    }

    static class NormsWarmer extends IndicesWarmer.Listener {

        @Override
        public TerminationHandle warmNewReaders(final IndexShard indexShard, IndexMetaData indexMetaData, final WarmerContext context, ThreadPool threadPool) {
            final Loading defaultLoading = Loading.parse(indexMetaData.settings().get(NORMS_LOADING_KEY), Loading.LAZY);
            final MapperService mapperService = indexShard.mapperService();
            final ObjectSet<String> warmUp = new ObjectOpenHashSet<>();
            for (DocumentMapper docMapper : mapperService.docMappers(false)) {
                for (FieldMapper<?> fieldMapper : docMapper.mappers()) {
                    final String indexName = fieldMapper.names().indexName();
                    if (fieldMapper.fieldType().indexOptions() != IndexOptions.NONE && !fieldMapper.fieldType().omitNorms() && fieldMapper.normsLoading(defaultLoading) == Loading.EAGER) {
                        warmUp.add(indexName);
                    }
                }
            }

            final CountDownLatch latch = new CountDownLatch(1);
            // Norms loading may be I/O intensive but is not CPU intensive, so we execute it in a single task
            threadPool.executor(executor()).execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        for (ObjectCursor<String> stringObjectCursor : warmUp) {
                            final String indexName = stringObjectCursor.value;
                            final long start = System.nanoTime();
                            for (final LeafReaderContext ctx : context.searcher().reader().leaves()) {
                                final NumericDocValues values = ctx.reader().getNormValues(indexName);
                                if (values != null) {
                                    values.get(0);
                                }
                            }
                            if (indexShard.warmerService().logger().isTraceEnabled()) {
                                indexShard.warmerService().logger().trace("warmed norms for [{}], took [{}]", indexName, TimeValue.timeValueNanos(System.nanoTime() - start));
                            }
                        }
                    } catch (Throwable t) {
                        indexShard.warmerService().logger().warn("failed to warm-up norms", t);
                    } finally {
                        latch.countDown();
                    }
                }
            });

            return new TerminationHandle() {
                @Override
                public void awaitTermination() throws InterruptedException {
                    latch.await();
                }
            };
        }

        @Override
        public TerminationHandle warmTopReader(IndexShard indexShard, IndexMetaData indexMetaData, WarmerContext context, ThreadPool threadPool) {
            return TerminationHandle.NO_WAIT;
        }
    }

    static class FieldDataWarmer extends IndicesWarmer.Listener {

        @Override
        public TerminationHandle warmNewReaders(final IndexShard indexShard, IndexMetaData indexMetaData, final WarmerContext context, ThreadPool threadPool) {
            final MapperService mapperService = indexShard.mapperService();
            final Map<String, FieldMapper<?>> warmUp = new HashMap<>();
            for (DocumentMapper docMapper : mapperService.docMappers(false)) {
                for (FieldMapper<?> fieldMapper : docMapper.mappers()) {
                    final FieldDataType fieldDataType = fieldMapper.fieldDataType();
                    if (fieldDataType == null) {
                        continue;
                    }
                    if (fieldDataType.getLoading() == Loading.LAZY) {
                        continue;
                    }

                    final String indexName = fieldMapper.names().indexName();
                    if (warmUp.containsKey(indexName)) {
                        continue;
                    }
                    warmUp.put(indexName, fieldMapper);
                }
            }
            final IndexFieldDataService indexFieldDataService = indexShard.indexFieldDataService();
            final Executor executor = threadPool.executor(executor());
            final CountDownLatch latch = new CountDownLatch(context.searcher().reader().leaves().size() * warmUp.size());
            for (final LeafReaderContext ctx : context.searcher().reader().leaves()) {
                for (final FieldMapper<?> fieldMapper : warmUp.values()) {
                    executor.execute(new Runnable() {

                        @Override
                        public void run() {
                            try {
                                final long start = System.nanoTime();
                                indexFieldDataService.getForField(fieldMapper).load(ctx);
                                if (indexShard.warmerService().logger().isTraceEnabled()) {
                                    indexShard.warmerService().logger().trace("warmed fielddata for [{}], took [{}]", fieldMapper.names().name(), TimeValue.timeValueNanos(System.nanoTime() - start));
                                }
                            } catch (Throwable t) {
                                indexShard.warmerService().logger().warn("failed to warm-up fielddata for [{}]", t, fieldMapper.names().name());
                            } finally {
                                latch.countDown();
                            }
                        }

                    });
                }
            }
            return new TerminationHandle() {
                @Override
                public void awaitTermination() throws InterruptedException {
                    latch.await();
                }
            };
        }

        @Override
        public TerminationHandle warmTopReader(final IndexShard indexShard, IndexMetaData indexMetaData, final WarmerContext context, ThreadPool threadPool) {
            final MapperService mapperService = indexShard.mapperService();
            final Map<String, FieldMapper<?>> warmUpGlobalOrdinals = new HashMap<>();
            for (DocumentMapper docMapper : mapperService.docMappers(false)) {
                for (FieldMapper<?> fieldMapper : docMapper.mappers()) {
                    final FieldDataType fieldDataType = fieldMapper.fieldDataType();
                    if (fieldDataType == null) {
                        continue;
                    }
                    if (fieldDataType.getLoading() != Loading.EAGER_GLOBAL_ORDINALS) {
                        continue;
                    }
                    final String indexName = fieldMapper.names().indexName();
                    if (warmUpGlobalOrdinals.containsKey(indexName)) {
                        continue;
                    }
                    warmUpGlobalOrdinals.put(indexName, fieldMapper);
                }
            }
            final IndexFieldDataService indexFieldDataService = indexShard.indexFieldDataService();
            final Executor executor = threadPool.executor(executor());
            final CountDownLatch latch = new CountDownLatch(warmUpGlobalOrdinals.size());
            for (final FieldMapper<?> fieldMapper : warmUpGlobalOrdinals.values()) {
                executor.execute(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            final long start = System.nanoTime();
                            IndexFieldData.Global ifd = indexFieldDataService.getForField(fieldMapper);
                            ifd.loadGlobal(context.reader());
                            if (indexShard.warmerService().logger().isTraceEnabled()) {
                                indexShard.warmerService().logger().trace("warmed global ordinals for [{}], took [{}]", fieldMapper.names().name(), TimeValue.timeValueNanos(System.nanoTime() - start));
                            }
                        } catch (Throwable t) {
                            indexShard.warmerService().logger().warn("failed to warm-up global ordinals for [{}]", t, fieldMapper.names().name());
                        } finally {
                            latch.countDown();
                        }
                    }
                });
            }
            return new TerminationHandle() {
                @Override
                public void awaitTermination() throws InterruptedException {
                    latch.await();
                }
            };
        }
    }

    class SearchWarmer extends IndicesWarmer.Listener {

        @Override
        public TerminationHandle warmNewReaders(IndexShard indexShard, IndexMetaData indexMetaData, WarmerContext context, ThreadPool threadPool) {
            return internalWarm(indexShard, indexMetaData, context, threadPool, false);
        }

        @Override
        public TerminationHandle warmTopReader(IndexShard indexShard, IndexMetaData indexMetaData, WarmerContext context, ThreadPool threadPool) {
            return internalWarm(indexShard, indexMetaData, context, threadPool, true);
        }

        public TerminationHandle internalWarm(final IndexShard indexShard, final IndexMetaData indexMetaData, final IndicesWarmer.WarmerContext warmerContext, ThreadPool threadPool, final boolean top) {
            IndexWarmersMetaData custom = indexMetaData.custom(IndexWarmersMetaData.TYPE);
            if (custom == null) {
                return TerminationHandle.NO_WAIT;
            }
            final Executor executor = threadPool.executor(executor());
            final CountDownLatch latch = new CountDownLatch(custom.entries().size());
            for (final IndexWarmersMetaData.Entry entry : custom.entries()) {
                executor.execute(new Runnable() {

                    @Override
                    public void run() {
                        SearchContext context = null;
                        try {
                            long now = System.nanoTime();
                            ShardSearchRequest request = new ShardSearchLocalRequest(indexShard.shardId(), indexMetaData.numberOfShards(),
                                    SearchType.QUERY_THEN_FETCH, entry.source(), entry.types(), entry.queryCache());
                            context = createContext(request, warmerContext.searcher());
                            // if we use sort, we need to do query to sort on it and load relevant field data
                            // if not, we might as well set size=0 (and cache if needed)
                            if (context.sort() == null) {
                                context.size(0);
                            }
                            boolean canCache = indicesQueryCache.canCache(request, context);
                            // early terminate when we can cache, since we can only do proper caching on top level searcher
                            // also, if we can't cache, and its top, we don't need to execute it, since we already did when its not top
                            if (canCache != top) {
                                return;
                            }
                            loadOrExecuteQueryPhase(request, context, queryPhase);
                            long took = System.nanoTime() - now;
                            if (indexShard.warmerService().logger().isTraceEnabled()) {
                                indexShard.warmerService().logger().trace("warmed [{}], took [{}]", entry.name(), TimeValue.timeValueNanos(took));
                            }
                        } catch (Throwable t) {
                            indexShard.warmerService().logger().warn("warmer [{}] failed", t, entry.name());
                        } finally {
                            try {
                                if (context != null) {
                                    freeContext(context.id());
                                    cleanContext(context);
                                }
                            } finally {
                                latch.countDown();
                            }
                        }
                    }

                });
            }
            return new TerminationHandle() {
                @Override
                public void awaitTermination() throws InterruptedException {
                    latch.await();
                }
            };
        }
    }

    class Reaper implements Runnable {
        @Override
        public void run() {
            final long time = threadPool.estimatedTimeInMillis();
            for (SearchContext context : activeContexts.values()) {
                // Use the same value for both checks since lastAccessTime can
                // be modified by another thread between checks!
                final long lastAccessTime = context.lastAccessTime();
                if (lastAccessTime == -1l) { // its being processed or timeout is disabled
                    continue;
                }
                if ((time - lastAccessTime > context.keepAlive())) {
                    logger.debug("freeing search context [{}], time [{}], lastAccessTime [{}], keepAlive [{}]", context.id(), time, lastAccessTime, context.keepAlive());
                    freeContext(context.id());
                }
            }
        }
    }
}
