/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import org.apache.lucene.search.TopDocs;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.Unicode;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.timer.Timeout;
import org.elasticsearch.common.timer.TimerTask;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.ConcurrentMapLong;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.service.IndexShard;
import org.elasticsearch.indices.IndicesLifecycle;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.dfs.CachedDfSource;
import org.elasticsearch.search.dfs.DfsPhase;
import org.elasticsearch.search.dfs.DfsSearchResult;
import org.elasticsearch.search.fetch.*;
import org.elasticsearch.search.internal.InternalScrollSearchRequest;
import org.elasticsearch.search.internal.InternalSearchRequest;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.query.*;
import org.elasticsearch.timer.TimerService;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.common.unit.TimeValue.*;

/**
 * @author kimchy (shay.banon)
 */
public class SearchService extends AbstractLifecycleComponent<SearchService> {

    private final ClusterService clusterService;

    private final IndicesService indicesService;

    private final TimerService timerService;

    private final ScriptService scriptService;

    private final DfsPhase dfsPhase;

    private final QueryPhase queryPhase;

    private final FetchPhase fetchPhase;


    private final TimeValue defaultKeepAlive;


    private final AtomicLong idGenerator = new AtomicLong();

    private final CleanContextOnIndicesLifecycleListener indicesLifecycleListener = new CleanContextOnIndicesLifecycleListener();

    private final ConcurrentMapLong<SearchContext> activeContexts = ConcurrentCollections.newConcurrentMapLong();

    private final ImmutableMap<String, SearchParseElement> elementParsers;

    @Inject public SearchService(Settings settings, ClusterService clusterService, IndicesService indicesService, IndicesLifecycle indicesLifecycle, TimerService timerService,
                                 ScriptService scriptService, DfsPhase dfsPhase, QueryPhase queryPhase, FetchPhase fetchPhase) {
        super(settings);
        this.clusterService = clusterService;
        this.indicesService = indicesService;
        this.timerService = timerService;
        this.scriptService = scriptService;
        this.dfsPhase = dfsPhase;
        this.queryPhase = queryPhase;
        this.fetchPhase = fetchPhase;

        // we can have 5 minutes here, since we make sure to clean with search requests and when shard/index closes
        this.defaultKeepAlive = componentSettings.getAsTime("default_keep_alive", timeValueMinutes(5));

        Map<String, SearchParseElement> elementParsers = new HashMap<String, SearchParseElement>();
        elementParsers.putAll(dfsPhase.parseElements());
        elementParsers.putAll(queryPhase.parseElements());
        elementParsers.putAll(fetchPhase.parseElements());
        this.elementParsers = ImmutableMap.copyOf(elementParsers);
        indicesLifecycle.addListener(indicesLifecycleListener);
    }

    @Override protected void doStart() throws ElasticSearchException {
    }

    @Override protected void doStop() throws ElasticSearchException {
        for (SearchContext context : activeContexts.values()) {
            freeContext(context);
        }
        activeContexts.clear();
    }

    @Override protected void doClose() throws ElasticSearchException {
        indicesService.indicesLifecycle().removeListener(indicesLifecycleListener);
    }

    public void releaseContextsForIndex(Index index) {
        for (SearchContext context : activeContexts.values()) {
            if (context.shardTarget().index().equals(index.name())) {
                freeContext(context);
            }
        }
    }

    public void releaseContextsForShard(ShardId shardId) {
        for (SearchContext context : activeContexts.values()) {
            if (context.shardTarget().index().equals(shardId.index().name()) && context.shardTarget().shardId() == shardId.id()) {
                freeContext(context);
            }
        }
    }

    public DfsSearchResult executeDfsPhase(InternalSearchRequest request) throws ElasticSearchException {
        SearchContext context = createContext(request);
        activeContexts.put(context.id(), context);
        try {
            contextProcessing(context);
            dfsPhase.execute(context);
            contextProcessingDone(context);
            return context.dfsResult();
        } catch (RuntimeException e) {
            freeContext(context);
            throw e;
        }
    }

    public QuerySearchResult executeQueryPhase(InternalSearchRequest request) throws ElasticSearchException {
        SearchContext context = createContext(request);
        activeContexts.put(context.id(), context);
        try {
            contextProcessing(context);
            queryPhase.execute(context);
            contextProcessingDone(context);
            return context.queryResult();
        } catch (RuntimeException e) {
            freeContext(context);
            throw e;
        }
    }

    public ScrollQuerySearchResult executeQueryPhase(InternalScrollSearchRequest request) throws ElasticSearchException {
        SearchContext context = findContext(request.id());
        try {
            contextProcessing(context);
            processScroll(request, context);
            contextProcessingDone(context);
            queryPhase.execute(context);
            return new ScrollQuerySearchResult(context.queryResult(), context.shardTarget());
        } catch (RuntimeException e) {
            freeContext(context);
            throw e;
        }
    }

    public QuerySearchResult executeQueryPhase(QuerySearchRequest request) throws ElasticSearchException {
        SearchContext context = findContext(request.id());
        contextProcessing(context);
        try {
            context.searcher().dfSource(new CachedDfSource(request.dfs(), context.similarityService().defaultSearchSimilarity()));
        } catch (IOException e) {
            freeContext(context);
            throw new QueryPhaseExecutionException(context, "Failed to set aggregated df", e);
        }
        try {
            queryPhase.execute(context);
            contextProcessingDone(context);
            return context.queryResult();
        } catch (RuntimeException e) {
            freeContext(context);
            throw e;
        }
    }

    public QueryFetchSearchResult executeFetchPhase(InternalSearchRequest request) throws ElasticSearchException {
        SearchContext context = createContext(request);
        activeContexts.put(context.id(), context);
        contextProcessing(context);
        try {
            queryPhase.execute(context);
            shortcutDocIdsToLoad(context);
            fetchPhase.execute(context);
            if (context.scroll() == null) {
                freeContext(context.id());
            } else {
                contextProcessingDone(context);
            }
            return new QueryFetchSearchResult(context.queryResult(), context.fetchResult());
        } catch (RuntimeException e) {
            freeContext(context);
            throw e;
        }
    }

    public QueryFetchSearchResult executeFetchPhase(QuerySearchRequest request) throws ElasticSearchException {
        SearchContext context = findContext(request.id());
        contextProcessing(context);
        try {
            context.searcher().dfSource(new CachedDfSource(request.dfs(), context.similarityService().defaultSearchSimilarity()));
        } catch (IOException e) {
            freeContext(context);
            throw new QueryPhaseExecutionException(context, "Failed to set aggregated df", e);
        }
        try {
            queryPhase.execute(context);
            shortcutDocIdsToLoad(context);
            fetchPhase.execute(context);
            if (context.scroll() == null) {
                freeContext(request.id());
            } else {
                contextProcessingDone(context);
            }
            return new QueryFetchSearchResult(context.queryResult(), context.fetchResult());
        } catch (RuntimeException e) {
            freeContext(context);
            throw e;
        }
    }

    public ScrollQueryFetchSearchResult executeFetchPhase(InternalScrollSearchRequest request) throws ElasticSearchException {
        SearchContext context = findContext(request.id());
        contextProcessing(context);
        try {
            processScroll(request, context);
            queryPhase.execute(context);
            shortcutDocIdsToLoad(context);
            fetchPhase.execute(context);
            if (context.scroll() == null) {
                freeContext(request.id());
            } else {
                contextProcessingDone(context);
            }
            return new ScrollQueryFetchSearchResult(new QueryFetchSearchResult(context.queryResult(), context.fetchResult()), context.shardTarget());
        } catch (RuntimeException e) {
            freeContext(context);
            throw e;
        }
    }

    public FetchSearchResult executeFetchPhase(FetchSearchRequest request) throws ElasticSearchException {
        SearchContext context = findContext(request.id());
        contextProcessing(context);
        try {
            context.docIdsToLoad(request.docIds(), 0, request.docIdsSize());
            fetchPhase.execute(context);
            if (context.scroll() == null) {
                freeContext(request.id());
            } else {
                contextProcessingDone(context);
            }
            return context.fetchResult();
        } catch (RuntimeException e) {
            freeContext(context);
            throw e;
        }
    }

    private SearchContext findContext(long id) throws SearchContextMissingException {
        SearchContext context = activeContexts.get(id);
        if (context == null) {
            throw new SearchContextMissingException(id);
        }
        return context;
    }

    private SearchContext createContext(InternalSearchRequest request) throws ElasticSearchException {
        IndexService indexService = indicesService.indexServiceSafe(request.index());
        IndexShard indexShard = indexService.shardSafe(request.shardId());

        SearchShardTarget shardTarget = new SearchShardTarget(clusterService.localNode().id(), request.index(), request.shardId());

        Engine.Searcher engineSearcher = indexShard.searcher();
        SearchContext context = new SearchContext(idGenerator.incrementAndGet(), shardTarget, request.numberOfShards(), request.timeout(), request.types(), engineSearcher, indexService, scriptService);

        try {
            context.scroll(request.scroll());

            parseSource(context, request.source(), request.sourceOffset(), request.sourceLength());
            parseSource(context, request.extraSource(), request.extraSourceOffset(), request.extraSourceLength());

            // if the from and size are still not set, default them
            if (context.from() == -1) {
                context.from(0);
            }
            if (context.size() == -1) {
                context.size(10);
            }

            // pre process
            dfsPhase.preProcess(context);
            queryPhase.preProcess(context);
            fetchPhase.preProcess(context);

            // compute the context keep alive
            TimeValue keepAlive = defaultKeepAlive;
            if (request.scroll() != null && request.scroll().keepAlive() != null) {
                keepAlive = request.scroll().keepAlive();
            }
            context.keepAlive(keepAlive);
        } catch (RuntimeException e) {
            context.release();
            throw e;
        }

        return context;
    }

    public void freeContext(long id) {
        SearchContext context = activeContexts.remove(id);
        if (context == null) {
            return;
        }
        freeContext(context);
    }

    private void freeContext(SearchContext context) {
        activeContexts.remove(context.id());
        context.release();
    }

    private void contextProcessing(SearchContext context) {
        if (context.keepAliveTimeout() != null) {
            ((KeepAliveTimerTask) context.keepAliveTimeout().getTask()).processing();
        }
    }

    private void contextProcessingDone(SearchContext context) {
        if (context.keepAliveTimeout() != null) {
            ((KeepAliveTimerTask) context.keepAliveTimeout().getTask()).doneProcessing();
        } else {
            context.accessed(timerService.estimatedTimeInMillis());
            context.keepAliveTimeout(timerService.newTimeout(new KeepAliveTimerTask(context), context.keepAlive(), TimerService.ExecutionType.DEFAULT));
        }
    }

    private void parseSource(SearchContext context, byte[] source, int offset, int length) throws SearchParseException {
        // nothing to parse...
        if (source == null || length == 0) {
            return;
        }
        try {
            XContentParser parser = XContentFactory.xContent(source, offset, length).createParser(source, offset, length);
            XContentParser.Token token;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    String fieldName = parser.currentName();
                    parser.nextToken();
                    SearchParseElement element = elementParsers.get(fieldName);
                    if (element == null) {
                        throw new SearchParseException(context, "No parser for element [" + fieldName + "]");
                    }
                    element.parse(parser, context);
                } else if (token == null) {
                    break;
                }
            }
            parser.close();
        } catch (Exception e) {
            String sSource = "_na_";
            try {
                sSource = Unicode.fromBytes(source, offset, length);
            } catch (Exception e1) {
                // ignore
            }
            throw new SearchParseException(context, "Failed to parse [" + sSource + "]", e);
        }
    }

    private static final int[] EMPTY_DOC_IDS = new int[0];

    private void shortcutDocIdsToLoad(SearchContext context) {
        TopDocs topDocs = context.queryResult().topDocs();
        if (topDocs.scoreDocs.length < context.from()) {
            // no more docs...
            context.docIdsToLoad(EMPTY_DOC_IDS, 0, 0);
            return;
        }
        int totalSize = context.from() + context.size();
        int[] docIdsToLoad = new int[context.size()];
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

    private void processScroll(InternalScrollSearchRequest request, SearchContext context) {
        // process scroll
        context.from(context.from() + context.size());
        context.scroll(request.scroll());
        // update the context keep alive based on the new scroll value
        if (request.scroll() != null && request.scroll().keepAlive() != null) {
            context.keepAlive(request.scroll().keepAlive());
        }
    }

    class CleanContextOnIndicesLifecycleListener extends IndicesLifecycle.Listener {

        @Override public void beforeIndexClosed(IndexService indexService, boolean delete) {
            releaseContextsForIndex(indexService.index());
        }

        @Override public void beforeIndexShardClosed(ShardId shardId, @Nullable IndexShard indexShard, boolean delete) {
            releaseContextsForShard(shardId);
        }
    }

    class KeepAliveTimerTask implements TimerTask {

        private final SearchContext context;

        KeepAliveTimerTask(SearchContext context) {
            this.context = context;
        }

        public void processing() {
            context.keepAliveTimeout().cancel();
        }

        public void doneProcessing() {
            context.accessed(timerService.estimatedTimeInMillis());
            context.keepAliveTimeout(timerService.newTimeout(this, context.keepAlive(), TimerService.ExecutionType.DEFAULT));
        }

        @Override public void run(Timeout timeout) throws Exception {
            if (timeout.isCancelled()) {
                return;
            }
            long currentTime = timerService.estimatedTimeInMillis();
            long nextDelay = context.keepAlive().millis() - (currentTime - context.lastAccessTime());
            if (nextDelay <= 0) {
                // Time out, free the context (and remove it from the active context)
                freeContext(context.id());
            } else {
                // Read occurred before the timeout - set a new timeout with shorter delay.
                context.keepAliveTimeout(timerService.newTimeout(this, nextDelay, TimeUnit.MILLISECONDS, TimerService.ExecutionType.DEFAULT));
            }
        }
    }
}
