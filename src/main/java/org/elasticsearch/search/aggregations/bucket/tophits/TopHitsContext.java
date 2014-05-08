package org.elasticsearch.search.aggregations.bucket.tophits;

import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.cache.recycler.CacheRecycler;
import org.elasticsearch.cache.recycler.PageCacheRecycler;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.analysis.AnalysisService;
import org.elasticsearch.index.cache.docset.DocSetCache;
import org.elasticsearch.index.cache.filter.FilterCache;
import org.elasticsearch.index.fielddata.IndexFieldDataService;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.FieldMappers;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.IndexQueryParserService;
import org.elasticsearch.index.query.ParsedFilter;
import org.elasticsearch.index.query.ParsedQuery;
import org.elasticsearch.index.shard.service.IndexShard;
import org.elasticsearch.index.similarity.SimilarityService;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.aggregations.SearchContextAggregations;
import org.elasticsearch.search.dfs.DfsSearchResult;
import org.elasticsearch.search.facet.SearchContextFacets;
import org.elasticsearch.search.fetch.FetchSearchResult;
import org.elasticsearch.search.fetch.fielddata.FieldDataFieldsContext;
import org.elasticsearch.search.fetch.partial.PartialFieldsContext;
import org.elasticsearch.search.fetch.script.ScriptFieldsContext;
import org.elasticsearch.search.fetch.source.FetchSourceContext;
import org.elasticsearch.search.highlight.SearchContextHighlight;
import org.elasticsearch.search.internal.ContextIndexSearcher;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.search.rescore.RescoreSearchContext;
import org.elasticsearch.search.scan.ScanContext;
import org.elasticsearch.search.suggest.SuggestionSearchContext;

import java.util.List;

/**
 */
public class TopHitsContext extends SearchContext {

    private int size = 3;
    private Sort sort;

    private final FetchSearchResult fetchSearchResult;
    private final QuerySearchResult querySearchResult;

    private int[] docIdsToLoad;
    private int docsIdsToLoadFrom;
    private int docsIdsToLoadSize;

    private final SearchContext context;
    private FetchSourceContext fetchSourceContext;

    public TopHitsContext(SearchContext context) {
        this.fetchSearchResult = new FetchSearchResult();
        this.querySearchResult = new QuerySearchResult();
        this.context = context;
    }

    @Override
    protected void doClose() {

    }

    @Override
    public void preProcess() {

    }

    @Override
    public Filter searchFilter(String[] types) {
        return null;
    }

    @Override
    public long id() {
        return context.id();
    }

    @Override
    public String source() {
        return null;
    }

    @Override
    public ShardSearchRequest request() {
        return null;
    }

    @Override
    public SearchType searchType() {
        return null;
    }

    @Override
    public SearchContext searchType(SearchType searchType) {
        return null;
    }

    @Override
    public SearchShardTarget shardTarget() {
        return context.shardTarget();
    }

    @Override
    public int numberOfShards() {
        return 0;
    }

    @Override
    public boolean hasTypes() {
        return false;
    }

    @Override
    public String[] types() {
        return new String[0];
    }

    @Override
    public float queryBoost() {
        return 0;
    }

    @Override
    public SearchContext queryBoost(float queryBoost) {
        return null;
    }

    @Override
    public long nowInMillis() {
        return 0;
    }

    @Override
    public Scroll scroll() {
        return null;
    }

    @Override
    public SearchContext scroll(Scroll scroll) {
        return null;
    }

    @Override
    public SearchContextAggregations aggregations() {
        return null;
    }

    @Override
    public SearchContext aggregations(SearchContextAggregations aggregations) {
        return null;
    }

    @Override
    public SearchContextFacets facets() {
        return null;
    }

    @Override
    public SearchContext facets(SearchContextFacets facets) {
        return null;
    }

    @Override
    public SearchContextHighlight highlight() {
        return null;
    }

    @Override
    public void highlight(SearchContextHighlight highlight) {

    }

    @Override
    public SuggestionSearchContext suggest() {
        return null;
    }

    @Override
    public void suggest(SuggestionSearchContext suggest) {

    }

    @Override
    public List<RescoreSearchContext> rescore() {
        return null;
    }

    @Override
    public void addRescore(RescoreSearchContext rescore) {

    }

    @Override
    public boolean hasFieldDataFields() {
        return false;
    }

    @Override
    public FieldDataFieldsContext fieldDataFields() {
        return null;
    }

    @Override
    public boolean hasScriptFields() {
        return false;
    }

    @Override
    public ScriptFieldsContext scriptFields() {
        return null;
    }

    @Override
    public boolean hasPartialFields() {
        return false;
    }

    @Override
    public PartialFieldsContext partialFields() {
        return null;
    }

    @Override
    public boolean sourceRequested() {
        return fetchSourceContext != null && fetchSourceContext.fetchSource();
    }

    @Override
    public boolean hasFetchSourceContext() {
        return fetchSourceContext != null;
    }

    @Override
    public FetchSourceContext fetchSourceContext() {
        return fetchSourceContext;
    }

    @Override
    public SearchContext fetchSourceContext(FetchSourceContext fetchSourceContext) {
        this.fetchSourceContext = fetchSourceContext;
        return this;
    }

    @Override
    public ContextIndexSearcher searcher() {
        return context.searcher();
    }

    @Override
    public IndexShard indexShard() {
        return null;
    }

    @Override
    public MapperService mapperService() {
        return context.mapperService();
    }

    @Override
    public AnalysisService analysisService() {
        return null;
    }

    @Override
    public IndexQueryParserService queryParserService() {
        return null;
    }

    @Override
    public SimilarityService similarityService() {
        return null;
    }

    @Override
    public ScriptService scriptService() {
        return null;
    }

    @Override
    public CacheRecycler cacheRecycler() {
        return null;
    }

    @Override
    public PageCacheRecycler pageCacheRecycler() {
        return null;
    }

    @Override
    public BigArrays bigArrays() {
        return null;
    }

    @Override
    public FilterCache filterCache() {
        return null;
    }

    @Override
    public DocSetCache docSetCache() {
        return null;
    }

    @Override
    public IndexFieldDataService fieldData() {
        return context.fieldData();
    }

    @Override
    public long timeoutInMillis() {
        return 0;
    }

    @Override
    public void timeoutInMillis(long timeoutInMillis) {

    }

    @Override
    public SearchContext minimumScore(float minimumScore) {
        return null;
    }

    @Override
    public Float minimumScore() {
        return null;
    }

    @Override
    public SearchContext sort(Sort sort) {
        this.sort = sort;
        return null;
    }

    @Override
    public Sort sort() {
        return sort;
    }

    @Override
    public SearchContext trackScores(boolean trackScores) {
        return null;
    }

    @Override
    public boolean trackScores() {
        return false;
    }

    @Override
    public SearchContext parsedPostFilter(ParsedFilter postFilter) {
        return null;
    }

    @Override
    public ParsedFilter parsedPostFilter() {
        return null;
    }

    @Override
    public Filter aliasFilter() {
        return null;
    }

    @Override
    public SearchContext parsedQuery(ParsedQuery query) {
        return context.parsedQuery(query);
    }

    @Override
    public ParsedQuery parsedQuery() {
        return context.parsedQuery();
    }

    @Override
    public Query query() {
        return context.query();
    }

    @Override
    public boolean queryRewritten() {
        return context.queryRewritten();
    }

    @Override
    public SearchContext updateRewriteQuery(Query rewriteQuery) {
        return null;
    }

    @Override
    public int from() {
        return 0;
    }

    @Override
    public SearchContext from(int from) {
        return null;
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public SearchContext size(int size) {
        this.size = size;
        return this;
    }

    @Override
    public boolean hasFieldNames() {
        return false;
    }

    @Override
    public List<String> fieldNames() {
        return null;
    }

    @Override
    public void emptyFieldNames() {

    }

    @Override
    public boolean explain() {
        return false;
    }

    @Override
    public void explain(boolean explain) {

    }

    @Override
    public List<String> groupStats() {
        return null;
    }

    @Override
    public void groupStats(List<String> groupStats) {

    }

    @Override
    public boolean version() {
        return false;
    }

    @Override
    public void version(boolean version) {

    }

    @Override
    public int[] docIdsToLoad() {
        return docIdsToLoad;
    }

    @Override
    public int docIdsToLoadFrom() {
        return docsIdsToLoadFrom;
    }

    @Override
    public int docIdsToLoadSize() {
        return docsIdsToLoadSize;
    }

    @Override
    public SearchContext docIdsToLoad(int[] docIdsToLoad, int docsIdsToLoadFrom, int docsIdsToLoadSize) {
        this.docIdsToLoad = docIdsToLoad;
        this.docsIdsToLoadFrom = docsIdsToLoadFrom;
        this.docsIdsToLoadSize = docsIdsToLoadSize;
        return this;
    }

    @Override
    public void accessed(long accessTime) {

    }

    @Override
    public long lastAccessTime() {
        return 0;
    }

    @Override
    public long keepAlive() {
        return 0;
    }

    @Override
    public void keepAlive(long keepAlive) {

    }

    @Override
    public void lastEmittedDoc(ScoreDoc doc) {

    }

    @Override
    public ScoreDoc lastEmittedDoc() {
        return null;
    }

    @Override
    public SearchLookup lookup() {
        return context.lookup();
    }

    @Override
    public DfsSearchResult dfsResult() {
        return null;
    }

    @Override
    public QuerySearchResult queryResult() {
        return querySearchResult;
    }

    @Override
    public FetchSearchResult fetchResult() {
        return fetchSearchResult;
    }

    @Override
    public ScanContext scanContext() {
        return null;
    }

    @Override
    public MapperService.SmartNameFieldMappers smartFieldMappers(String name) {
        return context.smartFieldMappers(name);
    }

    @Override
    public FieldMappers smartNameFieldMappers(String name) {
        return context.smartNameFieldMappers(name);
    }

    @Override
    public FieldMapper smartNameFieldMapper(String name) {
        return context.smartNameFieldMapper(name);
    }

    @Override
    public MapperService.SmartNameObjectMapper smartNameObjectMapper(String name) {
        return null;
    }

    @Override
    public boolean useSlowScroll() {
        return false;
    }

    @Override
    public SearchContext useSlowScroll(boolean useSlowScroll) {
        return null;
    }
}
