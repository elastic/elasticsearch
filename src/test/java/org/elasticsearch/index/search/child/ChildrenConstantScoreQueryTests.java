/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.elasticsearch.index.search.child;

import com.carrotsearch.hppc.ObjectObjectOpenHashMap;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.*;
import org.apache.lucene.queries.TermFilter;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.cache.recycler.CacheRecycler;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lucene.search.NoopCollector;
import org.elasticsearch.common.lucene.search.XConstantScoreQuery;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.analysis.AnalysisService;
import org.elasticsearch.index.cache.docset.DocSetCache;
import org.elasticsearch.index.cache.filter.FilterCache;
import org.elasticsearch.index.cache.id.IdCache;
import org.elasticsearch.index.cache.id.SimpleIdCacheTests;
import org.elasticsearch.index.cache.id.simple.SimpleIdCache;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.fielddata.IndexFieldDataService;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.FieldMappers;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.mapper.internal.ParentFieldMapper;
import org.elasticsearch.index.mapper.internal.TypeFieldMapper;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;
import org.elasticsearch.index.query.IndexQueryParserService;
import org.elasticsearch.index.query.ParsedFilter;
import org.elasticsearch.index.query.ParsedQuery;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.index.shard.service.IndexShard;
import org.elasticsearch.index.similarity.SimilarityService;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.dfs.DfsSearchResult;
import org.elasticsearch.search.facet.SearchContextFacets;
import org.elasticsearch.search.fetch.FetchSearchResult;
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
import org.elasticsearch.test.ElasticsearchLuceneTestCase;
import org.hamcrest.Description;
import org.hamcrest.StringDescription;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.NavigableSet;
import java.util.TreeSet;

import static org.hamcrest.Matchers.equalTo;

public class ChildrenConstantScoreQueryTests extends ElasticsearchLuceneTestCase {

    static ContextIndexSearcher contextIndexSearcher;

    @BeforeClass
    public static void before() throws IOException {
        SearchContext.setCurrent(createSearchContext("test", "parent", "child"));
    }

    @AfterClass
    public static void after() throws IOException {
        SearchContext.removeCurrent();
    }

    @Test
    public void testSimple() throws Exception {
        Directory directory = newDirectory();
        RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory);

        for (int parent = 1; parent <= 5; parent++) {
            Document document = new Document();
            document.add(new StringField(UidFieldMapper.NAME, Uid.createUid("parent", Integer.toString(parent)), Field.Store.NO));
            document.add(new StringField(TypeFieldMapper.NAME, "parent", Field.Store.NO));
            indexWriter.addDocument(document);

            for (int child = 1; child <= 3; child++) {
                document = new Document();
                document.add(new StringField(UidFieldMapper.NAME, Uid.createUid("child", Integer.toString(parent * 3 + child)), Field.Store.NO));
                document.add(new StringField(TypeFieldMapper.NAME, "child", Field.Store.NO));
                document.add(new StringField(ParentFieldMapper.NAME, Uid.createUid("parent", Integer.toString(parent)), Field.Store.NO));
                document.add(new StringField("field1", "value" + child, Field.Store.NO));
                indexWriter.addDocument(document);
            }
        }

        IndexReader indexReader = DirectoryReader.open(indexWriter.w, false);
        IndexSearcher searcher = new IndexSearcher(indexReader);

        TermQuery childQuery = new TermQuery(new Term("field1", "value" + (1 + random().nextInt(3))));
        TermFilter parentFilter = new TermFilter(new Term(TypeFieldMapper.NAME, "parent"));
        int shortCircuitParentDocSet = random().nextInt(5);
        ChildrenConstantScoreQuery query = new ChildrenConstantScoreQuery(childQuery, "parent", "child", parentFilter, shortCircuitParentDocSet, true);

        BitSetCollector collector = new BitSetCollector(indexReader.maxDoc());
        searcher.search(query, collector);
        FixedBitSet actualResult = collector.getResult();

        assertThat(actualResult.cardinality(), equalTo(5));

        indexWriter.close();
        indexReader.close();
        directory.close();
    }

    @Test
    public void testRandom() throws Exception {
        Directory directory = newDirectory();
        RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory);


        int numUniqueChildValues = 1 + random().nextInt(TEST_NIGHTLY ? 50000 : 1000);
        String[] childValues = new String[numUniqueChildValues];
        for (int i = 0; i < numUniqueChildValues; i++) {
            childValues[i] = Integer.toString(i);
        }

        ObjectObjectOpenHashMap<String, NavigableSet<String>> childValueToParentIds = new ObjectObjectOpenHashMap<String, NavigableSet<String>>();

        int childDocId = 0;
        int numParentDocs = 1 + random().nextInt(TEST_NIGHTLY ? 50000 : 1000);
        for (int parentDocId = 0; parentDocId < numParentDocs; parentDocId++) {
            boolean markParentAsDeleted = rarely();
            String parent = Integer.toString(parentDocId);
            Document document = new Document();
            document.add(new StringField(UidFieldMapper.NAME, Uid.createUid("parent", parent), Field.Store.YES));
            document.add(new StringField(TypeFieldMapper.NAME, "parent", Field.Store.NO));
            if (markParentAsDeleted) {
                document.add(new StringField("delete", "me", Field.Store.NO));
            }
            indexWriter.addDocument(document);

            int numChildDocs = random().nextInt(TEST_NIGHTLY ? 100 : 25);
            for (int i = 0; i < numChildDocs; i++) {
                boolean markChildAsDeleted = rarely();
                String childValue = childValues[random().nextInt(childValues.length)];

                document = new Document();
                document.add(new StringField(UidFieldMapper.NAME, Uid.createUid("child", Integer.toString(childDocId)), Field.Store.NO));
                document.add(new StringField(TypeFieldMapper.NAME, "child", Field.Store.NO));
                document.add(new StringField(ParentFieldMapper.NAME, Uid.createUid("parent", parent), Field.Store.NO));
                document.add(new StringField("field1", childValue, Field.Store.NO));
                if (markChildAsDeleted) {
                    document.add(new StringField("delete", "me", Field.Store.NO));
                }
                indexWriter.addDocument(document);

                if (!markChildAsDeleted) {
                    NavigableSet<String> parentIds;
                    if (childValueToParentIds.containsKey(childValue)) {
                        parentIds = childValueToParentIds.lget();
                    } else {
                        childValueToParentIds.put(childValue, parentIds = new TreeSet<String>());
                    }
                    if (!markParentAsDeleted) {
                        parentIds.add(parent);
                    }
                }
            }
        }

        // Delete docs that are marked to be deleted.
        indexWriter.deleteDocuments(new Term("delete", "me"));

        indexWriter.close();
        IndexReader indexReader = DirectoryReader.open(directory);
        IndexSearcher searcher = new IndexSearcher(indexReader);
        Engine.Searcher engineSearcher = new Engine.SimpleSearcher(
                ChildrenConstantScoreQueryTests.class.getSimpleName(), searcher
        );
        contextIndexSearcher = new ContextIndexSearcher(SearchContext.current(), engineSearcher);

        TermFilter parentFilter = new TermFilter(new Term(TypeFieldMapper.NAME, "parent"));
        for (String childValue : childValues) {
            TermQuery childQuery = new TermQuery(new Term("field1", childValue));
            int shortCircuitParentDocSet = random().nextInt(numParentDocs);
            Query query;
            boolean applyAcceptedDocs = random().nextBoolean();
            if (applyAcceptedDocs) {
                // Usage in HasChildQueryParser
                query = new ChildrenConstantScoreQuery(childQuery, "parent", "child", parentFilter, shortCircuitParentDocSet, applyAcceptedDocs);
            } else {
                // Usage in HasChildFilterParser
                query = new XConstantScoreQuery(
                            new CustomQueryWrappingFilter(
                                    new ChildrenConstantScoreQuery(childQuery, "parent", "child", parentFilter, shortCircuitParentDocSet, applyAcceptedDocs)
                            )
                );
            }
            BitSetCollector collector = new BitSetCollector(indexReader.maxDoc());
            searcher.search(query, collector);
            FixedBitSet actualResult = collector.getResult();

            FixedBitSet expectedResult = new FixedBitSet(indexReader.maxDoc());
            if (childValueToParentIds.containsKey(childValue)) {
                AtomicReader slowAtomicReader = SlowCompositeReaderWrapper.wrap(indexReader);
                Terms terms = slowAtomicReader.terms(UidFieldMapper.NAME);
                if (terms != null) {
                    NavigableSet<String> parentIds = childValueToParentIds.lget();
                    TermsEnum termsEnum = terms.iterator(null);
                    DocsEnum docsEnum = null;
                    for (String id : parentIds) {
                        TermsEnum.SeekStatus seekStatus = termsEnum.seekCeil(Uid.createUidAsBytes("parent", id));
                        if (seekStatus == TermsEnum.SeekStatus.FOUND) {
                            docsEnum = termsEnum.docs(slowAtomicReader.getLiveDocs(), docsEnum, DocsEnum.FLAG_NONE);
                            expectedResult.set(docsEnum.nextDoc());
                        } else if (seekStatus == TermsEnum.SeekStatus.END) {
                            break;
                        }
                    }
                }
            }

            assertEquals(actualResult, expectedResult, searcher);
        }

        indexReader.close();
        directory.close();
    }

    private void assertEquals(FixedBitSet actual, FixedBitSet expected, IndexSearcher searcher) throws IOException {
        if (!actual.equals(expected)) {
            Description description= new StringDescription();
            description.appendText(reason(actual, expected, searcher));
            description.appendText("\nExpected: ");
            description.appendValue(expected);
            description.appendText("\n     got: ");
            description.appendValue(actual);
            description.appendText("\n");
            throw new java.lang.AssertionError(description.toString());
        }
    }

    private String reason(FixedBitSet actual, FixedBitSet expected, IndexSearcher indexSearcher) throws IOException {
        StringBuilder builder = new StringBuilder();
        builder.append("expected cardinality:").append(expected.cardinality()).append('\n');
        DocIdSetIterator iterator = expected.iterator();
        for (int doc = iterator.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = iterator.nextDoc()) {
            builder.append("Expected doc[").append(doc).append("] with id value ").append(indexSearcher.doc(doc).get(UidFieldMapper.NAME)).append('\n');
        }
        builder.append("actual cardinality: ").append(actual.cardinality()).append('\n');
        iterator = actual.iterator();
        for (int doc = iterator.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = iterator.nextDoc()) {
            builder.append("Actual doc[").append(doc).append("] with id value ").append(indexSearcher.doc(doc).get(UidFieldMapper.NAME)).append('\n');
        }
        return builder.toString();
    }

    static SearchContext createSearchContext(String indexName, String parentType, String childType) throws IOException {
        final Index index = new Index(indexName);
        final IdCache idCache = new SimpleIdCache(index, ImmutableSettings.EMPTY);
        final CacheRecycler cacheRecycler = new CacheRecycler(ImmutableSettings.EMPTY);
        MapperService mapperService = new MapperService(
                index, ImmutableSettings.EMPTY, new Environment(), new AnalysisService(index), null, null, null
        );
        mapperService.merge(
                childType, PutMappingRequest.buildFromSimplifiedDef(childType, "_parent", "type=" + parentType).string(), true
        );
        final IndexService indexService = new SimpleIdCacheTests.StubIndexService(mapperService);
        idCache.setIndexService(indexService);
        return new TestSearchContext(cacheRecycler, idCache, indexService);
    }

    private class BitSetCollector extends NoopCollector {

        final FixedBitSet result;
        int docBase;

        private BitSetCollector(int topLevelMaxDoc) {
            this.result = new FixedBitSet(topLevelMaxDoc);
        }

        @Override
        public void collect(int doc) throws IOException {
            result.set(docBase + doc);
        }

        @Override
        public void setNextReader(AtomicReaderContext context) throws IOException {
            docBase = context.docBase;
        }

        private FixedBitSet getResult() {
            return result;
        }
    }

    private static class TestSearchContext extends SearchContext {

        final CacheRecycler cacheRecycler;
        final IdCache idCache;
        final IndexService indexService;

        private TestSearchContext(CacheRecycler cacheRecycler, IdCache idCache, IndexService indexService) {
            this.cacheRecycler = cacheRecycler;
            this.idCache = idCache;
            this.indexService = indexService;
        }

        @Override
        public boolean clearAndRelease() {
            return false;
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
            return 0;
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
            return null;
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
        public RescoreSearchContext rescore() {
            return null;
        }

        @Override
        public void rescore(RescoreSearchContext rescore) {
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
            return false;
        }

        @Override
        public boolean hasFetchSourceContext() {
            return false;
        }

        @Override
        public FetchSourceContext fetchSourceContext() {
            return null;
        }

        @Override
        public SearchContext fetchSourceContext(FetchSourceContext fetchSourceContext) {
            return null;
        }

        @Override
        public ContextIndexSearcher searcher() {
            return contextIndexSearcher;
        }

        @Override
        public IndexShard indexShard() {
            return null;
        }

        @Override
        public MapperService mapperService() {
            return indexService.mapperService();
        }

        @Override
        public AnalysisService analysisService() {
            return indexService.analysisService();
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
            return cacheRecycler;
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
            return null;
        }

        @Override
        public IdCache idCache() {
            return idCache;
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
            return null;
        }

        @Override
        public Sort sort() {
            return null;
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
        public SearchContext parsedFilter(ParsedFilter filter) {
            return null;
        }

        @Override
        public ParsedFilter parsedFilter() {
            return null;
        }

        @Override
        public Filter aliasFilter() {
            return null;
        }

        @Override
        public SearchContext parsedQuery(ParsedQuery query) {
            return null;
        }

        @Override
        public ParsedQuery parsedQuery() {
            return null;
        }

        @Override
        public Query query() {
            return null;
        }

        @Override
        public boolean queryRewritten() {
            return false;
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
            return 0;
        }

        @Override
        public SearchContext size(int size) {
            return null;
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
            return new int[0];
        }

        @Override
        public int docIdsToLoadFrom() {
            return 0;
        }

        @Override
        public int docIdsToLoadSize() {
            return 0;
        }

        @Override
        public SearchContext docIdsToLoad(int[] docIdsToLoad, int docsIdsToLoadFrom, int docsIdsToLoadSize) {
            return null;
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
        public SearchLookup lookup() {
            return null;
        }

        @Override
        public DfsSearchResult dfsResult() {
            return null;
        }

        @Override
        public QuerySearchResult queryResult() {
            return null;
        }

        @Override
        public FetchSearchResult fetchResult() {
            return null;
        }

        @Override
        public void addReleasable(Releasable releasable) {
        }

        @Override
        public void clearReleasables() {
        }

        @Override
        public ScanContext scanContext() {
            return null;
        }

        @Override
        public MapperService.SmartNameFieldMappers smartFieldMappers(String name) {
            return null;
        }

        @Override
        public FieldMappers smartNameFieldMappers(String name) {
            return null;
        }

        @Override
        public FieldMapper smartNameFieldMapper(String name) {
            return null;
        }

        @Override
        public MapperService.SmartNameObjectMapper smartNameObjectMapper(String name) {
            return null;
        }

        @Override
        public boolean release() throws ElasticSearchException {
            return false;
        }
    }

}
