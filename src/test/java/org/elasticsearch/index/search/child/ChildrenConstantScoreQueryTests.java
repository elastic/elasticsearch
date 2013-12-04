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

import com.carrotsearch.hppc.IntOpenHashSet;
import com.carrotsearch.hppc.ObjectObjectOpenHashMap;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.*;
import org.apache.lucene.queries.TermFilter;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.cache.recycler.CacheRecycler;
import org.elasticsearch.common.compress.CompressedString;
import org.elasticsearch.common.lucene.search.NotFilter;
import org.elasticsearch.common.lucene.search.XConstantScoreQuery;
import org.elasticsearch.common.lucene.search.XFilteredQuery;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.aliases.IndexAliasesService;
import org.elasticsearch.index.analysis.AnalysisService;
import org.elasticsearch.index.cache.filter.weighted.WeightedFilterCache;
import org.elasticsearch.index.cache.id.IdCache;
import org.elasticsearch.index.cache.id.SimpleIdCacheTests;
import org.elasticsearch.index.cache.id.simple.SimpleIdCache;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.mapper.internal.ParentFieldMapper;
import org.elasticsearch.index.mapper.internal.TypeFieldMapper;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;
import org.elasticsearch.index.search.nested.NonNestedDocsFilter;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.indices.cache.filter.IndicesFilterCache;
import org.elasticsearch.node.settings.NodeSettingsService;
import org.elasticsearch.search.internal.ContextIndexSearcher;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.test.ElasticsearchLuceneTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.hamcrest.Description;
import org.hamcrest.StringDescription;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.NavigableSet;
import java.util.TreeSet;

import static org.hamcrest.Matchers.equalTo;

public class ChildrenConstantScoreQueryTests extends ElasticsearchLuceneTestCase {

    @BeforeClass
    public static void before() throws IOException {
        forceDefaultCodec();
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
        ChildrenConstantScoreQuery query = new ChildrenConstantScoreQuery(childQuery, "parent", "child", parentFilter, shortCircuitParentDocSet, null);

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
        int numUniqueChildValues = 1 + random().nextInt(TEST_NIGHTLY ? 10000 : 1000);
        String[] childValues = new String[numUniqueChildValues];
        for (int i = 0; i < numUniqueChildValues; i++) {
            childValues[i] = Integer.toString(i);
        }

        IntOpenHashSet filteredOrDeletedDocs = new IntOpenHashSet();
        int childDocId = 0;
        int numParentDocs = 1 + random().nextInt(TEST_NIGHTLY ? 20000 : 1000);
        ObjectObjectOpenHashMap<String, NavigableSet<String>> childValueToParentIds = new ObjectObjectOpenHashMap<String, NavigableSet<String>>();
        for (int parentDocId = 0; parentDocId < numParentDocs; parentDocId++) {
            boolean markParentAsDeleted = rarely();
            boolean filterMe = rarely();
            String parent = Integer.toString(parentDocId);
            Document document = new Document();
            document.add(new StringField(UidFieldMapper.NAME, Uid.createUid("parent", parent), Field.Store.YES));
            document.add(new StringField(TypeFieldMapper.NAME, "parent", Field.Store.NO));
            if (markParentAsDeleted) {
                filteredOrDeletedDocs.add(parentDocId);
                document.add(new StringField("delete", "me", Field.Store.NO));
            }
            if (filterMe) {
                filteredOrDeletedDocs.add(parentDocId);
                document.add(new StringField("filter", "me", Field.Store.NO));
            }
            indexWriter.addDocument(document);

            int numChildDocs;
            if (rarely()) {
                numChildDocs = random().nextInt(TEST_NIGHTLY ? 100 : 25);
            } else {
                numChildDocs = random().nextInt(TEST_NIGHTLY ? 40 : 10);
            }
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
                    if (!markParentAsDeleted && !filterMe) {
                        parentIds.add(parent);
                    }
                }
            }
        }

        // Delete docs that are marked to be deleted.
        indexWriter.deleteDocuments(new Term("delete", "me"));

        indexWriter.commit();
        IndexReader indexReader = DirectoryReader.open(directory);
        IndexSearcher searcher = new IndexSearcher(indexReader);
        Engine.Searcher engineSearcher = new Engine.SimpleSearcher(
                ChildrenConstantScoreQueryTests.class.getSimpleName(), searcher
        );
        ((TestSearchContext) SearchContext.current()).setSearcher(new ContextIndexSearcher(SearchContext.current(), engineSearcher));

        Filter rawParentFilter = new TermFilter(new Term(TypeFieldMapper.NAME, "parent"));
        Filter rawFilterMe = new NotFilter(new TermFilter(new Term("filter", "me")));
        int max = numUniqueChildValues / 4;
        for (int i = 0; i < max; i++) {
            // Randomly pick a cached version: there is specific logic inside ChildrenQuery that deals with the fact
            // that deletes are applied at the top level when filters are cached.
            Filter parentFilter;
            if (random().nextBoolean()) {
                parentFilter = SearchContext.current().filterCache().cache(rawParentFilter);
            } else {
                parentFilter = rawParentFilter;
            }

            // Using this in FQ, will invoke / test the Scorer#advance(..) and also let the Weight#scorer not get live docs as acceptedDocs
            Filter filterMe;
            if (random().nextBoolean()) {
                filterMe = SearchContext.current().filterCache().cache(rawFilterMe);
            } else {
                filterMe = rawFilterMe;
            }

            // Simulate a parent update
            if (random().nextBoolean()) {
                int numberOfUpdates = 1 + random().nextInt(TEST_NIGHTLY ? 25 : 5);
                for (int j = 0; j < numberOfUpdates; j++) {
                    int parentId;
                    do {
                        parentId = random().nextInt(numParentDocs);
                    } while (filteredOrDeletedDocs.contains(parentId));

                    String parentUid = Uid.createUid("parent", Integer.toString(parentId));
                    indexWriter.deleteDocuments(new Term(UidFieldMapper.NAME, parentUid));

                    Document document = new Document();
                    document.add(new StringField(UidFieldMapper.NAME, parentUid, Field.Store.YES));
                    document.add(new StringField(TypeFieldMapper.NAME, "parent", Field.Store.NO));
                    indexWriter.addDocument(document);
                }

                indexReader.close();
                indexReader = DirectoryReader.open(indexWriter.w, true);
                searcher = new IndexSearcher(indexReader);
                engineSearcher = new Engine.SimpleSearcher(
                        ChildrenConstantScoreQueryTests.class.getSimpleName(), searcher
                );
                ((TestSearchContext) SearchContext.current()).setSearcher(new ContextIndexSearcher(SearchContext.current(), engineSearcher));
            }

            String childValue = childValues[random().nextInt(numUniqueChildValues)];
            TermQuery childQuery = new TermQuery(new Term("field1", childValue));
            int shortCircuitParentDocSet = random().nextInt(numParentDocs);
            Filter nonNestedDocsFilter = random().nextBoolean() ? NonNestedDocsFilter.INSTANCE : null;
            Query query;
            if (random().nextBoolean()) {
                // Usage in HasChildQueryParser
                query = new ChildrenConstantScoreQuery(childQuery, "parent", "child", parentFilter, shortCircuitParentDocSet, nonNestedDocsFilter);
            } else {
                // Usage in HasChildFilterParser
                query = new XConstantScoreQuery(
                        new CustomQueryWrappingFilter(
                                new ChildrenConstantScoreQuery(childQuery, "parent", "child", parentFilter, shortCircuitParentDocSet, nonNestedDocsFilter)
                        )
                );
            }
            query = new XFilteredQuery(query, filterMe);
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

            assertBitSet(actualResult, expectedResult, searcher);
        }

        indexWriter.close();
        indexReader.close();
        directory.close();
    }

    static void assertBitSet(FixedBitSet actual, FixedBitSet expected, IndexSearcher searcher) throws IOException {
        if (!actual.equals(expected)) {
            Description description = new StringDescription();
            description.appendText(reason(actual, expected, searcher));
            description.appendText("\nExpected: ");
            description.appendValue(expected);
            description.appendText("\n     got: ");
            description.appendValue(actual);
            description.appendText("\n");
            throw new java.lang.AssertionError(description.toString());
        }
    }

    static String reason(FixedBitSet actual, FixedBitSet expected, IndexSearcher indexSearcher) throws IOException {
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
        Settings settings = ImmutableSettings.EMPTY;
        MapperService mapperService = new MapperService(
                index, settings, new Environment(), new AnalysisService(index), null, null
        );
        mapperService.merge(
                childType, new CompressedString(PutMappingRequest.buildFromSimplifiedDef(childType, "_parent", "type=" + parentType).string()), true
        );
        IndexAliasesService aliasesService = new IndexAliasesService(index, ImmutableSettings.EMPTY, null);
        final IndexService indexService = new SimpleIdCacheTests.StubIndexService(mapperService, aliasesService);
        idCache.setIndexService(indexService);

        ThreadPool threadPool = new ThreadPool();
        NodeSettingsService nodeSettingsService = new NodeSettingsService(settings);
        IndicesFilterCache indicesFilterCache = new IndicesFilterCache(settings, threadPool, cacheRecycler, nodeSettingsService);
        WeightedFilterCache filterCache = new WeightedFilterCache(index, settings, indicesFilterCache);
        return new TestSearchContext(cacheRecycler, idCache, indexService, filterCache);
    }

}
