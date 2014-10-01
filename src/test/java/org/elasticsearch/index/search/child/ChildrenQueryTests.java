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
package org.elasticsearch.index.search.child;

import com.carrotsearch.hppc.FloatArrayList;
import com.carrotsearch.hppc.IntOpenHashSet;
import com.carrotsearch.hppc.ObjectObjectOpenHashMap;
import com.carrotsearch.randomizedtesting.generators.RandomInts;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.*;
import org.apache.lucene.queries.TermFilter;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.LuceneTestCase;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.lucene.search.NotFilter;
import org.elasticsearch.common.lucene.search.XFilteredQuery;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.cache.fixedbitset.FixedBitSetFilter;
import org.elasticsearch.index.fielddata.plain.ParentChildIndexFieldData;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.mapper.internal.ParentFieldMapper;
import org.elasticsearch.index.mapper.internal.TypeFieldMapper;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;
import org.elasticsearch.index.search.nested.NonNestedDocsFilter;
import org.elasticsearch.search.internal.ContextIndexSearcher;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.test.TestSearchContext;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Random;
import java.util.TreeMap;

public class ChildrenQueryTests extends AbstractChildTests {

    @BeforeClass
    public static void before() throws IOException {
        forceDefaultCodec();
        SearchContext.setCurrent(createSearchContext("test", "parent", "child"));
    }

    @AfterClass
    public static void after() throws IOException {
        SearchContext current = SearchContext.current();
        SearchContext.removeCurrent();
        Releasables.close(current);
    }

    @Test
    public void testBasicQuerySanities() {
        Query childQuery = new TermQuery(new Term("field", "value"));
        ScoreType scoreType = ScoreType.values()[random().nextInt(ScoreType.values().length)];
        ParentFieldMapper parentFieldMapper = SearchContext.current().mapperService().documentMapper("child").parentFieldMapper();
        ParentChildIndexFieldData parentChildIndexFieldData = SearchContext.current().fieldData().getForField(parentFieldMapper);
        FixedBitSetFilter parentFilter = wrap(new TermFilter(new Term(TypeFieldMapper.NAME, "parent")));
        int minChildren = random().nextInt(10);
        int maxChildren = scaledRandomIntBetween(minChildren, 10);
        Query query = new ChildrenQuery(parentChildIndexFieldData, "parent", "child", parentFilter, childQuery, scoreType, minChildren,
                maxChildren, 12, wrap(NonNestedDocsFilter.INSTANCE));
        QueryUtils.check(query);
    }

    @Test
    public void testRandom() throws Exception {
        Directory directory = newDirectory();
        final Random r = random();
        final IndexWriterConfig iwc = LuceneTestCase.newIndexWriterConfig(r,
                LuceneTestCase.TEST_VERSION_CURRENT, new MockAnalyzer(r))
                .setMaxBufferedDocs(IndexWriterConfig.DISABLE_AUTO_FLUSH)
                .setRAMBufferSizeMB(scaledRandomIntBetween(16, 64)); // we might index a lot - don't go crazy here
        RandomIndexWriter indexWriter = new RandomIndexWriter(r, directory, iwc);
        int numUniqueChildValues = scaledRandomIntBetween(100, 2000);
        String[] childValues = new String[numUniqueChildValues];
        for (int i = 0; i < numUniqueChildValues; i++) {
            childValues[i] = Integer.toString(i);
        }

        IntOpenHashSet filteredOrDeletedDocs = new IntOpenHashSet();

        int childDocId = 0;
        int numParentDocs = scaledRandomIntBetween(1, numUniqueChildValues);
        ObjectObjectOpenHashMap<String, NavigableMap<String, FloatArrayList>> childValueToParentIds = new ObjectObjectOpenHashMap<>();
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

            int numChildDocs = scaledRandomIntBetween(0, 100);
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
                    NavigableMap<String, FloatArrayList> parentIdToChildScores;
                    if (childValueToParentIds.containsKey(childValue)) {
                        parentIdToChildScores = childValueToParentIds.lget();
                    } else {
                        childValueToParentIds.put(childValue, parentIdToChildScores = new TreeMap<>());
                    }
                    if (!markParentAsDeleted && !filterMe) {
                        FloatArrayList childScores = parentIdToChildScores.get(parent);
                        if (childScores == null) {
                            parentIdToChildScores.put(parent, childScores = new FloatArrayList());
                        }
                        childScores.add(1f);
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
                ChildrenQueryTests.class.getSimpleName(), searcher
        );
        ((TestSearchContext) SearchContext.current()).setSearcher(new ContextIndexSearcher(SearchContext.current(), engineSearcher));

        ParentFieldMapper parentFieldMapper = SearchContext.current().mapperService().documentMapper("child").parentFieldMapper();
        ParentChildIndexFieldData parentChildIndexFieldData = SearchContext.current().fieldData().getForField(parentFieldMapper);
        FixedBitSetFilter parentFilter = wrap(new TermFilter(new Term(TypeFieldMapper.NAME, "parent")));
        Filter rawFilterMe = new NotFilter(new TermFilter(new Term("filter", "me")));
        int max = numUniqueChildValues / 4;
        for (int i = 0; i < max; i++) {
            // Using this in FQ, will invoke / test the Scorer#advance(..) and also let the Weight#scorer not get live docs as acceptedDocs
            Filter filterMe;
            if (random().nextBoolean()) {
                filterMe = SearchContext.current().filterCache().cache(rawFilterMe);
            } else {
                filterMe = rawFilterMe;
            }

            // Simulate a parent update
            if (random().nextBoolean()) {
                final int numberOfUpdatableParents = numParentDocs - filteredOrDeletedDocs.size();
                int numberOfUpdates = RandomInts.randomIntBetween(random(), 0, Math.min(numberOfUpdatableParents, TEST_NIGHTLY ? 25 : 5));
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
            Query childQuery = new ConstantScoreQuery(new TermQuery(new Term("field1", childValue)));
            int shortCircuitParentDocSet = random().nextInt(numParentDocs);
            ScoreType scoreType = ScoreType.values()[random().nextInt(ScoreType.values().length)];
            FixedBitSetFilter nonNestedDocsFilter = random().nextBoolean() ? wrap(NonNestedDocsFilter.INSTANCE) : null;

            // leave min/max set to 0 half the time
            int minChildren = random().nextInt(2) * scaledRandomIntBetween(0, 110);
            int maxChildren = random().nextInt(2) * scaledRandomIntBetween(minChildren, 110);

            Query query = new ChildrenQuery(parentChildIndexFieldData, "parent", "child", parentFilter, childQuery, scoreType, minChildren,
                    maxChildren, shortCircuitParentDocSet, nonNestedDocsFilter);
            query = new XFilteredQuery(query, filterMe);
            BitSetCollector collector = new BitSetCollector(indexReader.maxDoc());
            int numHits = 1 + random().nextInt(25);
            TopScoreDocCollector actualTopDocsCollector = TopScoreDocCollector.create(numHits, false);
            searcher.search(query, MultiCollector.wrap(collector, actualTopDocsCollector));
            FixedBitSet actualResult = collector.getResult();

            FixedBitSet expectedResult = new FixedBitSet(indexReader.maxDoc());
            MockScorer mockScorer = new MockScorer(scoreType);
            TopScoreDocCollector expectedTopDocsCollector = TopScoreDocCollector.create(numHits, false);
            expectedTopDocsCollector.setScorer(mockScorer);
            if (childValueToParentIds.containsKey(childValue)) {
                AtomicReader slowAtomicReader = SlowCompositeReaderWrapper.wrap(indexReader);
                Terms terms = slowAtomicReader.terms(UidFieldMapper.NAME);
                if (terms != null) {
                    NavigableMap<String, FloatArrayList> parentIdToChildScores = childValueToParentIds.lget();
                    TermsEnum termsEnum = terms.iterator(null);
                    DocsEnum docsEnum = null;
                    for (Map.Entry<String, FloatArrayList> entry : parentIdToChildScores.entrySet()) {
                        int count = entry.getValue().elementsCount;
                        if (count >= minChildren && (maxChildren == 0 || count <= maxChildren)) {
                            TermsEnum.SeekStatus seekStatus = termsEnum.seekCeil(Uid.createUidAsBytes("parent", entry.getKey()));
                            if (seekStatus == TermsEnum.SeekStatus.FOUND) {
                                docsEnum = termsEnum.docs(slowAtomicReader.getLiveDocs(), docsEnum, DocsEnum.FLAG_NONE);
                                expectedResult.set(docsEnum.nextDoc());
                                mockScorer.scores = entry.getValue();
                                expectedTopDocsCollector.collect(docsEnum.docID());
                            } else if (seekStatus == TermsEnum.SeekStatus.END) {
                                break;
                            }
                        }
                    }
                }
            }

            assertBitSet(actualResult, expectedResult, searcher);
            assertTopDocs(actualTopDocsCollector.topDocs(), expectedTopDocsCollector.topDocs());
        }

        indexWriter.close();
        indexReader.close();
        directory.close();
    }

}
