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
import com.carrotsearch.hppc.IntIntHashMap;
import com.carrotsearch.hppc.ObjectObjectHashMap;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.SuppressForbidden;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.fielddata.plain.ParentChildIndexFieldData;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.mapper.internal.ParentFieldMapper;
import org.elasticsearch.index.mapper.internal.TypeFieldMapper;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.shard.ShardId;
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

import static org.elasticsearch.index.query.QueryBuilders.*;

@SuppressForbidden(reason="Old p/c queries still use filters")
public class ParentQueryTests extends AbstractChildTestCase {

    @BeforeClass
    public static void before() throws IOException {
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
        Query parentQuery = new TermQuery(new Term("field", "value"));
        ParentFieldMapper parentFieldMapper = SearchContext.current().mapperService().documentMapper("child").parentFieldMapper();
        ParentChildIndexFieldData parentChildIndexFieldData = SearchContext.current().fieldData().getForField(parentFieldMapper.fieldType());
        Filter childrenFilter = new QueryWrapperFilter(new TermQuery(new Term(TypeFieldMapper.NAME, "child")));
        Query query = new ParentQuery(parentChildIndexFieldData, parentQuery, "parent", childrenFilter);
        QueryUtils.check(query);
    }

    @Test
    public void testRandom() throws Exception {
        Directory directory = newDirectory();
        final Random r = random();
        final IndexWriterConfig iwc = LuceneTestCase.newIndexWriterConfig(r, new MockAnalyzer(r))
                .setMaxBufferedDocs(IndexWriterConfig.DISABLE_AUTO_FLUSH)
                .setRAMBufferSizeMB(scaledRandomIntBetween(16, 64)); // we might index a lot - don't go crazy here
        RandomIndexWriter indexWriter = new RandomIndexWriter(r, directory, iwc);
        int numUniqueParentValues = scaledRandomIntBetween(100, 2000);
        String[] parentValues = new String[numUniqueParentValues];
        for (int i = 0; i < numUniqueParentValues; i++) {
            parentValues[i] = Integer.toString(i);
        }

        int childDocId = 0;
        int numParentDocs = scaledRandomIntBetween(1, numUniqueParentValues);
        ObjectObjectHashMap<String, NavigableMap<String, Float>> parentValueToChildIds = new ObjectObjectHashMap<>();
        IntIntHashMap childIdToParentId = new IntIntHashMap();
        for (int parentDocId = 0; parentDocId < numParentDocs; parentDocId++) {
            boolean markParentAsDeleted = rarely();
            String parentValue = parentValues[random().nextInt(parentValues.length)];
            String parent = Integer.toString(parentDocId);
            Document document = new Document();
            document.add(new StringField(UidFieldMapper.NAME, Uid.createUid("parent", parent), Field.Store.NO));
            document.add(new StringField(TypeFieldMapper.NAME, "parent", Field.Store.NO));
            document.add(new StringField("field1", parentValue, Field.Store.NO));
            if (markParentAsDeleted) {
                document.add(new StringField("delete", "me", Field.Store.NO));
            }
            indexWriter.addDocument(document);

            int numChildDocs = scaledRandomIntBetween(0, 100);
            if (parentDocId == numParentDocs - 1 && childIdToParentId.isEmpty()) {
                // ensure there is at least one child in the index
                numChildDocs = Math.max(1, numChildDocs);
            }
            for (int i = 0; i < numChildDocs; i++) {
                String child = Integer.toString(childDocId++);
                boolean markChildAsDeleted = rarely();
                boolean filterMe = rarely();
                document = new Document();
                document.add(new StringField(UidFieldMapper.NAME, Uid.createUid("child", child), Field.Store.YES));
                document.add(new StringField(TypeFieldMapper.NAME, "child", Field.Store.NO));
                document.add(new StringField(ParentFieldMapper.NAME, Uid.createUid("parent", parent), Field.Store.NO));
                if (markChildAsDeleted) {
                    document.add(new StringField("delete", "me", Field.Store.NO));
                }
                if (filterMe) {
                    document.add(new StringField("filter", "me", Field.Store.NO));
                }
                indexWriter.addDocument(document);

                if (!markParentAsDeleted) {
                    NavigableMap<String, Float> childIdToScore = parentValueToChildIds.getOrDefault(parentValue, null);
                    if (childIdToScore == null) {
                        parentValueToChildIds.put(parentValue, childIdToScore = new TreeMap<>());
                    }
                    if (!markChildAsDeleted && !filterMe) {
                        assertFalse("child ["+ child + "] already has a score", childIdToScore.containsKey(child));
                        childIdToScore.put(child, 1f);
                        childIdToParentId.put(Integer.valueOf(child), parentDocId);
                    }
                }
            }
        }

        // Delete docs that are marked to be deleted.
        indexWriter.deleteDocuments(new Term("delete", "me"));
        indexWriter.commit();

        IndexReader indexReader = ElasticsearchDirectoryReader.wrap(DirectoryReader.open(directory), new ShardId("foo", 1));
        IndexSearcher searcher = new IndexSearcher(indexReader);
        Engine.Searcher engineSearcher = new Engine.Searcher(
                ParentQueryTests.class.getSimpleName(), searcher
        );
        ((TestSearchContext) SearchContext.current()).setSearcher(engineSearcher);

        int max = numUniqueParentValues / 4;
        for (int i = 0; i < max; i++) {
            // Simulate a child update
            if (random().nextBoolean()) {
                int numberOfUpdates = childIdToParentId.isEmpty() ? 0 : scaledRandomIntBetween(1, 5);
                int[] childIds = childIdToParentId.keys().toArray();
                for (int j = 0; j < numberOfUpdates; j++) {
                    int childId = childIds[random().nextInt(childIds.length)];
                    String childUid = Uid.createUid("child", Integer.toString(childId));
                    indexWriter.deleteDocuments(new Term(UidFieldMapper.NAME, childUid));

                    Document document = new Document();
                    document.add(new StringField(UidFieldMapper.NAME, childUid, Field.Store.YES));
                    document.add(new StringField(TypeFieldMapper.NAME, "child", Field.Store.NO));
                    String parentUid = Uid.createUid("parent", Integer.toString(childIdToParentId.get(childId)));
                    document.add(new StringField(ParentFieldMapper.NAME, parentUid, Field.Store.NO));
                    indexWriter.addDocument(document);
                }

                indexReader.close();
                indexReader = ElasticsearchDirectoryReader.wrap(DirectoryReader.open(indexWriter.w, true), new ShardId("foo", 1));
                searcher = new IndexSearcher(indexReader);
                engineSearcher = new Engine.Searcher(
                        ParentConstantScoreQueryTests.class.getSimpleName(), searcher
                );
                ((TestSearchContext) SearchContext.current()).setSearcher(engineSearcher);
            }

            String parentValue = parentValues[random().nextInt(numUniqueParentValues)];
            QueryBuilder queryBuilder = hasParentQuery("parent", constantScoreQuery(termQuery("field1", parentValue)));
            // Using a FQ, will invoke / test the Scorer#advance(..) and also let the Weight#scorer not get live docs as acceptedDocs
            queryBuilder = filteredQuery(queryBuilder, notQuery(termQuery("filter", "me")));
            Query query = parseQuery(queryBuilder);

            BitSetCollector collector = new BitSetCollector(indexReader.maxDoc());
            int numHits = 1 + random().nextInt(25);
            TopScoreDocCollector actualTopDocsCollector = TopScoreDocCollector.create(numHits);
            searcher.search(query, MultiCollector.wrap(collector, actualTopDocsCollector));
            FixedBitSet actualResult = collector.getResult();

            FixedBitSet expectedResult = new FixedBitSet(indexReader.maxDoc());
            TopScoreDocCollector expectedTopDocsCollector = TopScoreDocCollector.create(numHits);
            if (parentValueToChildIds.containsKey(parentValue)) {
                LeafReader slowLeafReader = SlowCompositeReaderWrapper.wrap(indexReader);
                final FloatArrayList[] scores = new FloatArrayList[slowLeafReader.maxDoc()];
                Terms terms = slowLeafReader.terms(UidFieldMapper.NAME);
                if (terms != null) {
                    NavigableMap<String, Float> childIdsAndScore = parentValueToChildIds.get(parentValue);
                    TermsEnum termsEnum = terms.iterator();
                    PostingsEnum docsEnum = null;
                    for (Map.Entry<String, Float> entry : childIdsAndScore.entrySet()) {
                        TermsEnum.SeekStatus seekStatus = termsEnum.seekCeil(Uid.createUidAsBytes("child", entry.getKey()));
                        if (seekStatus == TermsEnum.SeekStatus.FOUND) {
                            docsEnum = termsEnum.postings(docsEnum, PostingsEnum.NONE);
                            final Bits liveDocs = slowLeafReader.getLiveDocs();
                            for (int doc = docsEnum.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = docsEnum.nextDoc()) {
                                if (liveDocs == null || liveDocs.get(doc)) {
                                    break;
                                }
                            }
                            expectedResult.set(docsEnum.docID());
                            FloatArrayList s = scores[docsEnum.docID()];
                            if (s == null) {
                                scores[docsEnum.docID()] = s = new FloatArrayList(2);
                            }
                            s.add(entry.getValue());
                        } else if (seekStatus == TermsEnum.SeekStatus.END) {
                            break;
                        }
                    }
                }
                MockScorer mockScorer = new MockScorer(ScoreType.MAX);
                mockScorer.scores = new FloatArrayList();
                final LeafCollector leafCollector = expectedTopDocsCollector.getLeafCollector(slowLeafReader.getContext());
                leafCollector.setScorer(mockScorer);
                for (int doc = expectedResult.nextSetBit(0); doc < slowLeafReader.maxDoc(); doc = doc + 1 >= expectedResult.length() ? DocIdSetIterator.NO_MORE_DOCS : expectedResult.nextSetBit(doc + 1)) {
                    mockScorer.scores.clear();
                    mockScorer.scores.addAll(scores[doc]);
                    leafCollector.collect(doc);
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
