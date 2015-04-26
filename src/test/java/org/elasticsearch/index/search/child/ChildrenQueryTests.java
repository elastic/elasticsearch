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
import org.apache.lucene.document.DoubleField;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.SlowCompositeReaderWrapper;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.MultiCollector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryUtils;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.search.join.BitDocIdSetFilter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.LuceneTestCase;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.fielddata.plain.ParentChildIndexFieldData;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.mapper.internal.IdFieldMapper;
import org.elasticsearch.index.mapper.internal.ParentFieldMapper;
import org.elasticsearch.index.mapper.internal.TypeFieldMapper;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.functionscore.fieldvaluefactor.FieldValueFactorFunctionBuilder;
import org.elasticsearch.search.internal.ContextIndexSearcher;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.test.TestSearchContext;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Random;
import java.util.TreeMap;

import static org.elasticsearch.index.query.FilterBuilders.notFilter;
import static org.elasticsearch.index.query.FilterBuilders.termFilter;
import static org.elasticsearch.index.query.FilterBuilders.typeFilter;
import static org.elasticsearch.index.query.QueryBuilders.constantScoreQuery;
import static org.elasticsearch.index.query.QueryBuilders.filteredQuery;
import static org.elasticsearch.index.query.QueryBuilders.functionScoreQuery;
import static org.elasticsearch.index.query.QueryBuilders.hasChildQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class ChildrenQueryTests extends AbstractChildTests {

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
        Query childQuery = new TermQuery(new Term("field", "value"));
        ScoreType scoreType = ScoreType.values()[random().nextInt(ScoreType.values().length)];
        ParentFieldMapper parentFieldMapper = SearchContext.current().mapperService().documentMapper("child").parentFieldMapper();
        ParentChildIndexFieldData parentChildIndexFieldData = SearchContext.current().fieldData().getForField(parentFieldMapper);
        BitDocIdSetFilter parentFilter = wrapWithBitSetFilter(Queries.wrap(new TermQuery(new Term(TypeFieldMapper.NAME, "parent"))));
        int minChildren = random().nextInt(10);
        int maxChildren = scaledRandomIntBetween(minChildren, 10);
        Query query = new ChildrenQuery(parentChildIndexFieldData, "parent", "child", parentFilter, childQuery, scoreType, minChildren,
                maxChildren, 12, wrapWithBitSetFilter(Queries.newNonNestedFilter()));
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
                document.add(new StringField(UidFieldMapper.NAME, Uid.createUid("child", Integer.toString(childDocId++)), Field.Store.NO));
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
        Engine.Searcher engineSearcher = new Engine.Searcher(
                ChildrenQueryTests.class.getSimpleName(), searcher
        );
        ((TestSearchContext) SearchContext.current()).setSearcher(new ContextIndexSearcher(SearchContext.current(), engineSearcher));

        int max = numUniqueChildValues / 4;
        for (int i = 0; i < max; i++) {
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
                engineSearcher = new Engine.Searcher(
                        ChildrenConstantScoreQueryTests.class.getSimpleName(), searcher
                );
                ((TestSearchContext) SearchContext.current()).setSearcher(new ContextIndexSearcher(SearchContext.current(), engineSearcher));
            }

            String childValue = childValues[random().nextInt(numUniqueChildValues)];
            int shortCircuitParentDocSet = random().nextInt(numParentDocs);
            ScoreType scoreType = ScoreType.values()[random().nextInt(ScoreType.values().length)];
            // leave min/max set to 0 half the time
            int minChildren = random().nextInt(2) * scaledRandomIntBetween(0, 110);
            int maxChildren = random().nextInt(2) * scaledRandomIntBetween(minChildren, 110);

            QueryBuilder queryBuilder = hasChildQuery("child", constantScoreQuery(termQuery("field1", childValue)))
                    .scoreType(scoreType.name().toLowerCase(Locale.ENGLISH))
                    .minChildren(minChildren)
                    .maxChildren(maxChildren)
                    .setShortCircuitCutoff(shortCircuitParentDocSet);
            // Using a FQ, will invoke / test the Scorer#advance(..) and also let the Weight#scorer not get live docs as acceptedDocs
            queryBuilder = filteredQuery(queryBuilder, notFilter(termFilter("filter", "me")));
            Query query = parseQuery(queryBuilder);
            BitSetCollector collector = new BitSetCollector(indexReader.maxDoc());
            int numHits = 1 + random().nextInt(25);
            TopScoreDocCollector actualTopDocsCollector = TopScoreDocCollector.create(numHits);
            searcher.search(query, MultiCollector.wrap(collector, actualTopDocsCollector));
            FixedBitSet actualResult = collector.getResult();

            FixedBitSet expectedResult = new FixedBitSet(indexReader.maxDoc());
            TopScoreDocCollector expectedTopDocsCollector = TopScoreDocCollector.create(numHits);
            if (childValueToParentIds.containsKey(childValue)) {
                LeafReader slowLeafReader = SlowCompositeReaderWrapper.wrap(indexReader);
                final FloatArrayList[] scores = new FloatArrayList[slowLeafReader.maxDoc()];
                Terms terms = slowLeafReader.terms(UidFieldMapper.NAME);
                if (terms != null) {
                    NavigableMap<String, FloatArrayList> parentIdToChildScores = childValueToParentIds.lget();
                    TermsEnum termsEnum = terms.iterator();
                    PostingsEnum docsEnum = null;
                    for (Map.Entry<String, FloatArrayList> entry : parentIdToChildScores.entrySet()) {
                        int count = entry.getValue().elementsCount;
                        if (count >= minChildren && (maxChildren == 0 || count <= maxChildren)) {
                            TermsEnum.SeekStatus seekStatus = termsEnum.seekCeil(Uid.createUidAsBytes("parent", entry.getKey()));
                            if (seekStatus == TermsEnum.SeekStatus.FOUND) {
                                docsEnum = termsEnum.postings(slowLeafReader.getLiveDocs(), docsEnum, PostingsEnum.NONE);
                                expectedResult.set(docsEnum.nextDoc());
                                scores[docsEnum.docID()] = new FloatArrayList(entry.getValue());
                            } else if (seekStatus == TermsEnum.SeekStatus.END) {
                                break;
                            }
                        }
                    }
                }
                MockScorer mockScorer = new MockScorer(scoreType);
                final LeafCollector leafCollector = expectedTopDocsCollector.getLeafCollector(slowLeafReader.getContext());
                leafCollector.setScorer(mockScorer);
                for (int doc = expectedResult.nextSetBit(0); doc < slowLeafReader.maxDoc(); doc = doc + 1 >= expectedResult.length() ? DocIdSetIterator.NO_MORE_DOCS : expectedResult.nextSetBit(doc + 1)) {
                    mockScorer.scores = scores[doc];
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

    @Test
    public void testMinScoreMode() throws IOException {
        assertScoreType(ScoreType.MIN);
    }

    @Test
    public void testMaxScoreMode() throws IOException {
        assertScoreType(ScoreType.MAX);
    }

    @Test
    public void testAvgScoreMode() throws IOException {
        assertScoreType(ScoreType.AVG);
    }

    @Test
    public void testSumScoreMode() throws IOException {
        assertScoreType(ScoreType.SUM);
    }

    /**
     * Assert that the {@code scoreType} operates as expected and parents are found in the expected order.
     * <p />
     * This will use the test index's parent/child types to create parents with multiple children. Each child will have
     * a randomly generated scored stored in {@link #CHILD_SCORE_NAME}, which is used to score based on the
     * {@code scoreType} by using a {@link MockScorer} to determine the expected scores.
     * @param scoreType The score type to use within the query to score parents relative to their children.
     * @throws IOException if any unexpected error occurs
     */
    private void assertScoreType(ScoreType scoreType) throws IOException {
        SearchContext context = SearchContext.current();
        Directory directory = newDirectory();
        IndexWriter writer = new IndexWriter(directory, newIndexWriterConfig(new MockAnalyzer(random())));

        // calculates the expected score per parent
        MockScorer scorer = new MockScorer(scoreType);
        scorer.scores = new FloatArrayList(10);

        // number of parents to generate
        int parentDocs = scaledRandomIntBetween(2, 10);
        // unique child ID
        int childDocId = 0;

        // Parent ID to expected score
        Map<String, Float> parentScores = new TreeMap<>();

        // Add a few random parents to ensure that the children's score is appropriately taken into account
        for (int parentDocId = 0; parentDocId < parentDocs; ++parentDocId) {
            String parent = Integer.toString(parentDocId);

            // Create the parent
            Document parentDocument = new Document();

            parentDocument.add(new StringField(UidFieldMapper.NAME, Uid.createUid("parent", parent), Field.Store.YES));
            parentDocument.add(new StringField(IdFieldMapper.NAME, parent, Field.Store.YES));
            parentDocument.add(new StringField(TypeFieldMapper.NAME, "parent", Field.Store.NO));

            // add the parent to the index
            writer.addDocument(parentDocument);

            int numChildDocs = scaledRandomIntBetween(1, 10);

            // forget any parent's previous scores
            scorer.scores.clear();

            // associate children with the parent
            for (int i = 0; i < numChildDocs; ++i) {
                int childScore = random().nextInt(128);

                Document childDocument = new Document();

                childDocument.add(new StringField(UidFieldMapper.NAME, Uid.createUid("child", Integer.toString(childDocId++)), Field.Store.NO));
                childDocument.add(new StringField(TypeFieldMapper.NAME, "child", Field.Store.NO));
                // parent association:
                childDocument.add(new StringField(ParentFieldMapper.NAME, Uid.createUid("parent", parent), Field.Store.NO));
                childDocument.add(new DoubleField(CHILD_SCORE_NAME, childScore, Field.Store.NO));

                // remember the score to be calculated
                scorer.scores.add(childScore);

                // add the associated child to the index
                writer.addDocument(childDocument);
            }

            // this score that should be returned for this parent
            parentScores.put(parent, scorer.score());
        }

        writer.commit();

        IndexReader reader = DirectoryReader.open(writer, true);
        IndexSearcher searcher = new IndexSearcher(reader);

        // setup to read the parent/child map
        Engine.Searcher engineSearcher = new Engine.Searcher(ChildrenQueryTests.class.getSimpleName(), searcher);
        ((TestSearchContext)context).setSearcher(new ContextIndexSearcher(context, engineSearcher));

        // child query that returns the score as the value of "childScore" for each child document, with the parent's score determined by the score type
        QueryBuilder childQueryBuilder = functionScoreQuery(typeFilter("child")).add(new FieldValueFactorFunctionBuilder(CHILD_SCORE_NAME));
        QueryBuilder queryBuilder = hasChildQuery("child", childQueryBuilder)
                .scoreType(scoreType.name().toLowerCase(Locale.ENGLISH))
                .setShortCircuitCutoff(parentDocs);

        // Perform the search for the documents using the selected score type
        TopDocs docs = searcher.search(parseQuery(queryBuilder), parentDocs);
        assertThat("Expected all parents", docs.totalHits, is(parentDocs));

        // score should be descending (just a sanity check)
        float topScore = docs.scoreDocs[0].score;

        // ensure each score is returned as expected
        for (int i = 0; i < parentDocs; ++i) {
            ScoreDoc scoreDoc = docs.scoreDocs[i];
            // get the ID from the document to get its expected score; remove it so we cannot double-count it
            float score = parentScores.remove(reader.document(scoreDoc.doc).get(IdFieldMapper.NAME));

            // expect exact match
            assertThat("Unexpected score", scoreDoc.score, is(score));
            assertThat("Not descending", score, lessThanOrEqualTo(topScore));

            // it had better keep descending
            topScore = score;
        }

        reader.close();
        writer.close();
        directory.close();
    }
}
