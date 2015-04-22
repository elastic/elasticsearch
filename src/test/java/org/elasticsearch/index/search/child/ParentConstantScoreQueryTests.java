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

import com.carrotsearch.hppc.IntIntOpenHashMap;
import com.carrotsearch.hppc.ObjectObjectOpenHashMap;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.SlowCompositeReaderWrapper;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryUtils;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.join.BitDocIdSetFilter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.LuceneTestCase;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.fielddata.plain.ParentChildIndexFieldData;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.mapper.internal.ParentFieldMapper;
import org.elasticsearch.index.mapper.internal.TypeFieldMapper;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.internal.ContextIndexSearcher;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.test.TestSearchContext;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.NavigableSet;
import java.util.Random;
import java.util.TreeSet;

import static org.elasticsearch.index.query.FilterBuilders.hasParentFilter;
import static org.elasticsearch.index.query.FilterBuilders.notFilter;
import static org.elasticsearch.index.query.FilterBuilders.termFilter;
import static org.elasticsearch.index.query.QueryBuilders.constantScoreQuery;
import static org.elasticsearch.index.query.QueryBuilders.filteredQuery;
import static org.elasticsearch.index.query.QueryBuilders.hasParentQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;

/**
 */
public class ParentConstantScoreQueryTests extends AbstractChildTests {

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
        ParentChildIndexFieldData parentChildIndexFieldData = SearchContext.current().fieldData().getForField(parentFieldMapper);
        BitDocIdSetFilter childrenFilter = wrapWithBitSetFilter(Queries.wrap(new TermQuery(new Term(TypeFieldMapper.NAME, "child"))));
        Query query = new ParentConstantScoreQuery(parentChildIndexFieldData, parentQuery, "parent", childrenFilter);
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
        ObjectObjectOpenHashMap<String, NavigableSet<String>> parentValueToChildDocIds = new ObjectObjectOpenHashMap<>();
        IntIntOpenHashMap childIdToParentId = new IntIntOpenHashMap();
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
                boolean markChildAsDeleted = rarely();
                boolean filterMe = rarely();
                String child = Integer.toString(childDocId++);

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
                    NavigableSet<String> childIds;
                    if (parentValueToChildDocIds.containsKey(parentValue)) {
                        childIds = parentValueToChildDocIds.lget();
                    } else {
                        parentValueToChildDocIds.put(parentValue, childIds = new TreeSet<>());
                    }
                    if (!markChildAsDeleted && !filterMe) {
                        childIdToParentId.put(Integer.valueOf(child), parentDocId);
                        childIds.add(child);
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
                ParentConstantScoreQuery.class.getSimpleName(), searcher
        );
        ((TestSearchContext) SearchContext.current()).setSearcher(new ContextIndexSearcher(SearchContext.current(), engineSearcher));

        int max = numUniqueParentValues / 4;
        for (int i = 0; i < max; i++) {
            // Simulate a child update
            if (random().nextBoolean()) {
                int numberOfUpdates = childIdToParentId.isEmpty() ? 0 : scaledRandomIntBetween(1, 25);
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
                indexReader = DirectoryReader.open(indexWriter.w, true);
                searcher = new IndexSearcher(indexReader);
                engineSearcher = new Engine.Searcher(
                        ParentConstantScoreQueryTests.class.getSimpleName(), searcher
                );
                ((TestSearchContext) SearchContext.current()).setSearcher(new ContextIndexSearcher(SearchContext.current(), engineSearcher));
            }

            String parentValue = parentValues[random().nextInt(numUniqueParentValues)];
            QueryBuilder queryBuilder;
            if (random().nextBoolean()) {
                queryBuilder = hasParentQuery("parent", termQuery("field1", parentValue));
            } else {
                queryBuilder = constantScoreQuery(hasParentFilter("parent", termFilter("field1", parentValue)));
            }
            // Using a FQ, will invoke / test the Scorer#advance(..) and also let the Weight#scorer not get live docs as acceptedDocs
            queryBuilder = filteredQuery(queryBuilder, notFilter(termFilter("filter", "me")));
            Query query = parseQuery(queryBuilder);
            BitSetCollector collector = new BitSetCollector(indexReader.maxDoc());
            searcher.search(query, collector);
            FixedBitSet actualResult = collector.getResult();

            FixedBitSet expectedResult = new FixedBitSet(indexReader.maxDoc());
            if (parentValueToChildDocIds.containsKey(parentValue)) {
                LeafReader slowLeafReader = SlowCompositeReaderWrapper.wrap(indexReader);
                Terms terms = slowLeafReader.terms(UidFieldMapper.NAME);
                if (terms != null) {
                    NavigableSet<String> childIds = parentValueToChildDocIds.lget();
                    TermsEnum termsEnum = terms.iterator();
                    PostingsEnum docsEnum = null;
                    for (String id : childIds) {
                        TermsEnum.SeekStatus seekStatus = termsEnum.seekCeil(Uid.createUidAsBytes("child", id));
                        if (seekStatus == TermsEnum.SeekStatus.FOUND) {
                            docsEnum = termsEnum.postings(slowLeafReader.getLiveDocs(), docsEnum, PostingsEnum.NONE);
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

}
