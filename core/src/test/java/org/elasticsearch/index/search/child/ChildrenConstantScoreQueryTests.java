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

import com.carrotsearch.hppc.IntHashSet;
import com.carrotsearch.hppc.ObjectObjectHashMap;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.*;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryUtils;
import org.apache.lucene.search.QueryWrapperFilter;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.SuppressForbidden;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.common.lucene.search.Queries;
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
import java.util.NavigableSet;
import java.util.Random;
import java.util.TreeSet;

import static org.elasticsearch.index.query.QueryBuilders.constantScoreQuery;
import static org.elasticsearch.index.query.QueryBuilders.filteredQuery;
import static org.elasticsearch.index.query.QueryBuilders.hasChildQuery;
import static org.elasticsearch.index.query.QueryBuilders.notQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.hamcrest.Matchers.equalTo;

@SuppressForbidden(reason="Old p/c queries still use filters")
public class ChildrenConstantScoreQueryTests extends AbstractChildTestCase {

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
        ParentFieldMapper parentFieldMapper = SearchContext.current().mapperService().documentMapper("child").parentFieldMapper();
        ParentChildIndexFieldData parentChildIndexFieldData = SearchContext.current().fieldData().getForField(parentFieldMapper.fieldType());
        Filter parentFilter = new QueryWrapperFilter(new TermQuery(new Term(TypeFieldMapper.NAME, "parent")));
        Query query = new ChildrenConstantScoreQuery(parentChildIndexFieldData, childQuery, "parent", "child", parentFilter, 12, wrapWithBitSetFilter(Queries.newNonNestedFilter()));
        QueryUtils.check(query);
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

        IndexReader indexReader = ElasticsearchDirectoryReader.wrap(DirectoryReader.open(indexWriter.w, false), new ShardId("test", 1));
        IndexSearcher searcher = new IndexSearcher(indexReader);
        ((TestSearchContext) SearchContext.current()).setSearcher(
                new Engine.Searcher(ChildrenConstantScoreQueryTests.class.getSimpleName(), searcher)
        );

        TermQuery childQuery = new TermQuery(new Term("field1", "value" + (1 + random().nextInt(3))));
        Filter parentFilter = new QueryWrapperFilter(new TermQuery(new Term(TypeFieldMapper.NAME, "parent")));
        int shortCircuitParentDocSet = random().nextInt(5);
        ParentFieldMapper parentFieldMapper = SearchContext.current().mapperService().documentMapper("child").parentFieldMapper();
        ParentChildIndexFieldData parentChildIndexFieldData = SearchContext.current().fieldData().getForField(parentFieldMapper.fieldType());
        ChildrenConstantScoreQuery query = new ChildrenConstantScoreQuery(parentChildIndexFieldData, childQuery, "parent", "child", parentFilter, shortCircuitParentDocSet, null);

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

        IntHashSet filteredOrDeletedDocs = new IntHashSet();
        int childDocId = 0;
        int numParentDocs = scaledRandomIntBetween(1, numUniqueChildValues);
        ObjectObjectHashMap<String, NavigableSet<String>> childValueToParentIds = new ObjectObjectHashMap<>();
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

            final int numChildDocs = scaledRandomIntBetween(0, 100);
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
                    NavigableSet<String> parentIds;
                    if (childValueToParentIds.containsKey(childValue)) {
                        parentIds = childValueToParentIds.get(childValue);
                    } else {
                        childValueToParentIds.put(childValue, parentIds = new TreeSet<>());
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
        IndexReader indexReader = ElasticsearchDirectoryReader.wrap(DirectoryReader.open(directory), new ShardId("test", 1));
        IndexSearcher searcher = new IndexSearcher(indexReader);
        Engine.Searcher engineSearcher = new Engine.Searcher(
                ChildrenConstantScoreQueryTests.class.getSimpleName(), searcher
        );
        ((TestSearchContext) SearchContext.current()).setSearcher(engineSearcher);

        int max = numUniqueChildValues / 4;
        for (int i = 0; i < max; i++) {
            // Simulate a parent update
            if (random().nextBoolean()) {
                final int numberOfUpdatableParents = numParentDocs - filteredOrDeletedDocs.size();
                int numberOfUpdates = scaledRandomIntBetween(0, numberOfUpdatableParents);
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
                indexReader = ElasticsearchDirectoryReader.wrap(DirectoryReader.open(indexWriter.w, true), new ShardId("test", 1));
                searcher = new IndexSearcher(indexReader);
                engineSearcher = new Engine.Searcher(
                        ChildrenConstantScoreQueryTests.class.getSimpleName(), searcher
                );
                ((TestSearchContext) SearchContext.current()).setSearcher(engineSearcher);
            }

            String childValue = childValues[random().nextInt(numUniqueChildValues)];
            int shortCircuitParentDocSet = random().nextInt(numParentDocs);
            QueryBuilder queryBuilder;
            if (random().nextBoolean()) {
                queryBuilder = hasChildQuery("child", termQuery("field1", childValue))
                        .setShortCircuitCutoff(shortCircuitParentDocSet);
            } else {
                queryBuilder = constantScoreQuery(
                        hasChildQuery("child", termQuery("field1", childValue))
                                .setShortCircuitCutoff(shortCircuitParentDocSet)
                );
            }
            // Using a FQ, will invoke / test the Scorer#advance(..) and also let the Weight#scorer not get live docs as acceptedDocs
            queryBuilder = filteredQuery(queryBuilder, notQuery(termQuery("filter", "me")));
            Query query = parseQuery(queryBuilder);

            BitSetCollector collector = new BitSetCollector(indexReader.maxDoc());
            searcher.search(query, collector);
            FixedBitSet actualResult = collector.getResult();

            FixedBitSet expectedResult = new FixedBitSet(indexReader.maxDoc());
            if (childValueToParentIds.containsKey(childValue)) {
                LeafReader slowLeafReader = SlowCompositeReaderWrapper.wrap(indexReader);
                Terms terms = slowLeafReader.terms(UidFieldMapper.NAME);
                if (terms != null) {
                    NavigableSet<String> parentIds = childValueToParentIds.get(childValue);
                    TermsEnum termsEnum = terms.iterator();
                    PostingsEnum docsEnum = null;
                    for (String id : parentIds) {
                        TermsEnum.SeekStatus seekStatus = termsEnum.seekCeil(Uid.createUidAsBytes("parent", id));
                        if (seekStatus == TermsEnum.SeekStatus.FOUND) {
                            docsEnum = termsEnum.postings(docsEnum, PostingsEnum.NONE);
                            final Bits liveDocs = slowLeafReader.getLiveDocs();
                            for (int doc = docsEnum.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = docsEnum.nextDoc()) {
                                if (liveDocs == null || liveDocs.get(doc)) {
                                    break;
                                }
                            }
                            expectedResult.set(docsEnum.docID());
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
