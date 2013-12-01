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

import com.carrotsearch.hppc.IntIntOpenHashMap;
import com.carrotsearch.hppc.ObjectObjectOpenHashMap;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.*;
import org.apache.lucene.queries.TermFilter;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.common.lucene.search.NotFilter;
import org.elasticsearch.common.lucene.search.XConstantScoreQuery;
import org.elasticsearch.common.lucene.search.XFilteredQuery;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.mapper.internal.ParentFieldMapper;
import org.elasticsearch.index.mapper.internal.TypeFieldMapper;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;
import org.elasticsearch.search.internal.ContextIndexSearcher;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.test.ElasticsearchLuceneTestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.NavigableSet;
import java.util.TreeSet;

import static org.elasticsearch.index.search.child.ChildrenConstantScoreQueryTests.assertBitSet;
import static org.elasticsearch.index.search.child.ChildrenConstantScoreQueryTests.createSearchContext;

/**
 */
public class ParentConstantScoreQueryTests extends ElasticsearchLuceneTestCase {

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
    public void testRandom() throws Exception {
        Directory directory = newDirectory();
        RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory);
        int numUniqueParentValues = 1 + random().nextInt(TEST_NIGHTLY ? 20000 : 1000);
        String[] parentValues = new String[numUniqueParentValues];
        for (int i = 0; i < numUniqueParentValues; i++) {
            parentValues[i] = Integer.toString(i);
        }

        int childDocId = 0;
        int numParentDocs = 1 + random().nextInt(TEST_NIGHTLY ? 10000 : 1000);
        ObjectObjectOpenHashMap<String, NavigableSet<String>> parentValueToChildDocIds = new ObjectObjectOpenHashMap<String, NavigableSet<String>>();
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

            int numChildDocs;
            if (rarely()) {
                numChildDocs = random().nextInt(TEST_NIGHTLY ? 100 : 25);
            } else {
                numChildDocs = random().nextInt(TEST_NIGHTLY ? 40 : 10);
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
                        parentValueToChildDocIds.put(parentValue, childIds = new TreeSet<String>());
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
        Engine.Searcher engineSearcher = new Engine.SimpleSearcher(
                ParentConstantScoreQuery.class.getSimpleName(), searcher
        );
        ((TestSearchContext) SearchContext.current()).setSearcher(new ContextIndexSearcher(SearchContext.current(), engineSearcher));

        TermFilter rawChildrenFilter = new TermFilter(new Term(TypeFieldMapper.NAME, "child"));
        Filter rawFilterMe = new NotFilter(new TermFilter(new Term("filter", "me")));
        int max = numUniqueParentValues / 4;
        for (int i = 0; i < max; i++) {
            // Randomly pick a cached version: there is specific logic inside ChildrenQuery that deals with the fact
            // that deletes are applied at the top level when filters are cached.
            Filter childrenFilter;
            if (random().nextBoolean()) {
                childrenFilter = SearchContext.current().filterCache().cache(rawChildrenFilter);
            } else {
                childrenFilter = rawChildrenFilter;
            }

            // Using this in FQ, will invoke / test the Scorer#advance(..) and also let the Weight#scorer not get live docs as acceptedDocs
            Filter filterMe;
            if (random().nextBoolean()) {
                filterMe = SearchContext.current().filterCache().cache(rawFilterMe);
            } else {
                filterMe = rawFilterMe;
            }

            // Simulate a child update
            if (random().nextBoolean()) {
                int numberOfUpdates = 1 + random().nextInt(TEST_NIGHTLY ? 25 : 5);
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
                engineSearcher = new Engine.SimpleSearcher(
                        ParentConstantScoreQueryTests.class.getSimpleName(), searcher
                );
                ((TestSearchContext) SearchContext.current()).setSearcher(new ContextIndexSearcher(SearchContext.current(), engineSearcher));
            }

            String parentValue = parentValues[random().nextInt(numUniqueParentValues)];
            TermQuery parentQuery = new TermQuery(new Term("field1", parentValue));
            Query query;
            if (random().nextBoolean()) {
                // Usage in HasParentQueryParser
                query = new ParentConstantScoreQuery(parentQuery, "parent", childrenFilter);
            } else {
                // Usage in HasParentFilterParser
                query = new XConstantScoreQuery(
                        new CustomQueryWrappingFilter(
                                new ParentConstantScoreQuery(parentQuery, "parent", childrenFilter)
                        )
                );
            }
            query = new XFilteredQuery(query, filterMe);
            BitSetCollector collector = new BitSetCollector(indexReader.maxDoc());
            searcher.search(query, collector);
            FixedBitSet actualResult = collector.getResult();

            FixedBitSet expectedResult = new FixedBitSet(indexReader.maxDoc());
            if (parentValueToChildDocIds.containsKey(parentValue)) {
                AtomicReader slowAtomicReader = SlowCompositeReaderWrapper.wrap(indexReader);
                Terms terms = slowAtomicReader.terms(UidFieldMapper.NAME);
                if (terms != null) {
                    NavigableSet<String> childIds = parentValueToChildDocIds.lget();
                    TermsEnum termsEnum = terms.iterator(null);
                    DocsEnum docsEnum = null;
                    for (String id : childIds) {
                        TermsEnum.SeekStatus seekStatus = termsEnum.seekCeil(Uid.createUidAsBytes("child", id));
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

}
