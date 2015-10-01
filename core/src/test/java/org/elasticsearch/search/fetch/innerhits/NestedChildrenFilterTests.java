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

package org.elasticsearch.search.fetch.innerhits;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.QueryWrapperFilter;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TotalHitCountCollector;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.search.join.QueryBitSetProducer;
import org.apache.lucene.store.Directory;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.fetch.innerhits.InnerHitsContext.NestedInnerHits.NestedChildrenQuery;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

/**
 */
public class NestedChildrenFilterTests extends ESTestCase {

    @Test
    public void testNestedChildrenFilter() throws Exception {
        int numParentDocs = scaledRandomIntBetween(0, 32);
        int maxChildDocsPerParent = scaledRandomIntBetween(8, 16);

        Directory dir = newDirectory();
        RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
        for (int i = 0; i < numParentDocs; i++) {
            int numChildDocs = scaledRandomIntBetween(0, maxChildDocsPerParent);
            List<Document> docs = new ArrayList<>(numChildDocs + 1);
            for (int j = 0; j < numChildDocs; j++) {
                Document childDoc = new Document();
                childDoc.add(new StringField("type", "child", Field.Store.NO));
                docs.add(childDoc);
            }

            Document parenDoc = new Document();
            parenDoc.add(new StringField("type", "parent", Field.Store.NO));
            parenDoc.add(new IntField("num_child_docs", numChildDocs, Field.Store.YES));
            docs.add(parenDoc);
            writer.addDocuments(docs);
        }

        IndexReader reader = writer.getReader();
        writer.close();

        IndexSearcher searcher = new IndexSearcher(reader);
        FetchSubPhase.HitContext hitContext = new FetchSubPhase.HitContext();
        BitSetProducer parentFilter = new QueryBitSetProducer(new TermQuery(new Term("type", "parent")));
        Filter childFilter = new QueryWrapperFilter(new TermQuery(new Term("type", "child")));
        int checkedParents = 0;
        for (LeafReaderContext leaf : reader.leaves()) {
            DocIdSetIterator parents = new QueryWrapperFilter(new TermQuery(new Term("type", "parent"))).getDocIdSet(leaf, null).iterator();
            for (int parentDoc = parents.nextDoc(); parentDoc != DocIdSetIterator.NO_MORE_DOCS ; parentDoc = parents.nextDoc()) {
                int expectedChildDocs = leaf.reader().document(parentDoc).getField("num_child_docs").numericValue().intValue();
                hitContext.reset(null, leaf, parentDoc, searcher);
                NestedChildrenQuery nestedChildrenFilter = new NestedChildrenQuery(parentFilter, childFilter, hitContext);
                TotalHitCountCollector totalHitCountCollector = new TotalHitCountCollector();
                searcher.search(new ConstantScoreQuery(nestedChildrenFilter), totalHitCountCollector);
                assertThat(totalHitCountCollector.getTotalHits(), equalTo(expectedChildDocs));
                checkedParents++;
            }
        }
        assertThat(checkedParents, equalTo(numParentDocs));
        reader.close();
        dir.close();
    }

}
