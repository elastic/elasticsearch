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

package org.elasticsearch.search.slice;

import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.QueryUtils;

import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;

public class TermsSliceQueryTests extends ESTestCase {

    public void testBasics() {
        TermsSliceQuery query1 =
            new TermsSliceQuery("field1", 1, 10);
        TermsSliceQuery query2 =
            new TermsSliceQuery("field1", 1, 10);
        TermsSliceQuery query3 =
            new TermsSliceQuery("field2", 1, 10);
        TermsSliceQuery query4 =
            new TermsSliceQuery("field1", 2, 10);
        QueryUtils.check(query1);
        QueryUtils.checkEqual(query1, query2);
        QueryUtils.checkUnequal(query1, query3);
        QueryUtils.checkUnequal(query1, query4);
    }

    public void testSearch() throws Exception {
        final int numDocs = randomIntBetween(100, 200);
        final Directory dir = newDirectory();
        final RandomIndexWriter w = new RandomIndexWriter(random(), dir, new KeywordAnalyzer());
        int max = randomIntBetween(2, 10);
        int[] sliceCounters = new int[max];
        Set<String> keys = new HashSet<>();
        for (int i = 0; i < numDocs; ++i) {
            Document doc = new Document();
            String uuid = UUIDs.base64UUID();
            BytesRef br = new BytesRef(uuid);
            int id = Math.floorMod(br.hashCode(), max);
            sliceCounters[id] ++;
            doc.add(new StringField("uuid", uuid, Field.Store.YES));
            w.addDocument(doc);
            keys.add(uuid);
        }
        final IndexReader reader = w.getReader();
        final IndexSearcher searcher = newSearcher(reader);

        for (int id = 0; id < max; id++) {
            TermsSliceQuery query1 =
                new TermsSliceQuery("uuid", id, max);
            assertThat(searcher.count(query1), equalTo(sliceCounters[id]));
            searcher.search(query1, new Collector() {
                @Override
                public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
                    return new LeafCollector() {
                        @Override
                        public void setScorer(Scorer scorer) throws IOException {
                        }

                        @Override
                        public void collect(int doc) throws IOException {
                            Document d = context.reader().document(doc, Collections.singleton("uuid"));
                            String uuid = d.get("uuid");
                            assertThat(keys.contains(uuid), equalTo(true));
                            keys.remove(uuid);
                        }
                    };
                }

                @Override
                public boolean needsScores() {
                    return false;
                }
            });
        }
        assertThat(keys.size(), equalTo(0));
        w.close();
        reader.close();
        dir.close();
    }
}
