/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.slice;

import com.carrotsearch.hppc.BitMixer;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.search.QueryUtils;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;

public class DocValuesSliceQueryTests extends ESTestCase {

    public void testBasics() {
        DocValuesSliceQuery query1 = new DocValuesSliceQuery("field1", 1, 10);
        DocValuesSliceQuery query2 = new DocValuesSliceQuery("field1", 1, 10);
        DocValuesSliceQuery query3 = new DocValuesSliceQuery("field2", 1, 10);
        DocValuesSliceQuery query4 = new DocValuesSliceQuery("field1", 2, 10);
        QueryUtils.check(query1);
        QueryUtils.checkEqual(query1, query2);
        QueryUtils.checkUnequal(query1, query3);
        QueryUtils.checkUnequal(query1, query4);
    }

    public void testSearch() throws Exception {
        final int numDocs = randomIntBetween(100, 200);
        final Directory dir = newDirectory();
        final RandomIndexWriter w = new RandomIndexWriter(random(), dir);
        int max = randomIntBetween(2, 10);
        int[] sliceCounters1 = new int[max];
        int[] sliceCounters2 = new int[max];
        Set<String> keys = new HashSet<>();
        for (int i = 0; i < numDocs; ++i) {
            Document doc = new Document();
            String uuid = UUIDs.base64UUID();
            int intValue = randomInt();
            long doubleValue = NumericUtils.doubleToSortableLong(randomDouble());
            doc.add(new StringField("uuid", uuid, Field.Store.YES));
            doc.add(new SortedNumericDocValuesField("intField", intValue));
            doc.add(new SortedNumericDocValuesField("doubleField", doubleValue));
            w.addDocument(doc);
            sliceCounters1[Math.floorMod(BitMixer.mix((long) intValue), max)]++;
            sliceCounters2[Math.floorMod(BitMixer.mix(doubleValue), max)]++;
            keys.add(uuid);
        }
        final IndexReader reader = w.getReader();
        final IndexSearcher searcher = newSearcher(reader);

        for (int id = 0; id < max; id++) {
            DocValuesSliceQuery query1 = new DocValuesSliceQuery("intField", id, max);
            assertThat(searcher.count(query1), equalTo(sliceCounters1[id]));

            DocValuesSliceQuery query2 = new DocValuesSliceQuery("doubleField", id, max);
            assertThat(searcher.count(query2), equalTo(sliceCounters2[id]));
            searcher.search(query1, new Collector() {
                @Override
                public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
                    return new LeafCollector() {
                        @Override
                        public void setScorer(Scorable scorer) throws IOException {}

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
                public ScoreMode scoreMode() {
                    return ScoreMode.COMPLETE_NO_SCORES;
                }
            });
        }
        assertThat(keys.size(), equalTo(0));
        w.close();
        reader.close();
        dir.close();
    }
}
