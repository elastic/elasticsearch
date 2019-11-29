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

package org.elasticsearch.search.query;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.QueryUtils;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.store.Directory;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Arrays;

public class SortedLongQueryTests extends ESTestCase {

    public void testBasics() {
        SortedLongQuery query1 = new SortedLongQuery("field", 1, 1, 1);
        SortedLongQuery query2 = new SortedLongQuery("field", 1, 1, 1);
        SortedLongQuery query3 = new SortedLongQuery("field", 2, 1, 1);
        SortedLongQuery query4 = new SortedLongQuery("field", 1, 2, 1);
        SortedLongQuery query5 = new SortedLongQuery("field", 1, 1, 2);
        SortedLongQuery query6 = new SortedLongQuery("field2", 1, 1, 1);
        QueryUtils.check(query1);
        QueryUtils.checkEqual(query1, query2);
        QueryUtils.checkUnequal(query1, query3);
        QueryUtils.checkUnequal(query1, query4);
        QueryUtils.checkUnequal(query1, query5);
        QueryUtils.checkUnequal(query1, query6);
    }

    public void testRandom() throws IOException {
        final int numDocs = randomIntBetween(100, 200);
        final Document doc = new Document();
        final Directory dir = newDirectory();
        final IndexWriterConfig config = new IndexWriterConfig();
        final RandomIndexWriter w = new RandomIndexWriter(random(), dir, config);
        long[] values = new long[numDocs];
        for (int i = 0; i < numDocs; ++i) {
            long rand = randomLong();
            values[i] = rand;
            doc.add(new SortedNumericDocValuesField("number", rand));
            doc.add(new LongPoint("number", rand));
            w.addDocument(doc);
            doc.clear();
            if (rarely()) {
                w.commit();
            }
        }
        Arrays.sort(values);
        final IndexReader reader = w.getReader();
        final IndexSearcher searcher = new IndexSearcher(reader);

        Sort sort = new Sort(new SortedNumericSortField("number", SortField.Type.LONG));
        for (int size = 1; size < numDocs; size++) {
            TopFieldDocs topDocs = searcher.search(new SortedLongQuery("number", size, Long.MIN_VALUE, Integer.MAX_VALUE), size, sort);
            assert(topDocs.scoreDocs.length == size);
            for (int j = 0; j < topDocs.scoreDocs.length; j++) {
                assertEquals(values[j], ((FieldDoc) topDocs.scoreDocs[j]).fields[0]);
            }
        }
        w.close();
        reader.close();
        dir.close();
    }
}
