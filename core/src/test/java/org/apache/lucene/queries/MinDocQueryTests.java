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

package org.apache.lucene.queries;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.QueryUtils;
import org.apache.lucene.store.Directory;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

public class MinDocQueryTests extends ESTestCase {

    public void testBasics() {
        MinDocQuery query1 = new MinDocQuery(42);
        MinDocQuery query2 = new MinDocQuery(42);
        MinDocQuery query3 = new MinDocQuery(43);
        QueryUtils.check(query1);
        QueryUtils.checkEqual(query1, query2);
        QueryUtils.checkUnequal(query1, query3);
    }

    public void testRandom() throws IOException {
        final int numDocs = randomIntBetween(10, 200);
        final Document doc = new Document();
        final Directory dir = newDirectory();
        final RandomIndexWriter w = new RandomIndexWriter(getRandom(), dir);
        for (int i = 0; i < numDocs; ++i) {
            w.addDocument(doc);
        }
        final IndexReader reader = w.getReader();
        final IndexSearcher searcher = newSearcher(reader);
        for (int i = 0; i <= numDocs; ++i) {
            assertEquals(numDocs - i, searcher.count(new MinDocQuery(i)));
        }
        w.close();
        reader.close();
        dir.close();
    }

}
