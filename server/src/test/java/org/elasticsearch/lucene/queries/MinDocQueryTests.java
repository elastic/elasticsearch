/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.lucene.queries;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
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

        MinDocQuery query4 = new MinDocQuery(42, new Object());
        MinDocQuery query5 = new MinDocQuery(42, new Object());
        QueryUtils.checkUnequal(query4, query5);
    }

    public void testRewrite() throws Exception {
        IndexReader reader = new MultiReader();
        MinDocQuery query = new MinDocQuery(42);
        Query rewritten = query.rewrite(reader);
        QueryUtils.checkUnequal(query, rewritten);
        Query rewritten2 = rewritten.rewrite(reader);
        assertSame(rewritten, rewritten2);
    }

    public void testRandom() throws IOException {
        final int numDocs = randomIntBetween(10, 200);
        final Document doc = new Document();
        final Directory dir = newDirectory();
        final RandomIndexWriter w = new RandomIndexWriter(random(), dir);
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
