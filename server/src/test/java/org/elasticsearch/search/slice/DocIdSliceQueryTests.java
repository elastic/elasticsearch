/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.slice;

import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.QueryUtils;
import org.apache.lucene.store.Directory;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;

public class DocIdSliceQueryTests extends ESTestCase {

    public void testEqualsAndHashCode() {
        DocIdSliceQuery query1 = new DocIdSliceQuery(1, 10);
        DocIdSliceQuery query2 = new DocIdSliceQuery(1, 10);
        DocIdSliceQuery query3 = new DocIdSliceQuery(1, 7);
        DocIdSliceQuery query4 = new DocIdSliceQuery(2, 10);

        QueryUtils.check(query1);
        QueryUtils.checkEqual(query1, query2);
        QueryUtils.checkUnequal(query1, query3);
        QueryUtils.checkUnequal(query1, query4);
    }

    public void testEmptySlice() throws Exception {
        Directory dir = newDirectory();
        RandomIndexWriter w = new RandomIndexWriter(random(), dir, new KeywordAnalyzer());
        for (int i = 0; i < 10; ++i) {
            Document doc = new Document();
            doc.add(new StringField("field", "value", Field.Store.YES));
            w.addDocument(doc);
        }

        IndexReader reader = w.getReader();
        IndexSearcher searcher = newSearcher(reader);
        DocIdSliceQuery query = new DocIdSliceQuery(1, 1);
        assertThat(searcher.count(query), equalTo(0));

        w.close();
        reader.close();
        dir.close();
    }

    public void testSearch() throws Exception {
        Directory dir = newDirectory();
        RandomIndexWriter w = new RandomIndexWriter(random(), dir, new KeywordAnalyzer());

        int numDocs = randomIntBetween(100, 200);
        int max = randomIntBetween(2, 10);

        for (int i = 0; i < numDocs; i++) {
            Document doc = new Document();
            doc.add(new StringField("field", "value", Field.Store.YES));
            w.addDocument(doc);
        }

        IndexReader reader = w.getReader();
        IndexSearcher searcher = newSearcher(reader);

        int remainder = numDocs % max;
        int quotient = numDocs / max;

        BooleanQuery.Builder booleanQuery = new BooleanQuery.Builder();
        for (int id = 0; id < max; id++) {
            DocIdSliceQuery query = new DocIdSliceQuery(id, max);

            int expectedCount = id < remainder ? quotient + 1 : quotient;
            assertThat(searcher.count(query), equalTo(expectedCount));
            booleanQuery.add(query, BooleanClause.Occur.SHOULD);
        }

        assertThat(searcher.count(booleanQuery.build()), equalTo(numDocs));

        w.close();
        reader.close();
        dir.close();
    }

}
