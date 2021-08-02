/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.apache.lucene.queries;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.QueryUtils;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.search.SortedSetSortField;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class TestSearchAfterSortedDocQuery extends ESTestCase {

    public void testBasics() {
        Sort sort1 = new Sort(
            new SortedNumericSortField("field1", SortField.Type.INT),
            new SortedSetSortField("field2", false)
        );
        Sort sort2 = new Sort(
            new SortedNumericSortField("field1", SortField.Type.INT),
            new SortedSetSortField("field3", false)
        );
        FieldDoc fieldDoc1 = new FieldDoc(0, 0f, new Object[]{5, new BytesRef("foo")});
        FieldDoc fieldDoc2 = new FieldDoc(0, 0f, new Object[]{5, new BytesRef("foo")});

        SearchAfterSortedDocQuery query1 = new SearchAfterSortedDocQuery(sort1, fieldDoc1);
        SearchAfterSortedDocQuery query2 = new SearchAfterSortedDocQuery(sort1, fieldDoc2);
        SearchAfterSortedDocQuery query3 = new SearchAfterSortedDocQuery(sort2, fieldDoc2);
        QueryUtils.check(query1);
        QueryUtils.checkEqual(query1, query2);
        QueryUtils.checkUnequal(query1, query3);
    }

    public void testInvalidSort() {
        Sort sort = new Sort(new SortedNumericSortField("field1", SortField.Type.INT));
        FieldDoc fieldDoc = new FieldDoc(0, 0f, new Object[] {4, 5});
        IllegalArgumentException ex =
            expectThrows(IllegalArgumentException.class, () -> new SearchAfterSortedDocQuery(sort, fieldDoc));
        assertThat(ex.getMessage(), equalTo("after doc  has 2 value(s) but sort has 1."));
    }

    public void testRandom() throws IOException {
        final int numDocs = randomIntBetween(100, 200);
        final Document doc = new Document();
        final Directory dir = newDirectory();
        Sort sort = new Sort(
            new SortedNumericSortField("number1", SortField.Type.INT, randomBoolean()),
            new SortField("string", SortField.Type.STRING, randomBoolean())
        );
        final IndexWriterConfig config = new IndexWriterConfig();
        config.setIndexSort(sort);
        final RandomIndexWriter w = new RandomIndexWriter(random(), dir, config);
        for (int i = 0; i < numDocs; ++i) {
            int rand = randomIntBetween(0, 10);
            doc.add(new SortedNumericDocValuesField("number", rand));
            doc.add(new SortedDocValuesField("string", new BytesRef(randomAlphaOfLength(randomIntBetween(5, 50)))));
            w.addDocument(doc);
            doc.clear();
            if (rarely()) {
                w.commit();
            }
        }
        final IndexReader reader = w.getReader();
        final IndexSearcher searcher = new IndexSearcher(reader);

        int step = randomIntBetween(1, 10);
        FixedBitSet bitSet = new FixedBitSet(numDocs);
        TopDocs topDocs = null;
        for (int i = 0; i < numDocs;) {
            if (topDocs != null) {
                FieldDoc after = (FieldDoc) topDocs.scoreDocs[topDocs.scoreDocs.length - 1];
                topDocs = searcher.search(new SearchAfterSortedDocQuery(sort, after), step, sort);
            } else {
                topDocs = searcher.search(new MatchAllDocsQuery(), step, sort);
            }
            i += step;
            for (ScoreDoc topDoc : topDocs.scoreDocs) {
                int readerIndex = ReaderUtil.subIndex(topDoc.doc, reader.leaves());
                final LeafReaderContext leafReaderContext = reader.leaves().get(readerIndex);
                int docRebase = topDoc.doc - leafReaderContext.docBase;
                if (leafReaderContext.reader().hasDeletions()) {
                    assertTrue(leafReaderContext.reader().getLiveDocs().get(docRebase));
                }
                assertFalse(bitSet.get(topDoc.doc));
                bitSet.set(topDoc.doc);
            }
        }
        assertThat(bitSet.cardinality(), equalTo(reader.numDocs()));
        w.close();
        reader.close();
        dir.close();
    }
}
