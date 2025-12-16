/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.fielddata.fieldcomparator;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.DocValuesRangeIterator;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

public class SecondarySortIteratorTests extends ESTestCase {

    public void testAgainstDocValuesRangeIterator() throws IOException {

        Directory dir = newDirectory();
        Sort indexSort = new Sort(new SortField("hostname", SortField.Type.STRING), new SortField("@timestamp", SortField.Type.LONG, true));
        IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random())).setIndexSort(indexSort);
        RandomIndexWriter indexWriter = new RandomIndexWriter(random(), dir, iwc);

        int numDocs = atLeast(1000);
        for (int i = 0; i < numDocs; i++) {
            Document doc = new Document();
            doc.add(SortedDocValuesField.indexedField("hostname", new BytesRef("host1")));
            doc.add(NumericDocValuesField.indexedField("@timestamp", 1_000_000 + i));
            indexWriter.addDocument(doc);
        }

        indexWriter.forceMerge(1);

        IndexReader reader = indexWriter.getReader();
        long start = 1_000_400;
        long end = 1_000_499;
        DocIdSetIterator dvIt = docValuesRangeIterator(reader.leaves().getFirst(), start, end);
        DocIdSetIterator ssIt = secondarySortIterator(reader.leaves().getFirst(), start, end);

        // Because the primary sort field has only a single value, we should get exactly the same
        // results from the secondary sort iterator as from a standard DVRangeIterator over the
        // secondary field
        for (int doc = dvIt.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = dvIt.nextDoc()) {
            assertEquals(doc, ssIt.nextDoc());
        }
        assertEquals(DocIdSetIterator.NO_MORE_DOCS, ssIt.nextDoc());

        reader.close();
        indexWriter.close();
        dir.close();
    }

    private static DocIdSetIterator secondarySortIterator(LeafReaderContext ctx, long start, long end) throws IOException {
        NumericDocValues timeStampDV = ctx.reader().getNumericDocValues("@timestamp");
        DocValuesSkipper primarySkipper = ctx.reader().getDocValuesSkipper("hostname");
        DocValuesSkipper secondarySkipper = ctx.reader().getDocValuesSkipper("@timestamp");
        return new SecondarySortIterator(timeStampDV, secondarySkipper, primarySkipper, start, end);
    }

    private static DocIdSetIterator docValuesRangeIterator(LeafReaderContext ctx, long start, long end) throws IOException {
        NumericDocValues timeStampDV = ctx.reader().getNumericDocValues("@timestamp");
        TwoPhaseIterator twoPhaseIterator = new TwoPhaseIterator(timeStampDV) {
            @Override
            public boolean matches() throws IOException {
                return timeStampDV.longValue() >= start && timeStampDV.longValue() <= end;
            }

            @Override
            public float matchCost() {
                return 2;
            }
        };
        DocValuesSkipper skipper = ctx.reader().getDocValuesSkipper("@timestamp");
        return TwoPhaseIterator.asDocIdSetIterator(new DocValuesRangeIterator(twoPhaseIterator, skipper, start, end, false));
    }
}
