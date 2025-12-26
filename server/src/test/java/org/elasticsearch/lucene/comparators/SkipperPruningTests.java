/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.lucene.comparators;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Pruning;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.fielddata.fieldcomparator.LongValuesComparatorSource;
import org.elasticsearch.index.fielddata.plain.SortedNumericIndexFieldData;
import org.elasticsearch.index.mapper.IndexType;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class SkipperPruningTests extends ESTestCase {

    public void testCompetitiveIterator() throws IOException {

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

        SortedNumericIndexFieldData fd = new SortedNumericIndexFieldData(
            "@timestamp",
            IndexNumericFieldData.NumericType.LONG,
            CoreValuesSourceType.NUMERIC,
            null,
            IndexType.skippers()
        );
        LongValuesComparatorSource source = new LongValuesComparatorSource(
            fd,
            null,
            MultiValueMode.MAX,
            null,
            IndexNumericFieldData.NumericType.LONG
        );

        var comparator = (XLongComparator) source.newComparator("@timestamp", 1, Pruning.GREATER_THAN_OR_EQUAL_TO, true);
        comparator.queueFull = true;
        comparator.hitsThresholdReached = true;
        comparator.bottom = Long.MAX_VALUE;
        var leafComparator = comparator.getLeafComparator(reader.leaves().getFirst());
        assertThat(leafComparator.competitiveIterator().nextDoc(), equalTo(DocIdSetIterator.NO_MORE_DOCS));

        reader.close();
        indexWriter.close();
        dir.close();
    }
}
