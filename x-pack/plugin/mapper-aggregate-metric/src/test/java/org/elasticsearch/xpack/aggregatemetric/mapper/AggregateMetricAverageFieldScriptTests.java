/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.aggregatemetric.mapper;

import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.DoubleStream;

import static org.hamcrest.Matchers.hasSize;

public class AggregateMetricAverageFieldScriptTests extends ESTestCase {

    public void testAverage() throws IOException {
        try (Directory directory = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
            FieldType fieldType = new FieldType();
            fieldType.setDocValuesType(DocValuesType.BINARY);
            int docCount = randomIntBetween(1, 20);
            double[] expectedAvg = DoubleStream.generate(() -> randomDoubleBetween(1, 100, false)).limit(docCount).toArray();
            for (int i = 0; i < docCount; i++) {
                int count = randomIntBetween(1, 10);
                iw.addDocument(createDocument(expectedAvg[i] * count, count));
            }
            try (DirectoryReader reader = iw.getReader()) {
                AggregateMetricAverageFieldScript docValues = new AggregateMetricAverageFieldScript(
                    "test",
                    new SearchLookup(field -> null, (ft, lookup, ftd) -> null, (ctx, doc) -> null),
                    reader.leaves().get(0)
                );
                List<Double> values = new ArrayList<>();
                for (int i = 0; i < docCount; i++) {
                    docValues.runForDoc(i, values::add);
                }
                assertThat(values, hasSize(docCount));
                for (int i = 0; i < docCount; i++) {
                    assertEquals(expectedAvg[i], values.get(i), 0.000001);
                }
            }
        }
    }

    private Iterable<IndexableField> createDocument(double sum, int count) {
        return List.of(
            new SortedNumericDocValuesField("test.sum", NumericUtils.doubleToSortableLong(sum)),
            new SortedNumericDocValuesField("test.value_count", count)
        );
    }
}
