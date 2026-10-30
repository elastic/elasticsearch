/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.extras;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.index.mapper.FieldTypeTestCase;
import org.elasticsearch.index.mapper.IndexType;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperBuilderContext;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.mapper.NumericDocValuesQueries;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.containsString;

public class ScaledFloatFieldTypeTests extends FieldTypeTestCase {

    public void testTermQuery() {
        ScaledFloatFieldMapper.ScaledFloatFieldType ft = new ScaledFloatFieldMapper.ScaledFloatFieldType(
            "scaled_float",
            0.1 + randomDouble() * 100
        );
        double value = (randomDouble() * 2 - 1) * 10000;
        long scaledValue = Math.round(value * ft.getScalingFactor());
        assertEquals(LongField.newExactQuery("scaled_float", scaledValue), ft.termQuery(value, MOCK_CONTEXT));

        MappedFieldType ft2 = new ScaledFloatFieldMapper.ScaledFloatFieldType("scaled_float", 0.1 + randomDouble() * 100, false);
        ElasticsearchException e2 = expectThrows(ElasticsearchException.class, () -> ft2.termQuery("42", MOCK_CONTEXT_DISALLOW_EXPENSIVE));
        assertEquals(
            "Cannot search on field [scaled_float] since it is not indexed and 'search.allow_expensive_queries' is set to false.",
            e2.getMessage()
        );
    }

    public void testTermsQuery() {
        ScaledFloatFieldMapper.ScaledFloatFieldType ft = new ScaledFloatFieldMapper.ScaledFloatFieldType(
            "scaled_float",
            0.1 + randomDouble() * 100
        );
        double value1 = (randomDouble() * 2 - 1) * 10000;
        long scaledValue1 = Math.round(value1 * ft.getScalingFactor());
        double value2 = (randomDouble() * 2 - 1) * 10000;
        long scaledValue2 = Math.round(value2 * ft.getScalingFactor());
        assertEquals(
            LongPoint.newSetQuery("scaled_float", scaledValue1, scaledValue2),
            ft.termsQuery(Arrays.asList(value1, value2), MOCK_CONTEXT)
        );

        MappedFieldType ft2 = new ScaledFloatFieldMapper.ScaledFloatFieldType("scaled_float", 0.1 + randomDouble() * 100, false);
        ElasticsearchException e2 = expectThrows(
            ElasticsearchException.class,
            () -> ft2.termsQuery(Arrays.asList(value1, value2), MOCK_CONTEXT_DISALLOW_EXPENSIVE)
        );
        assertEquals(
            "Cannot search on field [scaled_float] since it is not indexed and 'search.allow_expensive_queries' is set to false.",
            e2.getMessage()
        );
    }

    public void testRangeQuery() throws IOException {
        // make sure the accuracy loss of scaled floats only occurs at index time
        // this test checks that searching scaled floats yields the same results as
        // searching doubles that are rounded to the closest half float
        ScaledFloatFieldMapper.ScaledFloatFieldType ft = new ScaledFloatFieldMapper.ScaledFloatFieldType(
            "scaled_float",
            IndexType.points(true, randomBoolean()),
            false,
            Collections.emptyMap(),
            0.1 + randomDouble() * 100,
            null,
            null,
            null,
            false,
            false
        );
        Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, new IndexWriterConfig(null));
        final int numDocs = 1000;
        for (int i = 0; i < numDocs; ++i) {
            Document doc = new Document();
            double value = (randomDouble() * 2 - 1) * 10000;
            long scaledValue = Math.round(value * ft.getScalingFactor());
            double rounded = scaledValue / ft.getScalingFactor();
            doc.add(new LongPoint("scaled_float", scaledValue));
            doc.add(new SortedNumericDocValuesField("scaled_float", scaledValue));
            doc.add(new DoublePoint("double", rounded));
            doc.add(new SortedNumericDocValuesField("double", NumericUtils.doubleToSortableLong(rounded)));
            w.addDocument(doc);
        }
        final DirectoryReader reader = DirectoryReader.open(w);
        w.close();
        IndexSearcher searcher = newSearcher(reader);
        final int numQueries = 1000;
        for (int i = 0; i < numQueries; ++i) {
            Double l = randomBoolean() ? null : (randomDouble() * 2 - 1) * 10000;
            Double u = randomBoolean() ? null : (randomDouble() * 2 - 1) * 10000;
            boolean includeLower = randomBoolean();
            boolean includeUpper = randomBoolean();
            Query doubleQ = NumberFieldMapper.NumberType.DOUBLE.rangeQuery(
                "double",
                l,
                u,
                includeLower,
                includeUpper,
                false,
                MOCK_CONTEXT,
                randomBoolean(),
                false,
                NumericDocValuesQueries.SORTED_NUMERIC
            );
            Query scaledFloatQ = ft.rangeQuery(l, u, includeLower, includeUpper, MOCK_CONTEXT);
            assertEquals(searcher.count(doubleQ), searcher.count(scaledFloatQ));
        }
        IOUtils.close(reader, dir);
    }

    public void testRoundsUpperBoundCorrectly() {
        ScaledFloatFieldMapper.ScaledFloatFieldType ft = new ScaledFloatFieldMapper.ScaledFloatFieldType("scaled_float", 100);
        Query scaledFloatQ = ft.rangeQuery(null, 0.1, true, false, MOCK_CONTEXT);
        assertThat(scaledFloatQ.toString(), containsString("scaled_float:[-9223372036854775808 TO 9]"));
        scaledFloatQ = ft.rangeQuery(null, 0.1, true, true, MOCK_CONTEXT);
        assertThat(scaledFloatQ.toString(), containsString("scaled_float:[-9223372036854775808 TO 10]"));
        scaledFloatQ = ft.rangeQuery(null, 0.095, true, false, MOCK_CONTEXT);
        assertThat(scaledFloatQ.toString(), containsString("scaled_float:[-9223372036854775808 TO 9]"));
        scaledFloatQ = ft.rangeQuery(null, 0.095, true, true, MOCK_CONTEXT);
        assertThat(scaledFloatQ.toString(), containsString("scaled_float:[-9223372036854775808 TO 9]"));
        scaledFloatQ = ft.rangeQuery(null, 0.105, true, false, MOCK_CONTEXT);
        assertThat(scaledFloatQ.toString(), containsString("scaled_float:[-9223372036854775808 TO 10]"));
        scaledFloatQ = ft.rangeQuery(null, 0.105, true, true, MOCK_CONTEXT);
        assertThat(scaledFloatQ.toString(), containsString("scaled_float:[-9223372036854775808 TO 10]"));
        scaledFloatQ = ft.rangeQuery(null, 79.99, true, true, MOCK_CONTEXT);
        assertThat(scaledFloatQ.toString(), containsString("scaled_float:[-9223372036854775808 TO 7999]"));
    }

    public void testRoundsLowerBoundCorrectly() {
        ScaledFloatFieldMapper.ScaledFloatFieldType ft = new ScaledFloatFieldMapper.ScaledFloatFieldType("scaled_float", 100);
        Query scaledFloatQ = ft.rangeQuery(-0.1, null, false, true, MOCK_CONTEXT);
        assertThat(scaledFloatQ.toString(), containsString("scaled_float:[-9 TO 9223372036854775807]"));
        scaledFloatQ = ft.rangeQuery(-0.1, null, true, true, MOCK_CONTEXT);
        assertThat(scaledFloatQ.toString(), containsString("scaled_float:[-10 TO 9223372036854775807]"));
        scaledFloatQ = ft.rangeQuery(-0.095, null, false, true, MOCK_CONTEXT);
        assertThat(scaledFloatQ.toString(), containsString("scaled_float:[-9 TO 9223372036854775807]"));
        scaledFloatQ = ft.rangeQuery(-0.095, null, true, true, MOCK_CONTEXT);
        assertThat(scaledFloatQ.toString(), containsString("scaled_float:[-9 TO 9223372036854775807]"));
        scaledFloatQ = ft.rangeQuery(-0.105, null, false, true, MOCK_CONTEXT);
        assertThat(scaledFloatQ.toString(), containsString("scaled_float:[-10 TO 9223372036854775807]"));
        scaledFloatQ = ft.rangeQuery(-0.105, null, true, true, MOCK_CONTEXT);
        assertThat(scaledFloatQ.toString(), containsString("scaled_float:[-10 TO 9223372036854775807]"));
    }

    public void testValueForSearch() {
        ScaledFloatFieldMapper.ScaledFloatFieldType ft = new ScaledFloatFieldMapper.ScaledFloatFieldType(
            "scaled_float",
            0.1 + randomDouble() * 100
        );
        assertNull(ft.valueForDisplay(null));
        assertEquals(10 / ft.getScalingFactor(), ft.valueForDisplay(10L));
    }

    public void testFieldData() throws IOException {
        double scalingFactor = 0.1 + randomDouble() * 100;
        Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, new IndexWriterConfig(null));
        Document doc0 = new Document();
        doc0.add(new SortedNumericDocValuesField("scaled_float1", 10));
        doc0.add(new SortedNumericDocValuesField("scaled_float2", 5));
        doc0.add(new SortedNumericDocValuesField("scaled_float2", 12));
        w.addDocument(doc0);
        // a second doc with a different singleton value, and no value for the multi-valued field
        Document doc1 = new Document();
        doc1.add(new SortedNumericDocValuesField("scaled_float1", 20));
        w.addDocument(doc1);
        // a third doc with no value for either field
        w.addDocument(new Document());
        try (DirectoryReader reader = DirectoryReader.open(w)) {
            LeafReader leaf = getOnlyLeafReader(reader);

            // single-valued
            assertEquals(IndexNumericFieldData.NumericType.DOUBLE, scaledFloatFieldData("scaled_float1", scalingFactor).getNumericType());
            SortedNumericDoubleValues values = doubleValues(leaf, "scaled_float1", scalingFactor);
            assertNotNull(values.docIdIterator());
            assertTrue(values.advanceExact(0));
            assertEquals(1, values.docValueCount());
            assertEquals(10 / scalingFactor, values.nextValue(), 10e-5);
            assertTrue(values.advanceExact(1));
            assertEquals(1, values.docValueCount());
            assertEquals(20 / scalingFactor, values.nextValue(), 10e-5);
            assertFalse(values.advanceExact(2));

            // multi-valued
            values = doubleValues(leaf, "scaled_float2", scalingFactor);
            assertTrue(values.advanceExact(0));
            assertEquals(2, values.docValueCount());
            assertEquals(5 / scalingFactor, values.nextValue(), 10e-5);
            assertEquals(12 / scalingFactor, values.nextValue(), 10e-5);
            assertFalse(values.advanceExact(1));
            assertFalse(values.advanceExact(2));
        }
        IOUtils.close(w, dir);
    }

    /**
     * The {@link SortedNumericDoubleValues#docIdIterator()} must be the one that backs the values, so that positioning it
     * with {@code advance} makes {@code nextValue()} return that doc's value. This checks that for both the
     * single-valued path (which reads straight off the unwrapped singleton) and the multi-valued path, using sparse fields.
     */
    public void testFieldDataDocIdIteratorIsInSyncWithValues() throws IOException {
        double scalingFactor = 0.1 + randomDouble() * 100;
        try (Directory dir = newDirectory(); IndexWriter w = new IndexWriter(dir, new IndexWriterConfig(null))) {
            // docs 0, 2 and 4 have values; docs 1 and 3 have none, which keeps the fields sparse
            for (int doc = 0; doc < 5; doc++) {
                Document document = new Document();
                if (doc % 2 == 0) {
                    document.add(new SortedNumericDocValuesField("scaled_float1", 10L * (doc + 1)));
                    document.add(new SortedNumericDocValuesField("scaled_float2", 5L * (doc + 1)));
                    document.add(new SortedNumericDocValuesField("scaled_float2", 12L * (doc + 1)));
                }
                w.addDocument(document);
            }
            try (DirectoryReader reader = DirectoryReader.open(w)) {
                LeafReader leaf = getOnlyLeafReader(reader);

                // single-valued
                SortedNumericDoubleValues values = doubleValues(leaf, "scaled_float1", scalingFactor);
                assertNotNull(SortedNumericDoubleValues.unwrapSingleton(values));
                DocIdSetIterator iterator = values.docIdIterator();
                assertNotNull(iterator);
                assertSame("the same iterator instance is returned on every call", iterator, values.docIdIterator());

                assertEquals(0, iterator.nextDoc());
                assertEquals(10 / scalingFactor, values.nextValue(), 10e-5);
                assertEquals(2, iterator.advance(1));
                assertEquals(30 / scalingFactor, values.nextValue(), 10e-5);
                assertTrue(values.advanceExact(4));
                assertEquals(4, iterator.docID());
                assertEquals(50 / scalingFactor, values.nextValue(), 10e-5);
                assertEquals(DocIdSetIterator.NO_MORE_DOCS, iterator.nextDoc());

                // multi-valued
                values = doubleValues(leaf, "scaled_float2", scalingFactor);
                assertNull(SortedNumericDoubleValues.unwrapSingleton(values));
                iterator = values.docIdIterator();
                assertNotNull(iterator);

                assertEquals(0, iterator.nextDoc());
                assertEquals(2, values.docValueCount());
                assertEquals(5 / scalingFactor, values.nextValue(), 10e-5);
                assertEquals(12 / scalingFactor, values.nextValue(), 10e-5);
                assertEquals(2, iterator.advance(1));
                assertEquals(2, values.docValueCount());
                assertEquals(15 / scalingFactor, values.nextValue(), 10e-5);
                assertEquals(36 / scalingFactor, values.nextValue(), 10e-5);
                assertTrue(values.advanceExact(4));
                assertEquals(4, iterator.docID());
                assertEquals(25 / scalingFactor, values.nextValue(), 10e-5);
                assertEquals(60 / scalingFactor, values.nextValue(), 10e-5);
                assertEquals(DocIdSetIterator.NO_MORE_DOCS, iterator.nextDoc());
            }
        }
    }

    private static IndexNumericFieldData scaledFloatFieldData(String field, double scalingFactor) {
        ScaledFloatFieldMapper.ScaledFloatFieldType fieldType = new ScaledFloatFieldMapper.ScaledFloatFieldType(field, scalingFactor);
        return (IndexNumericFieldData) fieldType.fielddataBuilder(FieldDataContext.noRuntimeFields("index", "test")).build(null, null);
    }

    private static SortedNumericDoubleValues doubleValues(LeafReader leaf, String field, double scalingFactor) {
        return scaledFloatFieldData(field, scalingFactor).load(leaf.getContext()).getDoubleValues();
    }

    public void testFetchSourceValue() throws IOException {
        MappedFieldType mapper = new ScaledFloatFieldMapper.Builder("field", defaultIndexSettings()).scalingFactor(100)
            .build(MapperBuilderContext.root(false, false))
            .fieldType();
        assertEquals(List.of(3.14), fetchSourceValue(mapper, 3.1415926));
        assertEquals(List.of(3.14), fetchSourceValue(mapper, "3.1415"));
        assertEquals(List.of(), fetchSourceValue(mapper, ""));

        MappedFieldType nullValueMapper = new ScaledFloatFieldMapper.Builder("field", defaultIndexSettings()).scalingFactor(100)
            .nullValue(2.71)
            .build(MapperBuilderContext.root(false, false))
            .fieldType();
        assertEquals(List.of(2.71), fetchSourceValue(nullValueMapper, ""));
    }
}
