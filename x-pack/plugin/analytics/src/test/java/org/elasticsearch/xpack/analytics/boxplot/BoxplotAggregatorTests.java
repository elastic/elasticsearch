/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.analytics.boxplot;

import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.DocValuesFieldExistsQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.bucket.global.GlobalAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.global.InternalGlobal;
import org.elasticsearch.search.aggregations.bucket.histogram.HistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.InternalHistogram;
import org.elasticsearch.search.aggregations.support.AggregationInspectionHelper;

import java.io.IOException;
import java.util.function.Consumer;

import static java.util.Collections.singleton;
import static org.hamcrest.Matchers.equalTo;

public class BoxplotAggregatorTests extends AggregatorTestCase {

    public void testNoMatchingField() throws IOException {
        testCase(new MatchAllDocsQuery(), iw -> {
            iw.addDocument(singleton(new SortedNumericDocValuesField("wrong_number", 7)));
            iw.addDocument(singleton(new SortedNumericDocValuesField("wrong_number", 3)));
        }, boxplot -> {
            assertEquals(Double.POSITIVE_INFINITY, boxplot.getMin(), 0);
            assertEquals(Double.NEGATIVE_INFINITY, boxplot.getMax(), 0);
            assertEquals(Double.NaN, boxplot.getQ1(), 0);
            assertEquals(Double.NaN, boxplot.getQ2(), 0);
            assertEquals(Double.NaN, boxplot.getQ3(), 0);
        });
    }

    public void testMatchesSortedNumericDocValues() throws IOException {
        testCase(new MatchAllDocsQuery(), iw -> {
            iw.addDocument(singleton(new SortedNumericDocValuesField("number", 2)));
            iw.addDocument(singleton(new SortedNumericDocValuesField("number", 2)));
            iw.addDocument(singleton(new SortedNumericDocValuesField("number", 3)));
            iw.addDocument(singleton(new SortedNumericDocValuesField("number", 4)));
            iw.addDocument(singleton(new SortedNumericDocValuesField("number", 5)));
            iw.addDocument(singleton(new SortedNumericDocValuesField("number", 10)));
        }, boxplot -> {
            assertEquals(2, boxplot.getMin(), 0);
            assertEquals(10, boxplot.getMax(), 0);
            assertEquals(2, boxplot.getQ1(), 0);
            assertEquals(3.5, boxplot.getQ2(), 0);
            assertEquals(5, boxplot.getQ3(), 0);
        });
    }

    public void testMatchesNumericDocValues() throws IOException {
        testCase(new MatchAllDocsQuery(), iw -> {
            iw.addDocument(singleton(new NumericDocValuesField("number", 2)));
            iw.addDocument(singleton(new NumericDocValuesField("number", 2)));
            iw.addDocument(singleton(new NumericDocValuesField("number", 3)));
            iw.addDocument(singleton(new NumericDocValuesField("number", 4)));
            iw.addDocument(singleton(new NumericDocValuesField("number", 5)));
            iw.addDocument(singleton(new NumericDocValuesField("number", 10)));
        }, boxplot -> {
            assertEquals(2, boxplot.getMin(), 0);
            assertEquals(10, boxplot.getMax(), 0);
            assertEquals(2, boxplot.getQ1(), 0);
            assertEquals(3.5, boxplot.getQ2(), 0);
            assertEquals(5, boxplot.getQ3(), 0);
        });
    }

    public void testSomeMatchesSortedNumericDocValues() throws IOException {
        testCase(new DocValuesFieldExistsQuery("number"), iw -> {
            iw.addDocument(singleton(new SortedNumericDocValuesField("number", 2)));
            iw.addDocument(singleton(new SortedNumericDocValuesField("number", 2)));
            iw.addDocument(singleton(new SortedNumericDocValuesField("number2", 2)));
            iw.addDocument(singleton(new SortedNumericDocValuesField("number", 3)));
            iw.addDocument(singleton(new SortedNumericDocValuesField("number", 4)));
            iw.addDocument(singleton(new SortedNumericDocValuesField("number", 5)));
            iw.addDocument(singleton(new SortedNumericDocValuesField("number", 10)));
        }, boxplot -> {
            assertEquals(2, boxplot.getMin(), 0);
            assertEquals(10, boxplot.getMax(), 0);
            assertEquals(2, boxplot.getQ1(), 0);
            assertEquals(3.5, boxplot.getQ2(), 0);
            assertEquals(5, boxplot.getQ3(), 0);
        });
    }

    public void testSomeMatchesNumericDocValues() throws IOException {
        testCase(new DocValuesFieldExistsQuery("number"), iw -> {
            iw.addDocument(singleton(new NumericDocValuesField("number", 2)));
            iw.addDocument(singleton(new NumericDocValuesField("number", 2)));
            iw.addDocument(singleton(new NumericDocValuesField("number2", 2)));
            iw.addDocument(singleton(new NumericDocValuesField("number", 3)));
            iw.addDocument(singleton(new NumericDocValuesField("number", 4)));
            iw.addDocument(singleton(new NumericDocValuesField("number", 5)));
            iw.addDocument(singleton(new NumericDocValuesField("number", 10)));
        }, boxplot -> {
            assertEquals(2, boxplot.getMin(), 0);
            assertEquals(10, boxplot.getMax(), 0);
            assertEquals(2, boxplot.getQ1(), 0);
            assertEquals(3.5, boxplot.getQ2(), 0);
            assertEquals(5, boxplot.getQ3(), 0);
        });
    }

    public void testUnmappedWithMissingField() throws IOException {
        BoxplotAggregationBuilder aggregationBuilder = new BoxplotAggregationBuilder("boxplot")
            .field("does_not_exist").missing(0L);

        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType(NumberFieldMapper.NumberType.INTEGER);
        fieldType.setName("number");

        testCase(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            iw.addDocument(singleton(new NumericDocValuesField("number", 7)));
            iw.addDocument(singleton(new NumericDocValuesField("number", 1)));
        }, (Consumer<InternalBoxplot>) boxplot -> {
            assertEquals(0, boxplot.getMin(), 0);
            assertEquals(0, boxplot.getMax(), 0);
            assertEquals(0, boxplot.getQ1(), 0);
            assertEquals(0, boxplot.getQ2(), 0);
            assertEquals(0, boxplot.getQ3(), 0);
        }, fieldType);
    }

    public void testUnsupportedType() {
        BoxplotAggregationBuilder aggregationBuilder = new BoxplotAggregationBuilder("boxplot").field("not_a_number");

        MappedFieldType fieldType = new KeywordFieldMapper.KeywordFieldType();
        fieldType.setName("not_a_number");
        fieldType.setHasDocValues(true);

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
            () -> testCase(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
                iw.addDocument(singleton(new SortedSetDocValuesField("string", new BytesRef("foo"))));
            }, (Consumer<InternalBoxplot>) boxplot -> {
                fail("Should have thrown exception");
            }, fieldType));
        assertEquals(e.getMessage(), "Expected numeric type on field [not_a_number], but got [keyword]");
    }

    public void testBadMissingField() {
        BoxplotAggregationBuilder aggregationBuilder = new BoxplotAggregationBuilder("boxplot").field("number")
            .missing("not_a_number");

        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType(NumberFieldMapper.NumberType.INTEGER);
        fieldType.setName("number");

        expectThrows(NumberFormatException.class,
            () -> testCase(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
                iw.addDocument(singleton(new NumericDocValuesField("number", 2)));
                iw.addDocument(singleton(new NumericDocValuesField("number", 2)));
                iw.addDocument(singleton(new NumericDocValuesField("number", 3)));
                iw.addDocument(singleton(new NumericDocValuesField("number", 4)));
                iw.addDocument(singleton(new NumericDocValuesField("number", 5)));
                iw.addDocument(singleton(new NumericDocValuesField("number", 10)));
            }, (Consumer<InternalBoxplot>) boxplot -> {
                fail("Should have thrown exception");
            }, fieldType));
    }

    public void testUnmappedWithBadMissingField() {
        BoxplotAggregationBuilder aggregationBuilder = new BoxplotAggregationBuilder("boxplot")
            .field("does_not_exist").missing("not_a_number");

        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType(NumberFieldMapper.NumberType.INTEGER);
        fieldType.setName("number");

        expectThrows(NumberFormatException.class,
            () -> testCase(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
                iw.addDocument(singleton(new NumericDocValuesField("number", 2)));
                iw.addDocument(singleton(new NumericDocValuesField("number", 2)));
                iw.addDocument(singleton(new NumericDocValuesField("number", 3)));
                iw.addDocument(singleton(new NumericDocValuesField("number", 4)));
                iw.addDocument(singleton(new NumericDocValuesField("number", 5)));
                iw.addDocument(singleton(new NumericDocValuesField("number", 10)));
            }, (Consumer<InternalBoxplot>) boxplot -> {
                fail("Should have thrown exception");
            }, fieldType));
    }

    public void testEmptyBucket() throws IOException {
        HistogramAggregationBuilder histogram = new HistogramAggregationBuilder("histo").field("number").interval(10).minDocCount(0)
            .subAggregation(new BoxplotAggregationBuilder("boxplot").field("number"));

        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType(NumberFieldMapper.NumberType.INTEGER);
        fieldType.setName("number");

        testCase(histogram, new MatchAllDocsQuery(), iw -> {
            iw.addDocument(singleton(new NumericDocValuesField("number", 1)));
            iw.addDocument(singleton(new NumericDocValuesField("number", 3)));
            iw.addDocument(singleton(new NumericDocValuesField("number", 21)));
            iw.addDocument(singleton(new NumericDocValuesField("number", 23)));
        }, (Consumer<InternalHistogram>) histo -> {
            assertThat(histo.getBuckets().size(), equalTo(3));

            assertNotNull(histo.getBuckets().get(0).getAggregations().asMap().get("boxplot"));
            InternalBoxplot boxplot = (InternalBoxplot) histo.getBuckets().get(0).getAggregations().asMap().get("boxplot");
            assertEquals(1, boxplot.getMin(), 0);
            assertEquals(3, boxplot.getMax(), 0);
            assertEquals(1, boxplot.getQ1(), 0);
            assertEquals(2, boxplot.getQ2(), 0);
            assertEquals(3, boxplot.getQ3(), 0);

            assertNotNull(histo.getBuckets().get(1).getAggregations().asMap().get("boxplot"));
            boxplot = (InternalBoxplot) histo.getBuckets().get(1).getAggregations().asMap().get("boxplot");
            assertEquals(Double.POSITIVE_INFINITY, boxplot.getMin(), 0);
            assertEquals(Double.NEGATIVE_INFINITY, boxplot.getMax(), 0);
            assertEquals(Double.NaN, boxplot.getQ1(), 0);
            assertEquals(Double.NaN, boxplot.getQ2(), 0);
            assertEquals(Double.NaN, boxplot.getQ3(), 0);

            assertNotNull(histo.getBuckets().get(2).getAggregations().asMap().get("boxplot"));
            boxplot = (InternalBoxplot) histo.getBuckets().get(2).getAggregations().asMap().get("boxplot");
            assertEquals(21, boxplot.getMin(), 0);
            assertEquals(23, boxplot.getMax(), 0);
            assertEquals(21, boxplot.getQ1(), 0);
            assertEquals(22, boxplot.getQ2(), 0);
            assertEquals(23, boxplot.getQ3(), 0);
        }, fieldType);
    }

    public void testFormatter() throws IOException {
        BoxplotAggregationBuilder aggregationBuilder = new BoxplotAggregationBuilder("boxplot").field("number")
            .format("0000.0");

        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType(NumberFieldMapper.NumberType.INTEGER);
        fieldType.setName("number");

        testCase(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            iw.addDocument(singleton(new NumericDocValuesField("number", 1)));
            iw.addDocument(singleton(new NumericDocValuesField("number", 2)));
            iw.addDocument(singleton(new NumericDocValuesField("number", 3)));
            iw.addDocument(singleton(new NumericDocValuesField("number", 4)));
            iw.addDocument(singleton(new NumericDocValuesField("number", 5)));
        }, (Consumer<InternalBoxplot>) boxplot -> {
            assertEquals(1, boxplot.getMin(), 0);
            assertEquals(5, boxplot.getMax(), 0);
            assertEquals(1.75, boxplot.getQ1(), 0);
            assertEquals(3, boxplot.getQ2(), 0);
            assertEquals(4.25, boxplot.getQ3(), 0);
            assertEquals("0001.0", boxplot.getMinAsString());
            assertEquals("0005.0", boxplot.getMaxAsString());
            assertEquals("0001.8", boxplot.getQ1AsString());
            assertEquals("0003.0", boxplot.getQ2AsString());
            assertEquals("0004.2", boxplot.getQ3AsString());
        }, fieldType);
    }

    public void testGetProperty() throws IOException {
        GlobalAggregationBuilder globalBuilder = new GlobalAggregationBuilder("global")
            .subAggregation(new BoxplotAggregationBuilder("boxplot").field("number"));

        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType(NumberFieldMapper.NumberType.INTEGER);
        fieldType.setName("number");

        testCase(globalBuilder, new MatchAllDocsQuery(), iw -> {
            iw.addDocument(singleton(new NumericDocValuesField("number", 1)));
            iw.addDocument(singleton(new NumericDocValuesField("number", 2)));
            iw.addDocument(singleton(new NumericDocValuesField("number", 3)));
            iw.addDocument(singleton(new NumericDocValuesField("number", 4)));
            iw.addDocument(singleton(new NumericDocValuesField("number", 5)));
        }, (Consumer<InternalGlobal>) global -> {
            assertEquals(5, global.getDocCount());
            assertTrue(AggregationInspectionHelper.hasValue(global));
            assertNotNull(global.getAggregations().asMap().get("boxplot"));
            InternalBoxplot boxplot = (InternalBoxplot) global.getAggregations().asMap().get("boxplot");
            assertThat(global.getProperty("boxplot"), equalTo(boxplot));
            assertThat(global.getProperty("boxplot.min"), equalTo(1.0));
            assertThat(global.getProperty("boxplot.max"), equalTo(5.0));
            assertThat(boxplot.getProperty("min"), equalTo(1.0));
            assertThat(boxplot.getProperty("max"), equalTo(5.0));
        }, fieldType);
    }

    private void testCase(Query query,
                          CheckedConsumer<RandomIndexWriter, IOException> buildIndex,
                          Consumer<InternalBoxplot> verify) throws IOException {
        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType(NumberFieldMapper.NumberType.INTEGER);
        fieldType.setName("number");
        BoxplotAggregationBuilder aggregationBuilder = new BoxplotAggregationBuilder("boxplot").field("number");
        testCase(aggregationBuilder, query, buildIndex, verify, fieldType);
    }

    private <T extends AggregationBuilder, V extends InternalAggregation> void testCase(
        T aggregationBuilder, Query query,
        CheckedConsumer<RandomIndexWriter, IOException> buildIndex,
        Consumer<V> verify, MappedFieldType fieldType) throws IOException {
        try (Directory directory = newDirectory()) {
            RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory);
            buildIndex.accept(indexWriter);
            indexWriter.close();

            try (IndexReader indexReader = DirectoryReader.open(directory)) {
                IndexSearcher indexSearcher = newSearcher(indexReader, true, true);

                V agg = searchAndReduce(indexSearcher, query, aggregationBuilder, fieldType);
                verify.accept(agg);

            }
        }
    }


}
