/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.metrics;

import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.LongPoint;
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
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.mapper.RangeFieldMapper;
import org.elasticsearch.index.mapper.RangeType;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.support.AggregationInspectionHelper;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static org.elasticsearch.search.aggregations.AggregationBuilders.percentiles;
import static org.hamcrest.Matchers.equalTo;

public class HDRPercentilesAggregatorTests extends AggregatorTestCase {

    @Override
    protected AggregationBuilder createAggBuilderForTypeTest(MappedFieldType fieldType, String fieldName) {
        return new PercentilesAggregationBuilder("hdr_percentiles").field(fieldName).percentilesConfig(new PercentilesConfig.Hdr());
    }

    @Override
    protected List<ValuesSourceType> getSupportedValuesSourceTypes() {
        return List.of(CoreValuesSourceType.NUMERIC, CoreValuesSourceType.DATE, CoreValuesSourceType.BOOLEAN);
    }

    public void testNoDocs() throws IOException {
        testCase(new MatchAllDocsQuery(), iw -> {
            // Intentionally not writing any docs
        }, hdr -> {
            assertEquals(0L, hdr.state.getTotalCount());
            assertFalse(AggregationInspectionHelper.hasValue(hdr));
        });
    }

    /**
     * Attempting to use HDRPercentileAggregation on a string field throws IllegalArgumentException
     */
    public void testStringField() throws IOException {
        final String fieldName = "string";
        MappedFieldType fieldType = new KeywordFieldMapper.KeywordFieldType(fieldName);
        expectThrows(IllegalArgumentException.class, () -> testCase(new DocValuesFieldExistsQuery(fieldName), iw -> {
            iw.addDocument(singleton(new SortedSetDocValuesField("string", new BytesRef("bogus"))));
            iw.addDocument(singleton(new SortedSetDocValuesField("string", new BytesRef("zwomp"))));
            iw.addDocument(singleton(new SortedSetDocValuesField("string", new BytesRef("foobar"))));
        }, hdr -> {}, fieldType, fieldName));
    }

    /**
     * Attempting to use HDRPercentileAggregation on a range field throws IllegalArgumentException
     */
    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/42949")
    public void testRangeField() throws IOException {
        // Currently fails (throws ClassCast exception), but should be fixed once HDRPercentileAggregation uses the ValuesSource registry
        final String fieldName = "range";
        MappedFieldType fieldType = new RangeFieldMapper.RangeFieldType(fieldName, RangeType.DOUBLE);
        RangeFieldMapper.Range range = new RangeFieldMapper.Range(RangeType.DOUBLE, 1.0D, 5.0D, true, true);
        BytesRef encodedRange = RangeType.DOUBLE.encodeRanges(Collections.singleton(range));
        expectThrows(
            IllegalArgumentException.class,
            () -> testCase(
                new DocValuesFieldExistsQuery(fieldName),
                iw -> { iw.addDocument(singleton(new BinaryDocValuesField(fieldName, encodedRange))); },
                hdr -> {},
                fieldType,
                fieldName
            )
        );
    }

    public void testNoMatchingField() throws IOException {
        testCase(new MatchAllDocsQuery(), iw -> {
            iw.addDocument(singleton(new SortedNumericDocValuesField("wrong_number", 7)));
            iw.addDocument(singleton(new SortedNumericDocValuesField("wrong_number", 1)));
        }, hdr -> {
            assertEquals(0L, hdr.state.getTotalCount());
            assertFalse(AggregationInspectionHelper.hasValue(hdr));
        });
    }

    public void testSomeMatchesSortedNumericDocValues() throws IOException {
        testCase(new DocValuesFieldExistsQuery("number"), iw -> {
            iw.addDocument(singleton(new SortedNumericDocValuesField("number", 60)));
            iw.addDocument(singleton(new SortedNumericDocValuesField("number", 40)));
            iw.addDocument(singleton(new SortedNumericDocValuesField("number", 20)));
            iw.addDocument(singleton(new SortedNumericDocValuesField("number", 10)));
        }, hdr -> {
            assertEquals(4L, hdr.state.getTotalCount());
            double approximation = 0.05d;
            assertEquals(10.0d, hdr.percentile(25), approximation);
            assertEquals(20.0d, hdr.percentile(50), approximation);
            assertEquals(40.0d, hdr.percentile(75), approximation);
            assertEquals(60.0d, hdr.percentile(99), approximation);
            assertTrue(AggregationInspectionHelper.hasValue(hdr));
        });
    }

    public void testSomeMatchesNumericDocValues() throws IOException {
        testCase(new DocValuesFieldExistsQuery("number"), iw -> {
            iw.addDocument(singleton(new NumericDocValuesField("number", 60)));
            iw.addDocument(singleton(new NumericDocValuesField("number", 40)));
            iw.addDocument(singleton(new NumericDocValuesField("number", 20)));
            iw.addDocument(singleton(new NumericDocValuesField("number", 10)));
        }, hdr -> {
            assertEquals(4L, hdr.state.getTotalCount());
            double approximation = 0.05d;
            assertEquals(10.0d, hdr.percentile(25), approximation);
            assertEquals(20.0d, hdr.percentile(50), approximation);
            assertEquals(40.0d, hdr.percentile(75), approximation);
            assertEquals(60.0d, hdr.percentile(99), approximation);
            assertTrue(AggregationInspectionHelper.hasValue(hdr));
        });
    }

    public void testQueryFiltering() throws IOException {
        final CheckedConsumer<RandomIndexWriter, IOException> docs = iw -> {
            iw.addDocument(asList(new LongPoint("row", 4), new SortedNumericDocValuesField("number", 60)));
            iw.addDocument(asList(new LongPoint("row", 3), new SortedNumericDocValuesField("number", 40)));
            iw.addDocument(asList(new LongPoint("row", 2), new SortedNumericDocValuesField("number", 20)));
            iw.addDocument(asList(new LongPoint("row", 1), new SortedNumericDocValuesField("number", 10)));
        };

        testCase(LongPoint.newRangeQuery("row", 0, 2), docs, hdr -> {
            assertEquals(2L, hdr.state.getTotalCount());
            assertEquals(10.0d, hdr.percentile(randomDoubleBetween(1, 50, true)), 0.05d);
            assertTrue(AggregationInspectionHelper.hasValue(hdr));
        });

        testCase(LongPoint.newRangeQuery("row", 5, 10), docs, hdr -> {
            assertEquals(0L, hdr.state.getTotalCount());
            assertFalse(AggregationInspectionHelper.hasValue(hdr));
        });
    }

    public void testHdrThenTdigestSettings() throws Exception {
        int sigDigits = randomIntBetween(1, 5);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> {
            percentiles("percentiles").numberOfSignificantValueDigits(sigDigits)
                .method(PercentilesMethod.HDR)
                .compression(100.0) // <-- this should trigger an exception
                .field("value");
        });
        assertThat(e.getMessage(), equalTo("Cannot set [compression] because the method has already been configured for HDRHistogram"));
    }

    private void testCase(Query query, CheckedConsumer<RandomIndexWriter, IOException> buildIndex, Consumer<InternalHDRPercentiles> verify)
        throws IOException {
        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("number", NumberFieldMapper.NumberType.LONG);
        testCase(query, buildIndex, verify, fieldType, "number");
    }

    private void testCase(
        Query query,
        CheckedConsumer<RandomIndexWriter, IOException> buildIndex,
        Consumer<InternalHDRPercentiles> verify,
        MappedFieldType fieldType,
        String fieldName
    ) throws IOException {
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {
                buildIndex.accept(indexWriter);
            }

            try (IndexReader indexReader = DirectoryReader.open(directory)) {
                IndexSearcher indexSearcher = newSearcher(indexReader, true, true);

                PercentilesAggregationBuilder builder;
                // TODO this randomization path should be removed when the old settings are removed
                if (randomBoolean()) {
                    builder = new PercentilesAggregationBuilder("test").field(fieldName).method(PercentilesMethod.HDR);
                } else {
                    PercentilesConfig hdr = new PercentilesConfig.Hdr();
                    builder = new PercentilesAggregationBuilder("test").field(fieldName).percentilesConfig(hdr);
                }

                HDRPercentilesAggregator aggregator = createAggregator(builder, indexSearcher, fieldType);
                aggregator.preCollection();
                indexSearcher.search(query, aggregator);
                aggregator.postCollection();
                verify.accept((InternalHDRPercentiles) aggregator.buildAggregation(0L));

            }
        }
    }
}
