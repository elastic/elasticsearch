/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.aggregations.bucket.histogram;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.aggregations.AggregationsPlugin;
import org.elasticsearch.aggregations.pipeline.DerivativePipelineAggregationBuilder;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.core.Strings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.BooleanFieldMapper;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.IpFieldMapper;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.MultiBucketConsumerService;
import org.elasticsearch.search.aggregations.bucket.DateHistogramAggregatorTestCase;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.histogram.HistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.InternalHistogram;
import org.elasticsearch.search.aggregations.bucket.range.InternalBinaryRange;
import org.elasticsearch.search.aggregations.bucket.range.InternalRange;
import org.elasticsearch.search.aggregations.bucket.range.IpRangeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.range.RangeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.InternalStats;
import org.elasticsearch.search.aggregations.metrics.Max;
import org.elasticsearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.InternalSimpleValue;
import org.elasticsearch.search.aggregations.support.AggregationInspectionHelper;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.YearMonth;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.elasticsearch.test.ListMatcher.matchesList;
import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasSize;

public class AutoDateHistogramAggregatorTests extends DateHistogramAggregatorTestCase {
    private static final String DATE_FIELD = "date";
    private static final String INSTANT_FIELD = "instant";
    private static final String NUMERIC_FIELD = "numeric";
    private static final String IP_FIELD = "ip";

    private static final List<ZonedDateTime> DATES_WITH_TIME = Arrays.asList(
        ZonedDateTime.of(2010, 3, 12, 1, 7, 45, 0, ZoneOffset.UTC),
        ZonedDateTime.of(2010, 4, 27, 3, 43, 34, 0, ZoneOffset.UTC),
        ZonedDateTime.of(2012, 5, 18, 4, 11, 0, 0, ZoneOffset.UTC),
        ZonedDateTime.of(2013, 5, 29, 5, 11, 31, 0, ZoneOffset.UTC),
        ZonedDateTime.of(2013, 10, 31, 8, 24, 5, 0, ZoneOffset.UTC),
        ZonedDateTime.of(2015, 2, 13, 13, 9, 32, 0, ZoneOffset.UTC),
        ZonedDateTime.of(2015, 6, 24, 13, 47, 43, 0, ZoneOffset.UTC),
        ZonedDateTime.of(2015, 11, 13, 16, 14, 34, 0, ZoneOffset.UTC),
        ZonedDateTime.of(2016, 3, 4, 17, 9, 50, 0, ZoneOffset.UTC),
        ZonedDateTime.of(2017, 12, 12, 22, 55, 46, 0, ZoneOffset.UTC)
    );

    private static final Query DEFAULT_QUERY = new MatchAllDocsQuery();

    // TODO: remove when moving DateHistogramAggregatorTestCase to aggregations module
    @Override
    protected List<SearchPlugin> getSearchPlugins() {
        return List.of(new AggregationsPlugin());
    }

    public void testMatchNoDocs() throws IOException {
        testSearchCase(
            new MatchNoDocsQuery(),
            DATES_WITH_TIME,
            aggregation -> aggregation.setNumBuckets(10).field(DATE_FIELD),
            histogram -> {
                assertEquals(0, histogram.getBuckets().size());
                assertFalse(AggregationInspectionHelper.hasValue(histogram));
            }
        );
    }

    public void testMatchAllDocs() throws IOException {
        Map<String, Integer> expectedDocCount = new TreeMap<>();
        expectedDocCount.put("2010-01-01T00:00:00.000Z", 2);
        expectedDocCount.put("2012-01-01T00:00:00.000Z", 1);
        expectedDocCount.put("2013-01-01T00:00:00.000Z", 2);
        expectedDocCount.put("2015-01-01T00:00:00.000Z", 3);
        expectedDocCount.put("2016-01-01T00:00:00.000Z", 1);
        expectedDocCount.put("2017-01-01T00:00:00.000Z", 1);
        expectedDocCount.put("2011-01-01T00:00:00.000Z", 0);
        expectedDocCount.put("2014-01-01T00:00:00.000Z", 0);
        testSearchCase(
            DEFAULT_QUERY,
            DATES_WITH_TIME,
            aggregation -> aggregation.setNumBuckets(8).field(DATE_FIELD),
            result -> assertThat(bucketCountsAsMap(result), equalTo(expectedDocCount))
        );
    }

    public void testSubAggregations() throws IOException {
        testSearchCase(
            DEFAULT_QUERY,
            DATES_WITH_TIME,
            aggregation -> aggregation.setNumBuckets(8)
                .field(DATE_FIELD)
                .subAggregation(AggregationBuilders.stats("stats").field(DATE_FIELD)),
            histogram -> {
                assertTrue(AggregationInspectionHelper.hasValue(histogram));
                final List<? extends Histogram.Bucket> buckets = histogram.getBuckets();
                assertEquals(8, buckets.size());

                Histogram.Bucket bucket = buckets.get(0);
                assertEquals("2010-01-01T00:00:00.000Z", bucket.getKeyAsString());
                assertEquals(2, bucket.getDocCount());
                InternalStats stats = bucket.getAggregations().get("stats");
                assertEquals("2010-03-12T01:07:45.000Z", stats.getMinAsString());
                assertEquals("2010-04-27T03:43:34.000Z", stats.getMaxAsString());
                assertEquals(2L, stats.getCount());
                assertTrue(AggregationInspectionHelper.hasValue(stats));

                bucket = buckets.get(1);
                assertEquals("2011-01-01T00:00:00.000Z", bucket.getKeyAsString());
                assertEquals(0, bucket.getDocCount());
                stats = bucket.getAggregations().get("stats");
                assertTrue(Double.isInfinite(stats.getMin()));
                assertTrue(Double.isInfinite(stats.getMax()));
                assertEquals(0L, stats.getCount());
                assertFalse(AggregationInspectionHelper.hasValue(stats));

                bucket = buckets.get(2);
                assertEquals("2012-01-01T00:00:00.000Z", bucket.getKeyAsString());
                assertEquals(1, bucket.getDocCount());
                stats = bucket.getAggregations().get("stats");
                assertEquals("2012-05-18T04:11:00.000Z", stats.getMinAsString());
                assertEquals("2012-05-18T04:11:00.000Z", stats.getMaxAsString());
                assertEquals(1L, stats.getCount());
                assertTrue(AggregationInspectionHelper.hasValue(stats));

                bucket = buckets.get(3);
                assertEquals("2013-01-01T00:00:00.000Z", bucket.getKeyAsString());
                assertEquals(2, bucket.getDocCount());
                stats = bucket.getAggregations().get("stats");
                assertEquals("2013-05-29T05:11:31.000Z", stats.getMinAsString());
                assertEquals("2013-10-31T08:24:05.000Z", stats.getMaxAsString());
                assertEquals(2L, stats.getCount());
                assertTrue(AggregationInspectionHelper.hasValue(stats));

                bucket = buckets.get(4);
                assertEquals("2014-01-01T00:00:00.000Z", bucket.getKeyAsString());
                assertEquals(0, bucket.getDocCount());
                stats = bucket.getAggregations().get("stats");
                assertTrue(Double.isInfinite(stats.getMin()));
                assertTrue(Double.isInfinite(stats.getMax()));
                assertEquals(0L, stats.getCount());
                assertFalse(AggregationInspectionHelper.hasValue(stats));

                bucket = buckets.get(5);
                assertEquals("2015-01-01T00:00:00.000Z", bucket.getKeyAsString());
                assertEquals(3, bucket.getDocCount());
                stats = bucket.getAggregations().get("stats");
                assertEquals("2015-02-13T13:09:32.000Z", stats.getMinAsString());
                assertEquals("2015-11-13T16:14:34.000Z", stats.getMaxAsString());
                assertEquals(3L, stats.getCount());
                assertTrue(AggregationInspectionHelper.hasValue(stats));

                bucket = buckets.get(6);
                assertEquals("2016-01-01T00:00:00.000Z", bucket.getKeyAsString());
                assertEquals(1, bucket.getDocCount());
                stats = bucket.getAggregations().get("stats");
                assertEquals("2016-03-04T17:09:50.000Z", stats.getMinAsString());
                assertEquals("2016-03-04T17:09:50.000Z", stats.getMaxAsString());
                assertEquals(1L, stats.getCount());
                assertTrue(AggregationInspectionHelper.hasValue(stats));

                bucket = buckets.get(7);
                assertEquals("2017-01-01T00:00:00.000Z", bucket.getKeyAsString());
                assertEquals(1, bucket.getDocCount());
                stats = bucket.getAggregations().get("stats");
                assertEquals("2017-12-12T22:55:46.000Z", stats.getMinAsString());
                assertEquals("2017-12-12T22:55:46.000Z", stats.getMaxAsString());
                assertEquals(1L, stats.getCount());
                assertTrue(AggregationInspectionHelper.hasValue(stats));
            }
        );
    }

    public void testAsSubAgg() throws IOException {
        AggregationBuilder builder = new TermsAggregationBuilder("k1").field("k1")
            .subAggregation(
                new AutoDateHistogramAggregationBuilder("dh").field(AGGREGABLE_DATE)
                    .setNumBuckets(3)
                    .subAggregation(new MaxAggregationBuilder("max").field("n"))
            );
        asSubAggTestCase(builder, (StringTerms terms) -> {
            StringTerms.Bucket a = terms.getBucketByKey("a");
            InternalAutoDateHistogram adh = a.getAggregations().get("dh");
            Map<String, Integer> expectedDocCount = new TreeMap<>();
            expectedDocCount.put("2020-01-01T00:00:00.000Z", 2);
            expectedDocCount.put("2021-01-01T00:00:00.000Z", 2);
            assertThat(bucketCountsAsMap(adh), equalTo(expectedDocCount));
            Map<String, Double> expectedMax = new TreeMap<>();
            expectedMax.put("2020-01-01T00:00:00.000Z", 2.0);
            expectedMax.put("2021-01-01T00:00:00.000Z", 4.0);
            assertThat(maxAsMap(adh), equalTo(expectedMax));

            StringTerms.Bucket b = terms.getBucketByKey("b");
            InternalAutoDateHistogram bdh = b.getAggregations().get("dh");
            expectedDocCount.clear();
            expectedDocCount.put("2020-02-01T00:00:00.000Z", 1);
            assertThat(bucketCountsAsMap(bdh), equalTo(expectedDocCount));
            expectedMax.clear();
            expectedMax.put("2020-02-01T00:00:00.000Z", 5.0);
            assertThat(maxAsMap(bdh), equalTo(expectedMax));
        });
        builder = new TermsAggregationBuilder("k2").field("k2").subAggregation(builder);
        asSubAggTestCase(builder, (StringTerms terms) -> {
            StringTerms.Bucket a = terms.getBucketByKey("a");
            StringTerms ak1 = a.getAggregations().get("k1");
            StringTerms.Bucket ak1a = ak1.getBucketByKey("a");
            InternalAutoDateHistogram ak1adh = ak1a.getAggregations().get("dh");
            Map<String, Integer> expectedDocCount = new TreeMap<>();
            expectedDocCount.put("2020-01-01T00:00:00.000Z", 2);
            expectedDocCount.put("2021-01-01T00:00:00.000Z", 1);
            assertThat(bucketCountsAsMap(ak1adh), equalTo(expectedDocCount));
            Map<String, Double> expectedMax = new TreeMap<>();
            expectedMax.put("2020-01-01T00:00:00.000Z", 2.0);
            expectedMax.put("2021-01-01T00:00:00.000Z", 3.0);
            assertThat(maxAsMap(ak1adh), equalTo(expectedMax));

            StringTerms.Bucket b = terms.getBucketByKey("b");
            StringTerms bk1 = b.getAggregations().get("k1");
            StringTerms.Bucket bk1a = bk1.getBucketByKey("a");
            InternalAutoDateHistogram bk1adh = bk1a.getAggregations().get("dh");
            expectedDocCount.clear();
            expectedDocCount.put("2021-03-01T00:00:00.000Z", 1);
            assertThat(bucketCountsAsMap(bk1adh), equalTo(expectedDocCount));
            expectedMax.clear();
            expectedMax.put("2021-03-01T00:00:00.000Z", 4.0);
            assertThat(maxAsMap(bk1adh), equalTo(expectedMax));
            StringTerms.Bucket bk1b = bk1.getBucketByKey("b");
            InternalAutoDateHistogram bk1bdh = bk1b.getAggregations().get("dh");
            expectedDocCount.clear();
            expectedDocCount.put("2020-02-01T00:00:00.000Z", 1);
            assertThat(bucketCountsAsMap(bk1bdh), equalTo(expectedDocCount));
            expectedMax.clear();
            expectedMax.put("2020-02-01T00:00:00.000Z", 5.0);
            assertThat(maxAsMap(bk1bdh), equalTo(expectedMax));
        });
    }

    public void testAsSubAggWithIncreasedRounding() throws IOException {
        CheckedBiConsumer<RandomIndexWriter, DateFieldMapper.DateFieldType, IOException> buildIndex = (iw, dft) -> {
            long start = dft.parse("2020-01-01T00:00:00Z");
            long end = dft.parse("2021-01-01T00:00:00Z");
            long useC = dft.parse("2020-07-01T00:00Z");
            long anHour = dft.resolution().convert(Instant.ofEpochSecond(TimeUnit.HOURS.toSeconds(1)));
            List<List<IndexableField>> docs = new ArrayList<>();
            BytesRef aBytes = new BytesRef("a");
            BytesRef bBytes = new BytesRef("b");
            BytesRef cBytes = new BytesRef("c");
            int n = 0;
            for (long d = start; d < end; d += anHour) {
                docs.add(
                    List.of(
                        new SortedNumericDocValuesField(AGGREGABLE_DATE, d),
                        new Field("k1", aBytes, KeywordFieldMapper.Defaults.FIELD_TYPE),
                        new Field("k1", d < useC ? bBytes : cBytes, KeywordFieldMapper.Defaults.FIELD_TYPE),
                        new SortedNumericDocValuesField("n", n++)
                    )
                );
            }
            /*
             * Intentionally add all documents at once to put them on the
             * same shard to make the reduce behavior consistent.
             */
            iw.addDocuments(docs);
        };
        AggregationBuilder builder = new TermsAggregationBuilder("k1").field("k1")
            .subAggregation(
                new AutoDateHistogramAggregationBuilder("dh").field(AGGREGABLE_DATE)
                    .setNumBuckets(4)
                    .subAggregation(new MaxAggregationBuilder("max").field("n"))
            );
        asSubAggTestCase(builder, buildIndex, (StringTerms terms) -> {
            StringTerms.Bucket a = terms.getBucketByKey("a");
            InternalAutoDateHistogram adh = a.getAggregations().get("dh");
            Map<String, Integer> expectedDocCount = new TreeMap<>();
            expectedDocCount.put("2020-01-01T00:00:00.000Z", 2184);
            expectedDocCount.put("2020-04-01T00:00:00.000Z", 2184);
            expectedDocCount.put("2020-07-01T00:00:00.000Z", 2208);
            expectedDocCount.put("2020-10-01T00:00:00.000Z", 2208);
            assertThat(bucketCountsAsMap(adh), equalTo(expectedDocCount));
            Map<String, Double> expectedMax = new TreeMap<>();
            expectedMax.put("2020-01-01T00:00:00.000Z", 2183.0);
            expectedMax.put("2020-04-01T00:00:00.000Z", 4367.0);
            expectedMax.put("2020-07-01T00:00:00.000Z", 6575.0);
            expectedMax.put("2020-10-01T00:00:00.000Z", 8783.0);
            assertThat(maxAsMap(adh), equalTo(expectedMax));

            StringTerms.Bucket b = terms.getBucketByKey("b");
            InternalAutoDateHistogram bdh = b.getAggregations().get("dh");
            expectedDocCount.clear();
            expectedDocCount.put("2020-01-01T00:00:00.000Z", 2184);
            expectedDocCount.put("2020-04-01T00:00:00.000Z", 2184);
            assertThat(bucketCountsAsMap(bdh), equalTo(expectedDocCount));
            expectedMax.clear();
            expectedMax.put("2020-01-01T00:00:00.000Z", 2183.0);
            expectedMax.put("2020-04-01T00:00:00.000Z", 4367.0);
            assertThat(maxAsMap(bdh), equalTo(expectedMax));

            StringTerms.Bucket c = terms.getBucketByKey("c");
            InternalAutoDateHistogram cdh = c.getAggregations().get("dh");
            expectedDocCount.clear();
            expectedDocCount.put("2020-07-01T00:00:00.000Z", 2208);
            expectedDocCount.put("2020-10-01T00:00:00.000Z", 2208);
            assertThat(bucketCountsAsMap(cdh), equalTo(expectedDocCount));
            expectedMax.clear();
            expectedMax.put("2020-07-01T00:00:00.000Z", 6575.0);
            expectedMax.put("2020-10-01T00:00:00.000Z", 8783.0);
            assertThat(maxAsMap(cdh), equalTo(expectedMax));
        });
    }

    public void testAsSubAggInManyBuckets() throws IOException {
        CheckedBiConsumer<RandomIndexWriter, DateFieldMapper.DateFieldType, IOException> buildIndex = (iw, dft) -> {
            long start = dft.parse("2020-01-01T00:00:00Z");
            long end = dft.parse("2021-01-01T00:00:00Z");
            long anHour = dft.resolution().convert(Instant.ofEpochSecond(TimeUnit.HOURS.toSeconds(1)));
            List<List<IndexableField>> docs = new ArrayList<>();
            int n = 0;
            for (long d = start; d < end; d += anHour) {
                docs.add(List.of(new SortedNumericDocValuesField(AGGREGABLE_DATE, d), new SortedNumericDocValuesField("n", n % 100)));
                n++;
            }
            /*
             * Intentionally add all documents at once to put them on the
             * same shard to make the reduce behavior consistent.
             */
            iw.addDocuments(docs);
        };
        AggregationBuilder builder = new HistogramAggregationBuilder("n").field("n")
            .interval(1)
            .subAggregation(
                new AutoDateHistogramAggregationBuilder("dh").field(AGGREGABLE_DATE)
                    .setNumBuckets(4)
                    .subAggregation(new MaxAggregationBuilder("max").field("n"))
            );
        asSubAggTestCase(builder, buildIndex, (InternalHistogram histo) -> {
            assertThat(histo.getBuckets(), hasSize(100));
            for (int n = 0; n < 100; n++) {
                InternalHistogram.Bucket b = histo.getBuckets().get(n);
                InternalAutoDateHistogram dh = b.getAggregations().get("dh");
                assertThat(bucketCountsAsMap(dh), hasEntry(equalTo("2020-01-01T00:00:00.000Z"), either(equalTo(21)).or(equalTo(22))));
                assertThat(bucketCountsAsMap(dh), hasEntry(equalTo("2020-04-01T00:00:00.000Z"), either(equalTo(21)).or(equalTo(22))));
                assertThat(bucketCountsAsMap(dh), hasEntry(equalTo("2020-07-01T00:00:00.000Z"), either(equalTo(22)).or(equalTo(23))));
                assertThat(bucketCountsAsMap(dh), hasEntry(equalTo("2020-10-01T00:00:00.000Z"), either(equalTo(22)).or(equalTo(23))));
                Map<String, Double> expectedMax = new TreeMap<>();
                expectedMax.put("2020-01-01T00:00:00.000Z", (double) n);
                expectedMax.put("2020-04-01T00:00:00.000Z", (double) n);
                expectedMax.put("2020-07-01T00:00:00.000Z", (double) n);
                expectedMax.put("2020-10-01T00:00:00.000Z", (double) n);
                assertThat(maxAsMap(dh), equalTo(expectedMax));
            }
        });
    }

    public void testNoDocs() throws IOException {
        final List<ZonedDateTime> dates = Collections.emptyList();
        final Consumer<AutoDateHistogramAggregationBuilder> aggregation = agg -> agg.setNumBuckets(10).field(DATE_FIELD);

        testSearchCase(DEFAULT_QUERY, dates, aggregation, histogram -> {
            assertEquals(0, histogram.getBuckets().size());
            assertFalse(AggregationInspectionHelper.hasValue(histogram));
        });
        testSearchCase(DEFAULT_QUERY, dates, aggregation, histogram -> {
            assertEquals(0, histogram.getBuckets().size());
            assertFalse(AggregationInspectionHelper.hasValue(histogram));
        });
    }

    public void testAggregateWrongField() throws IOException {
        AutoDateHistogramAggregationBuilder aggregation = new AutoDateHistogramAggregationBuilder("_name").setNumBuckets(10)
            .field("bogus_bogus");

        final DateFieldMapper.DateFieldType fieldType = new DateFieldMapper.DateFieldType("date_field");

        testCase(iw -> {}, (Consumer<InternalAutoDateHistogram>) histogram -> {
            assertEquals(0, histogram.getBuckets().size());
            assertFalse(AggregationInspectionHelper.hasValue(histogram));
        }, new AggTestConfig(aggregation, fieldType).withQuery(DEFAULT_QUERY));
    }

    public void testBooleanFieldDeprecated() throws IOException {
        final String fieldName = "bogusBoolean";
        testCase(iw -> {
            Document d = new Document();
            d.add(new SortedNumericDocValuesField(fieldName, 0));
            iw.addDocument(d);
        },
            a -> {},
            new AggTestConfig(
                new AutoDateHistogramAggregationBuilder("name").field(fieldName),
                new BooleanFieldMapper.BooleanFieldType(fieldName)
            )
        );
        assertWarnings("Running AutoIntervalDateHistogram aggregations on [boolean] fields is deprecated");
    }

    public void testUnmappedMissing() throws IOException {
        AutoDateHistogramAggregationBuilder aggregation = new AutoDateHistogramAggregationBuilder("_name").setNumBuckets(10)
            .field("bogus_bogus")
            .missing("2017-12-12");

        final DateFieldMapper.DateFieldType fieldType = new DateFieldMapper.DateFieldType("date_field");

        testCase(iw -> {}, (Consumer<InternalAutoDateHistogram>) histogram -> {
            assertEquals(0, histogram.getBuckets().size());
            assertFalse(AggregationInspectionHelper.hasValue(histogram));
        }, new AggTestConfig(aggregation, fieldType).withQuery(DEFAULT_QUERY));
    }

    public void testIntervalYear() throws IOException {
        final long start = LocalDate.of(2015, 1, 1).atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
        final long end = LocalDate.of(2017, 12, 31).atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
        final Query rangeQuery = LongPoint.newRangeQuery(INSTANT_FIELD, start, end);
        testSearchCase(rangeQuery, DATES_WITH_TIME, aggregation -> aggregation.setNumBuckets(4).field(DATE_FIELD), histogram -> {
            final ZonedDateTime startDate = ZonedDateTime.of(2015, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC);
            final Map<ZonedDateTime, Integer> expectedDocCount = new HashMap<>();
            expectedDocCount.put(startDate, 3);
            expectedDocCount.put(startDate.plusYears(1), 1);
            expectedDocCount.put(startDate.plusYears(2), 1);
            final List<? extends Histogram.Bucket> buckets = histogram.getBuckets();
            assertEquals(expectedDocCount.size(), buckets.size());
            buckets.forEach(bucket -> assertEquals(expectedDocCount.getOrDefault(bucket.getKey(), 0).longValue(), bucket.getDocCount()));
            assertTrue(AggregationInspectionHelper.hasValue(histogram));
        });
    }

    public void testIntervalMonth() throws IOException {
        final List<ZonedDateTime> datesForMonthInterval = Arrays.asList(
            ZonedDateTime.of(2017, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC),
            ZonedDateTime.of(2017, 2, 2, 0, 0, 0, 0, ZoneOffset.UTC),
            ZonedDateTime.of(2017, 2, 3, 0, 0, 0, 0, ZoneOffset.UTC),
            ZonedDateTime.of(2017, 3, 4, 0, 0, 0, 0, ZoneOffset.UTC),
            ZonedDateTime.of(2017, 3, 5, 0, 0, 0, 0, ZoneOffset.UTC),
            ZonedDateTime.of(2017, 3, 6, 0, 0, 0, 0, ZoneOffset.UTC)
        );
        Map<String, Integer> expectedDocCount = new TreeMap<>();
        expectedDocCount.put("2017-01-01T00:00:00.000Z", 1);
        expectedDocCount.put("2017-02-01T00:00:00.000Z", 2);
        expectedDocCount.put("2017-03-01T00:00:00.000Z", 3);
        testSearchCase(
            DEFAULT_QUERY,
            datesForMonthInterval,
            aggregation -> aggregation.setNumBuckets(4).field(DATE_FIELD),
            result -> assertThat(bucketCountsAsMap(result), equalTo(expectedDocCount))
        );
    }

    public void testWithLargeNumberOfBuckets() {
        final IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> testSearchCase(
                DEFAULT_QUERY,
                DATES_WITH_TIME,
                aggregation -> aggregation.setNumBuckets(MultiBucketConsumerService.DEFAULT_MAX_BUCKETS + 1).field(DATE_FIELD),
                // since an exception is thrown, this assertion won't be invoked.
                histogram -> fail()
            )
        );
        assertThat(exception.getMessage(), Matchers.containsString("must be less than"));
    }

    public void testIntervalDay() throws IOException {
        final List<ZonedDateTime> datesForDayInterval = Arrays.asList(
            ZonedDateTime.of(2017, 2, 1, 0, 0, 0, 0, ZoneOffset.UTC),
            ZonedDateTime.of(2017, 2, 2, 0, 0, 0, 0, ZoneOffset.UTC),
            ZonedDateTime.of(2017, 2, 2, 0, 0, 0, 0, ZoneOffset.UTC),
            ZonedDateTime.of(2017, 2, 3, 0, 0, 0, 0, ZoneOffset.UTC),
            ZonedDateTime.of(2017, 2, 3, 0, 0, 0, 0, ZoneOffset.UTC),
            ZonedDateTime.of(2017, 2, 3, 0, 0, 0, 0, ZoneOffset.UTC),
            ZonedDateTime.of(2017, 2, 5, 0, 0, 0, 0, ZoneOffset.UTC)
        );
        Map<String, Integer> expectedDocCount = new TreeMap<>();
        expectedDocCount.put("2017-02-01T00:00:00.000Z", 1);
        expectedDocCount.put("2017-02-02T00:00:00.000Z", 2);
        expectedDocCount.put("2017-02-03T00:00:00.000Z", 3);
        expectedDocCount.put("2017-02-05T00:00:00.000Z", 1);
        expectedDocCount.put("2017-02-04T00:00:00.000Z", 0);
        testSearchCase(
            DEFAULT_QUERY,
            datesForDayInterval,
            aggregation -> aggregation.setNumBuckets(5).field(DATE_FIELD),
            result -> assertThat(bucketCountsAsMap(result), equalTo(expectedDocCount))
        );
    }

    public void testIntervalDayWithTZ() throws IOException {
        final List<ZonedDateTime> datesForDayInterval = Arrays.asList(
            ZonedDateTime.of(2017, 2, 1, 0, 0, 0, 0, ZoneOffset.UTC),
            ZonedDateTime.of(2017, 2, 2, 0, 0, 0, 0, ZoneOffset.UTC),
            ZonedDateTime.of(2017, 2, 2, 0, 0, 0, 0, ZoneOffset.UTC),
            ZonedDateTime.of(2017, 2, 3, 0, 0, 0, 0, ZoneOffset.UTC),
            ZonedDateTime.of(2017, 2, 3, 0, 0, 0, 0, ZoneOffset.UTC),
            ZonedDateTime.of(2017, 2, 3, 0, 0, 0, 0, ZoneOffset.UTC),
            ZonedDateTime.of(2017, 2, 5, 0, 0, 0, 0, ZoneOffset.UTC)
        );
        Map<String, Integer> expectedDocCount = new TreeMap<>();
        expectedDocCount.put("2017-01-31T00:00:00.000-01:00", 1);
        expectedDocCount.put("2017-02-01T00:00:00.000-01:00", 2);
        expectedDocCount.put("2017-02-02T00:00:00.000-01:00", 3);
        expectedDocCount.put("2017-02-04T00:00:00.000-01:00", 1);
        expectedDocCount.put("2017-02-03T00:00:00.000-01:00", 0);
        testSearchCase(
            DEFAULT_QUERY,
            datesForDayInterval,
            aggregation -> aggregation.setNumBuckets(5).field(DATE_FIELD).timeZone(ZoneOffset.ofHours(-1)),
            result -> assertThat(bucketCountsAsMap(result), equalTo(expectedDocCount))
        );
    }

    public void testIntervalHour() throws IOException {
        final List<ZonedDateTime> datesForHourInterval = Arrays.asList(
            ZonedDateTime.of(2017, 2, 1, 9, 2, 0, 0, ZoneOffset.UTC),
            ZonedDateTime.of(2017, 2, 1, 9, 35, 0, 0, ZoneOffset.UTC),
            ZonedDateTime.of(2017, 2, 1, 10, 15, 0, 0, ZoneOffset.UTC),
            ZonedDateTime.of(2017, 2, 1, 13, 6, 0, 0, ZoneOffset.UTC),
            ZonedDateTime.of(2017, 2, 1, 14, 4, 0, 0, ZoneOffset.UTC),
            ZonedDateTime.of(2017, 2, 1, 14, 5, 0, 0, ZoneOffset.UTC),
            ZonedDateTime.of(2017, 2, 1, 15, 59, 0, 0, ZoneOffset.UTC),
            ZonedDateTime.of(2017, 2, 1, 16, 6, 0, 0, ZoneOffset.UTC),
            ZonedDateTime.of(2017, 2, 1, 16, 48, 0, 0, ZoneOffset.UTC),
            ZonedDateTime.of(2017, 2, 1, 16, 59, 0, 0, ZoneOffset.UTC)
        );
        Map<String, Integer> expectedDocCount = new TreeMap<>();
        expectedDocCount.put("2017-02-01T09:00:00.000Z", 2);
        expectedDocCount.put("2017-02-01T10:00:00.000Z", 1);
        expectedDocCount.put("2017-02-01T13:00:00.000Z", 1);
        expectedDocCount.put("2017-02-01T14:00:00.000Z", 2);
        expectedDocCount.put("2017-02-01T15:00:00.000Z", 1);
        expectedDocCount.put("2017-02-01T15:00:00.000Z", 1);
        expectedDocCount.put("2017-02-01T16:00:00.000Z", 3);
        expectedDocCount.put("2017-02-01T11:00:00.000Z", 0);
        expectedDocCount.put("2017-02-01T12:00:00.000Z", 0);
        testSearchCase(
            DEFAULT_QUERY,
            datesForHourInterval,
            aggregation -> aggregation.setNumBuckets(10).field(DATE_FIELD),
            result -> assertThat(bucketCountsAsMap(result), equalTo(expectedDocCount))
        );
        expectedDocCount.clear();
        expectedDocCount.put("2017-02-01T09:00:00.000Z", 3);
        expectedDocCount.put("2017-02-01T12:00:00.000Z", 3);
        expectedDocCount.put("2017-02-01T15:00:00.000Z", 4);
        testSearchCase(
            DEFAULT_QUERY,
            datesForHourInterval,
            aggregation -> aggregation.setNumBuckets(6).field(DATE_FIELD),
            result -> assertThat(bucketCountsAsMap(result), equalTo(expectedDocCount))
        );
    }

    public void testIntervalHourWithTZ() throws IOException {
        List<ZonedDateTime> datesForHourInterval = Arrays.asList(
            ZonedDateTime.of(2017, 2, 1, 9, 2, 0, 0, ZoneOffset.UTC),
            ZonedDateTime.of(2017, 2, 1, 9, 35, 0, 0, ZoneOffset.UTC),
            ZonedDateTime.of(2017, 2, 1, 10, 15, 0, 0, ZoneOffset.UTC),
            ZonedDateTime.of(2017, 2, 1, 13, 6, 0, 0, ZoneOffset.UTC),
            ZonedDateTime.of(2017, 2, 1, 14, 4, 0, 0, ZoneOffset.UTC),
            ZonedDateTime.of(2017, 2, 1, 14, 5, 0, 0, ZoneOffset.UTC),
            ZonedDateTime.of(2017, 2, 1, 15, 59, 0, 0, ZoneOffset.UTC),
            ZonedDateTime.of(2017, 2, 1, 16, 6, 0, 0, ZoneOffset.UTC),
            ZonedDateTime.of(2017, 2, 1, 16, 48, 0, 0, ZoneOffset.UTC),
            ZonedDateTime.of(2017, 2, 1, 16, 59, 0, 0, ZoneOffset.UTC)
        );
        Map<String, Integer> expectedDocCount = new TreeMap<>();
        expectedDocCount.put("2017-02-01T08:00:00.000-01:00", 2);
        expectedDocCount.put("2017-02-01T09:00:00.000-01:00", 1);
        expectedDocCount.put("2017-02-01T12:00:00.000-01:00", 1);
        expectedDocCount.put("2017-02-01T13:00:00.000-01:00", 2);
        expectedDocCount.put("2017-02-01T14:00:00.000-01:00", 1);
        expectedDocCount.put("2017-02-01T15:00:00.000-01:00", 3);
        expectedDocCount.put("2017-02-01T10:00:00.000-01:00", 0);
        expectedDocCount.put("2017-02-01T11:00:00.000-01:00", 0);
        testSearchCase(
            DEFAULT_QUERY,
            datesForHourInterval,
            aggregation -> aggregation.setNumBuckets(10).field(DATE_FIELD).timeZone(ZoneOffset.ofHours(-1)),
            result -> assertThat(bucketCountsAsMap(result), equalTo(expectedDocCount))
        );
    }

    public void testRandomSecondIntervals() throws IOException {
        final int length = 120;
        final List<ZonedDateTime> dataset = new ArrayList<>(length);
        final ZonedDateTime startDate = ZonedDateTime.of(2017, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC);
        for (int i = 0; i < length; i++) {
            final ZonedDateTime date = startDate.plusSeconds(i);
            dataset.add(date);
        }
        final Map<Integer, Integer> bucketsToExpectedDocCountMap = new HashMap<>();
        bucketsToExpectedDocCountMap.put(120, 1);
        bucketsToExpectedDocCountMap.put(60, 5);
        bucketsToExpectedDocCountMap.put(20, 10);
        bucketsToExpectedDocCountMap.put(10, 30);
        bucketsToExpectedDocCountMap.put(3, 60);
        final Map.Entry<Integer, Integer> randomEntry = randomFrom(bucketsToExpectedDocCountMap.entrySet());
        testSearchCase(
            DEFAULT_QUERY,
            dataset,
            aggregation -> aggregation.setNumBuckets(randomEntry.getKey()).field(DATE_FIELD),
            histogram -> {
                final List<? extends Histogram.Bucket> buckets = histogram.getBuckets();
                final int expectedDocCount = randomEntry.getValue();
                final int expectedSize = length / expectedDocCount;
                assertEquals(expectedSize, buckets.size());
                final int randomIndex = randomInt(expectedSize - 1);
                final Histogram.Bucket bucket = buckets.get(randomIndex);
                assertEquals(startDate.plusSeconds(randomIndex * expectedDocCount), bucket.getKey());
                assertEquals(expectedDocCount, bucket.getDocCount());
            }
        );
    }

    public void testRandomMinuteIntervals() throws IOException {
        final int length = 120;
        final List<ZonedDateTime> dataset = new ArrayList<>(length);
        final ZonedDateTime startDate = ZonedDateTime.of(2017, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC);
        for (int i = 0; i < length; i++) {
            final ZonedDateTime date = startDate.plusMinutes(i);
            dataset.add(date);
        }
        final Map<Integer, Integer> bucketsToExpectedDocCountMap = new HashMap<>();
        bucketsToExpectedDocCountMap.put(120, 1);
        bucketsToExpectedDocCountMap.put(60, 5);
        bucketsToExpectedDocCountMap.put(20, 10);
        bucketsToExpectedDocCountMap.put(10, 30);
        bucketsToExpectedDocCountMap.put(3, 60);
        final Map.Entry<Integer, Integer> randomEntry = randomFrom(bucketsToExpectedDocCountMap.entrySet());
        testSearchCase(
            DEFAULT_QUERY,
            dataset,
            aggregation -> aggregation.setNumBuckets(randomEntry.getKey()).field(DATE_FIELD),
            histogram -> {
                final List<? extends Histogram.Bucket> buckets = histogram.getBuckets();
                final int expectedDocCount = randomEntry.getValue();
                final int expectedSize = length / expectedDocCount;
                assertEquals(expectedSize, buckets.size());
                final int randomIndex = randomInt(expectedSize - 1);
                final Histogram.Bucket bucket = buckets.get(randomIndex);
                assertEquals(startDate.plusMinutes(randomIndex * expectedDocCount), bucket.getKey());
                assertEquals(expectedDocCount, bucket.getDocCount());
            }
        );
    }

    public void testRandomHourIntervals() throws IOException {
        final int length = 72;
        final List<ZonedDateTime> dataset = new ArrayList<>(length);
        final ZonedDateTime startDate = ZonedDateTime.of(2017, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC);
        for (int i = 0; i < length; i++) {
            final ZonedDateTime date = startDate.plusHours(i);
            dataset.add(date);
        }
        final Map<Integer, Integer> bucketsToExpectedDocCountMap = new HashMap<>();
        bucketsToExpectedDocCountMap.put(72, 1);
        bucketsToExpectedDocCountMap.put(36, 3);
        bucketsToExpectedDocCountMap.put(12, 12);
        bucketsToExpectedDocCountMap.put(3, 24);
        final Map.Entry<Integer, Integer> randomEntry = randomFrom(bucketsToExpectedDocCountMap.entrySet());
        testSearchCase(
            DEFAULT_QUERY,
            dataset,
            aggregation -> aggregation.setNumBuckets(randomEntry.getKey()).field(DATE_FIELD),
            histogram -> {
                final List<? extends Histogram.Bucket> buckets = histogram.getBuckets();
                final int expectedDocCount = randomEntry.getValue();
                final int expectedSize = length / expectedDocCount;
                assertEquals(expectedSize, buckets.size());
                final int randomIndex = randomInt(expectedSize - 1);
                final Histogram.Bucket bucket = buckets.get(randomIndex);
                assertEquals(startDate.plusHours(randomIndex * expectedDocCount), bucket.getKey());
                assertEquals(expectedDocCount, bucket.getDocCount());
            }
        );
    }

    public void testRandomDayIntervals() throws IOException {
        final int length = 140;
        final List<ZonedDateTime> dataset = new ArrayList<>(length);
        final ZonedDateTime startDate = ZonedDateTime.of(2017, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC);
        for (int i = 0; i < length; i++) {
            final ZonedDateTime date = startDate.plusDays(i);
            dataset.add(date);
        }
        final int randomChoice = randomIntBetween(1, 3);
        if (randomChoice == 1) {
            testSearchCase(DEFAULT_QUERY, dataset, aggregation -> aggregation.setNumBuckets(length).field(DATE_FIELD), histogram -> {
                final List<? extends Histogram.Bucket> buckets = histogram.getBuckets();
                assertEquals(length, buckets.size());
                final int randomIndex = randomInt(length - 1);
                final Histogram.Bucket bucket = buckets.get(randomIndex);
                assertEquals(startDate.plusDays(randomIndex), bucket.getKey());
                assertEquals(1, bucket.getDocCount());
            });
        } else if (randomChoice == 2) {
            testSearchCase(DEFAULT_QUERY, dataset, aggregation -> aggregation.setNumBuckets(60).field(DATE_FIELD), histogram -> {
                final List<? extends Histogram.Bucket> buckets = histogram.getBuckets();
                final int expectedDocCount = 7;
                assertEquals(20, buckets.size());
                final int randomIndex = randomInt(19);
                final Histogram.Bucket bucket = buckets.get(randomIndex);
                assertEquals(startDate.plusDays(randomIndex * expectedDocCount), bucket.getKey());
                assertEquals(expectedDocCount, bucket.getDocCount());
            });
        } else if (randomChoice == 3) {
            testSearchCase(DEFAULT_QUERY, dataset, aggregation -> aggregation.setNumBuckets(6).field(DATE_FIELD), histogram -> {
                final List<? extends Histogram.Bucket> buckets = histogram.getBuckets();
                assertEquals(5, buckets.size());
                final int randomIndex = randomInt(2);
                final Histogram.Bucket bucket = buckets.get(randomIndex);
                assertEquals(startDate.plusMonths(randomIndex), bucket.getKey());
                assertEquals(YearMonth.from(startDate.plusMonths(randomIndex)).lengthOfMonth(), bucket.getDocCount());
            });
        }
    }

    public void testRandomMonthIntervals() throws IOException {
        final int length = 60;
        final List<ZonedDateTime> dataset = new ArrayList<>(length);
        final ZonedDateTime startDate = ZonedDateTime.of(2017, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC);
        for (int i = 0; i < length; i++) {
            final ZonedDateTime date = startDate.plusMonths(i);
            dataset.add(date);
        }
        final Map<Integer, Integer> bucketsToExpectedDocCountMap = new HashMap<>();
        bucketsToExpectedDocCountMap.put(60, 1);
        bucketsToExpectedDocCountMap.put(30, 3);
        bucketsToExpectedDocCountMap.put(6, 12);
        final Map.Entry<Integer, Integer> randomEntry = randomFrom(bucketsToExpectedDocCountMap.entrySet());
        testSearchCase(
            DEFAULT_QUERY,
            dataset,
            aggregation -> aggregation.setNumBuckets(randomEntry.getKey()).field(DATE_FIELD),
            histogram -> {
                final List<? extends Histogram.Bucket> buckets = histogram.getBuckets();
                final int expectedDocCount = randomEntry.getValue();
                final int expectedSize = length / expectedDocCount;
                assertEquals(expectedSize, buckets.size());
                final int randomIndex = randomInt(expectedSize - 1);
                final Histogram.Bucket bucket = buckets.get(randomIndex);
                assertEquals(startDate.plusMonths(randomIndex * expectedDocCount), bucket.getKey());
                assertEquals(expectedDocCount, bucket.getDocCount());
            }
        );
    }

    public void testRandomYearIntervals() throws IOException {
        final int length = 300;
        final List<ZonedDateTime> dataset = new ArrayList<>(length);
        final ZonedDateTime startDate = ZonedDateTime.of(2017, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC);
        for (int i = 0; i < length; i++) {
            final ZonedDateTime date = startDate.plusYears(i);
            dataset.add(date);
        }
        final Map<Integer, Integer> bucketsToExpectedDocCountMap = new HashMap<>();
        bucketsToExpectedDocCountMap.put(300, 1);
        bucketsToExpectedDocCountMap.put(150, 5);
        bucketsToExpectedDocCountMap.put(50, 10);
        bucketsToExpectedDocCountMap.put(25, 20);
        bucketsToExpectedDocCountMap.put(10, 50);
        bucketsToExpectedDocCountMap.put(5, 100);
        final Map.Entry<Integer, Integer> randomEntry = randomFrom(bucketsToExpectedDocCountMap.entrySet());
        testSearchCase(
            DEFAULT_QUERY,
            dataset,
            aggregation -> aggregation.setNumBuckets(randomEntry.getKey()).field(DATE_FIELD),
            histogram -> {
                final List<? extends Histogram.Bucket> buckets = histogram.getBuckets();
                final int expectedDocCount = randomEntry.getValue();
                final int expectedSize = length / expectedDocCount;
                assertEquals(expectedSize, buckets.size());
                final int randomIndex = randomInt(expectedSize - 1);
                final Histogram.Bucket bucket = buckets.get(randomIndex);
                assertEquals(startDate.plusYears(randomIndex * expectedDocCount), bucket.getKey());
                assertEquals(expectedDocCount, bucket.getDocCount());
            }
        );
    }

    public void testIntervalMinute() throws IOException {
        final List<ZonedDateTime> datesForMinuteInterval = Arrays.asList(
            ZonedDateTime.of(2017, 2, 1, 9, 2, 35, 0, ZoneOffset.UTC),
            ZonedDateTime.of(2017, 2, 1, 9, 2, 59, 0, ZoneOffset.UTC),
            ZonedDateTime.of(2017, 2, 1, 9, 15, 37, 0, ZoneOffset.UTC),
            ZonedDateTime.of(2017, 2, 1, 9, 16, 4, 0, ZoneOffset.UTC),
            ZonedDateTime.of(2017, 2, 1, 9, 16, 42, 0, ZoneOffset.UTC)
        );
        Map<String, Integer> skeletonDocCount = new TreeMap<>();
        skeletonDocCount.put("2017-02-01T09:02:00.000Z", 2);
        skeletonDocCount.put("2017-02-01T09:15:00.000Z", 1);
        skeletonDocCount.put("2017-02-01T09:16:00.000Z", 2);
        Map<String, Integer> fullDocCount = new TreeMap<>();
        fullDocCount.put("2017-02-01T09:02:00.000Z", 2);
        fullDocCount.put("2017-02-01T09:07:00.000Z", 0);
        fullDocCount.put("2017-02-01T09:12:00.000Z", 3);
        testSearchCase(
            DEFAULT_QUERY,
            datesForMinuteInterval,
            aggregation -> aggregation.setNumBuckets(4).field(DATE_FIELD),
            result -> assertThat(bucketCountsAsMap(result), equalTo(fullDocCount))
        );
        fullDocCount.clear();
        fullDocCount.putAll(skeletonDocCount);
        for (int minute = 3; minute < 15; minute++) {
            fullDocCount.put(Strings.format("2017-02-01T09:%02d:00.000Z", minute), 0);
        }
        testSearchCase(
            DEFAULT_QUERY,
            datesForMinuteInterval,
            aggregation -> aggregation.setNumBuckets(15).field(DATE_FIELD),
            result -> assertThat(bucketCountsAsMap(result), equalTo(fullDocCount))
        );
    }

    public void testIntervalSecond() throws IOException {
        final List<ZonedDateTime> datesForSecondInterval = Arrays.asList(
            ZonedDateTime.of(2017, 2, 1, 0, 0, 5, 15, ZoneOffset.UTC),
            ZonedDateTime.of(2017, 2, 1, 0, 0, 7, 299, ZoneOffset.UTC),
            ZonedDateTime.of(2017, 2, 1, 0, 0, 7, 74, ZoneOffset.UTC),
            ZonedDateTime.of(2017, 2, 1, 0, 0, 11, 688, ZoneOffset.UTC),
            ZonedDateTime.of(2017, 2, 1, 0, 0, 11, 210, ZoneOffset.UTC),
            ZonedDateTime.of(2017, 2, 1, 0, 0, 11, 380, ZoneOffset.UTC)
        );
        Map<String, Integer> expectedDocCount = new TreeMap<>();
        expectedDocCount.put("2017-02-01T00:00:05.000Z", 1);
        expectedDocCount.put("2017-02-01T00:00:07.000Z", 2);
        expectedDocCount.put("2017-02-01T00:00:11.000Z", 3);
        expectedDocCount.put("2017-02-01T00:00:06.000Z", 0);
        expectedDocCount.put("2017-02-01T00:00:08.000Z", 0);
        expectedDocCount.put("2017-02-01T00:00:09.000Z", 0);
        expectedDocCount.put("2017-02-01T00:00:10.000Z", 0);
        testSearchCase(
            DEFAULT_QUERY,
            datesForSecondInterval,
            aggregation -> aggregation.setNumBuckets(7).field(DATE_FIELD),
            result -> assertThat(bucketCountsAsMap(result), equalTo(expectedDocCount))
        );
    }

    public void testWithPipelineReductions() throws IOException {
        testSearchCase(
            DEFAULT_QUERY,
            DATES_WITH_TIME,
            aggregation -> aggregation.setNumBuckets(1)
                .field(DATE_FIELD)
                .subAggregation(
                    AggregationBuilders.histogram("histo")
                        .field(NUMERIC_FIELD)
                        .interval(1)
                        .subAggregation(AggregationBuilders.max("max").field(NUMERIC_FIELD))
                        .subAggregation(new DerivativePipelineAggregationBuilder("deriv", "max"))
                ),
            histogram -> {
                assertTrue(AggregationInspectionHelper.hasValue(histogram));
                final List<? extends Histogram.Bucket> buckets = histogram.getBuckets();
                assertEquals(1, buckets.size());

                Histogram.Bucket bucket = buckets.get(0);
                assertEquals("2010-01-01T00:00:00.000Z", bucket.getKeyAsString());
                assertEquals(10, bucket.getDocCount());
                assertThat(bucket.getAggregations().asList().size(), equalTo(1));
                InternalHistogram histo = (InternalHistogram) bucket.getAggregations().asList().get(0);
                assertThat(histo.getBuckets().size(), equalTo(10));
                for (int i = 0; i < 10; i++) {
                    assertThat(histo.getBuckets().get(i).getKey(), equalTo((double) i));
                    assertThat(((Max) histo.getBuckets().get(i).getAggregations().get("max")).value(), equalTo((double) i));
                    if (i > 0) {
                        assertThat(
                            ((InternalSimpleValue) histo.getBuckets().get(i).getAggregations().get("deriv")).getValue(),
                            equalTo(1.0)
                        );
                    }
                }

            }
        );
    }

    public void testSubNumericRange() throws IOException {
        assertSubNumericRange(DATES_WITH_TIME, 2);
    }

    /**
     * Tests very few documents with a sub {@code range} agg which causes
     * us to collect in a very tight time range and then merge many of those
     * ranges together, thus merging unmapped {@code range} aggs with mapped
     * ones.
     */
    public void testSmallSubNumericRange() throws IOException {
        assertSubNumericRange(DATES_WITH_TIME.subList(0, 2), 1);
    }

    private void assertSubNumericRange(List<ZonedDateTime> dates, long firstBucketIpCount) throws IOException {
        MappedFieldType dateFieldType = new DateFieldMapper.DateFieldType(DATE_FIELD);
        MappedFieldType numericFieldType = new NumberFieldMapper.NumberFieldType(NUMERIC_FIELD, NumberFieldMapper.NumberType.LONG);

        AutoDateHistogramAggregationBuilder b = new AutoDateHistogramAggregationBuilder("a").field(DATE_FIELD)
            .subAggregation(new RangeAggregationBuilder("r").field(NUMERIC_FIELD).addRange(0, 2).addRange(3, 4));
        testCase(iw -> indexSampleData(dates, iw), (InternalAutoDateHistogram h) -> {
            InternalAutoDateHistogram.Bucket bucket = h.getBuckets().get(0);
            InternalRange<?, ?> range = bucket.getAggregations().get("r");
            assertMap(
                range.getBuckets().stream().map(InternalRange.Bucket::getKeyAsString).toList(),
                matchesList().item("0.0-2.0").item("3.0-4.0")
            );
            assertMap(
                range.getBuckets().stream().map(InternalRange.Bucket::getDocCount).toList(),
                matchesList().item(firstBucketIpCount).item(0L)
            );
        }, new AggTestConfig(b, dateFieldType, numericFieldType));
    }

    public void testSubIpRange() throws IOException {
        assertSubIpRange(DATES_WITH_TIME, 2);
    }

    /**
     * Tests very few documents with a sub {@code ip_range} agg which causes
     * us to collect in a very tight time range and then merge many of those
     * ranges together, thus merging unmapped {@code ip_range} aggs with mapped
     * ones.
     */
    public void testSmallSubIpRange() throws IOException {
        assertSubIpRange(DATES_WITH_TIME.subList(0, 2), 1);
    }

    private void assertSubIpRange(List<ZonedDateTime> dates, long firstBucketIpCount) throws IOException {
        MappedFieldType dateFieldType = new DateFieldMapper.DateFieldType(DATE_FIELD);
        MappedFieldType ipFieldType = new IpFieldMapper.IpFieldType(IP_FIELD);

        AutoDateHistogramAggregationBuilder b = new AutoDateHistogramAggregationBuilder("a").field(DATE_FIELD)
            .subAggregation(
                new IpRangeAggregationBuilder("r").field(IP_FIELD)
                    .addRange("192.168.0.0", "192.168.0.2")
                    .addRange("192.168.0.3", "192.168.0.4")
            );
        testCase(iw -> indexSampleData(dates, iw), (InternalAutoDateHistogram h) -> {
            InternalAutoDateHistogram.Bucket bucket = h.getBuckets().get(0);
            InternalBinaryRange range = bucket.getAggregations().get("r");
            assertMap(
                range.getBuckets().stream().map(InternalBinaryRange.Bucket::getKeyAsString).toList(),
                matchesList().item("192.168.0.0-192.168.0.2").item("192.168.0.3-192.168.0.4")
            );
            assertMap(
                range.getBuckets().stream().map(InternalBinaryRange.Bucket::getDocCount).toList(),
                matchesList().item(firstBucketIpCount).item(0L)
            );
        }, new AggTestConfig(b, dateFieldType, ipFieldType));
    }

    @Override
    protected IndexSettings createIndexSettings() {
        final Settings nodeSettings = Settings.builder().put("search.max_buckets", 25000).build();
        return new IndexSettings(
            IndexMetadata.builder("_index")
                .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT))
                .numberOfShards(1)
                .numberOfReplicas(0)
                .creationDate(System.currentTimeMillis())
                .build(),
            nodeSettings
        );
    }

    private void testSearchCase(
        final Query query,
        final List<ZonedDateTime> dataset,
        final Consumer<AutoDateHistogramAggregationBuilder> configure,
        final Consumer<InternalAutoDateHistogram> verify
    ) throws IOException {
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {
                indexSampleData(dataset, indexWriter);
            }

            try (DirectoryReader indexReader = DirectoryReader.open(directory)) {
                final IndexSearcher indexSearcher = newIndexSearcher(indexReader);

                final AutoDateHistogramAggregationBuilder aggregationBuilder = new AutoDateHistogramAggregationBuilder("_name");
                if (configure != null) {
                    configure.accept(aggregationBuilder);
                }

                final DateFieldMapper.DateFieldType fieldType = new DateFieldMapper.DateFieldType(aggregationBuilder.field());

                MappedFieldType instantFieldType = new NumberFieldMapper.NumberFieldType(INSTANT_FIELD, NumberFieldMapper.NumberType.LONG);
                MappedFieldType numericFieldType = new NumberFieldMapper.NumberFieldType(NUMERIC_FIELD, NumberFieldMapper.NumberType.LONG);

                final InternalAutoDateHistogram histogram = searchAndReduce(
                    indexSearcher,
                    new AggTestConfig(aggregationBuilder, fieldType, instantFieldType, numericFieldType).withQuery(query)
                );
                verify.accept(histogram);
            }
        }
    }

    private void indexSampleData(List<ZonedDateTime> dataset, RandomIndexWriter indexWriter) throws IOException {
        final Document document = new Document();
        int i = 0;
        for (final ZonedDateTime date : dataset) {
            final long instant = date.toInstant().toEpochMilli();
            document.add(new SortedNumericDocValuesField(DATE_FIELD, instant));
            document.add(new LongPoint(INSTANT_FIELD, instant));
            document.add(new SortedNumericDocValuesField(NUMERIC_FIELD, i));
            document.add(
                new SortedSetDocValuesField(
                    IP_FIELD,
                    new BytesRef(InetAddressPoint.encode(InetAddresses.forString("192.168.0." + (i % 256))))
                )
            );
            indexWriter.addDocument(document);
            document.clear();
            i += 1;
        }
    }

    private Map<String, Integer> bucketCountsAsMap(InternalAutoDateHistogram result) {
        Map<String, Integer> map = Maps.newLinkedHashMapWithExpectedSize(result.getBuckets().size());
        result.getBuckets().stream().forEach(b -> {
            Object old = map.put(b.getKeyAsString(), Math.toIntExact(b.getDocCount()));
            assertNull(old);
        });
        return map;
    }

    private Map<String, Double> maxAsMap(InternalAutoDateHistogram result) {
        Map<String, Double> map = Maps.newLinkedHashMapWithExpectedSize(result.getBuckets().size());
        result.getBuckets().stream().forEach(b -> {
            Max max = b.getAggregations().get("max");
            Object old = map.put(b.getKeyAsString(), max.value());
            assertNull(old);
        });
        return map;
    }

    @Override
    public void doAssertReducedMultiBucketConsumer(Aggregation agg, MultiBucketConsumerService.MultiBucketConsumer bucketConsumer) {
        /*
         * No-op.
         */
    }
}
