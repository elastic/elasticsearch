/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.rollup.job;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.store.Directory;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregation;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.DateHistogramValuesSourceBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.TermsValuesSourceBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.InternalNumericMetricsAggregation;
import org.elasticsearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.SumAggregationBuilder;
import org.elasticsearch.xpack.core.rollup.RollupField;
import org.elasticsearch.xpack.core.rollup.job.DateHistogramGroupConfig;
import org.elasticsearch.xpack.core.rollup.job.GroupConfig;
import org.elasticsearch.xpack.core.rollup.job.HistogramGroupConfig;
import org.elasticsearch.xpack.core.rollup.job.MetricConfig;
import org.elasticsearch.xpack.core.rollup.job.RollupIndexerJobStats;
import org.elasticsearch.xpack.core.rollup.job.TermsGroupConfig;
import org.joda.time.DateTime;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.Collections.singletonList;
import static org.elasticsearch.xpack.core.rollup.ConfigTestHelpers.randomDateHistogramGroupConfig;
import static org.elasticsearch.xpack.core.rollup.ConfigTestHelpers.randomGroupConfig;
import static org.elasticsearch.xpack.core.rollup.ConfigTestHelpers.randomHistogramGroupConfig;
import static org.elasticsearch.xpack.rollup.job.RollupIndexer.createAggregationBuilders;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class IndexerUtilsTests extends AggregatorTestCase {
    public void testMissingFields() throws IOException {
        String indexName = randomAlphaOfLengthBetween(1, 10);
        RollupIndexerJobStats stats = new RollupIndexerJobStats(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);

        String timestampField = "the_histo";
        String valueField = "the_avg";

        Directory directory = newDirectory();
        RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory);

        int numDocs = randomIntBetween(1,10);
        for (int i = 0; i < numDocs; i++) {
            Document document = new Document();
            long timestamp = new DateTime().minusDays(i).getMillis();
            document.add(new SortedNumericDocValuesField(timestampField, timestamp));
            document.add(new LongPoint(timestampField, timestamp));
            document.add(new SortedNumericDocValuesField(valueField, randomIntBetween(1,100)));
            indexWriter.addDocument(document);
        }

        indexWriter.close();

        IndexReader indexReader = DirectoryReader.open(directory);
        IndexSearcher indexSearcher = newIndexSearcher(indexReader);

        DateFieldMapper.DateFieldType timestampFieldType = new DateFieldMapper.DateFieldType(timestampField);
        MappedFieldType valueFieldType = new NumberFieldMapper.NumberFieldType(valueField, NumberFieldMapper.NumberType.LONG);

        // Setup the composite agg
        DateHistogramGroupConfig dateHistoGroupConfig
            = new DateHistogramGroupConfig.CalendarInterval(timestampField, DateHistogramInterval.DAY);
        CompositeAggregationBuilder compositeBuilder =
            new CompositeAggregationBuilder(RollupIndexer.AGGREGATION_NAME,
                RollupIndexer.createValueSourceBuilders(dateHistoGroupConfig));
        MetricConfig metricConfig = new MetricConfig("does_not_exist", singletonList("max"));
        List<AggregationBuilder> metricAgg = createAggregationBuilders(singletonList(metricConfig));
        metricAgg.forEach(compositeBuilder::subAggregation);

        Aggregator aggregator = createAggregator(compositeBuilder, indexSearcher, timestampFieldType, valueFieldType);
        aggregator.preCollection();
        indexSearcher.search(new MatchAllDocsQuery(), aggregator);
        aggregator.postCollection();
        CompositeAggregation composite = (CompositeAggregation) aggregator.buildTopLevel();
        indexReader.close();
        directory.close();

        final GroupConfig groupConfig = randomGroupConfig(random());
        List<IndexRequest> docs = IndexerUtils.processBuckets(composite, indexName, stats, groupConfig, "foo").collect(Collectors.toList());

        assertThat(docs.size(), equalTo(numDocs));
        for (IndexRequest doc : docs) {
            Map<String, Object> map = doc.sourceAsMap();
            assertNull(map.get("does_not_exist"));
            assertThat(map.get("the_histo." + DateHistogramAggregationBuilder.NAME + "." + RollupField.COUNT_FIELD), equalTo(1));
        }
    }

    public void testCorrectFields() throws IOException {
        String indexName = randomAlphaOfLengthBetween(1, 10);
        RollupIndexerJobStats stats = new RollupIndexerJobStats(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);

        String timestampField = "the_histo";
        String valueField = "the_avg";

        Directory directory = newDirectory();
        RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory);

        int numDocs = randomIntBetween(1,10);
        for (int i = 0; i < numDocs; i++) {
            Document document = new Document();
            long timestamp = new DateTime().minusDays(i).getMillis();
            document.add(new SortedNumericDocValuesField(timestampField, timestamp));
            document.add(new LongPoint(timestampField, timestamp));
            document.add(new SortedNumericDocValuesField(valueField, randomIntBetween(1,100)));
            indexWriter.addDocument(document);
        }

        indexWriter.close();

        IndexReader indexReader = DirectoryReader.open(directory);
        IndexSearcher indexSearcher = newIndexSearcher(indexReader);

        DateFieldMapper.DateFieldType timestampFieldType = new DateFieldMapper.DateFieldType(timestampField);
        MappedFieldType valueFieldType = new NumberFieldMapper.NumberFieldType(valueField, NumberFieldMapper.NumberType.LONG);

        // Setup the composite agg
        //TODO swap this over to DateHistoConfig.Builder once DateInterval is in
        DateHistogramValuesSourceBuilder dateHisto
                = new DateHistogramValuesSourceBuilder("the_histo." + DateHistogramAggregationBuilder.NAME)
                .field(timestampField)
                .fixedInterval(new DateHistogramInterval("1ms"));

        CompositeAggregationBuilder compositeBuilder = new CompositeAggregationBuilder(RollupIndexer.AGGREGATION_NAME,
                singletonList(dateHisto));

        MetricConfig metricConfig = new MetricConfig(valueField, singletonList("max"));
        List<AggregationBuilder> metricAgg = createAggregationBuilders(singletonList(metricConfig));
        metricAgg.forEach(compositeBuilder::subAggregation);

        Aggregator aggregator = createAggregator(compositeBuilder, indexSearcher, timestampFieldType, valueFieldType);
        aggregator.preCollection();
        indexSearcher.search(new MatchAllDocsQuery(), aggregator);
        aggregator.postCollection();
        CompositeAggregation composite = (CompositeAggregation) aggregator.buildTopLevel();
        indexReader.close();
        directory.close();

        final GroupConfig groupConfig = randomGroupConfig(random());
        List<IndexRequest> docs = IndexerUtils.processBuckets(composite, indexName, stats, groupConfig, "foo").collect(Collectors.toList());

        assertThat(docs.size(), equalTo(numDocs));
        for (IndexRequest doc : docs) {
            Map<String, Object> map = doc.sourceAsMap();
            assertNotNull( map.get(valueField + "." + MaxAggregationBuilder.NAME + "." + RollupField.VALUE));
            assertThat(map.get("the_histo." + DateHistogramAggregationBuilder.NAME + "." + RollupField.COUNT_FIELD), equalTo(1));
        }
    }

    public void testNumericTerms() throws IOException {
        String indexName = randomAlphaOfLengthBetween(1, 10);
        RollupIndexerJobStats stats= new RollupIndexerJobStats(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);

        String valueField = "the_avg";

        Directory directory = newDirectory();
        RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory);

        int numDocs = randomIntBetween(1,10);
        for (int i = 0; i < numDocs; i++) {
            Document document = new Document();
            document.add(new SortedNumericDocValuesField(valueField, i));
            document.add(new LongPoint(valueField, i));
            indexWriter.addDocument(document);
        }

        indexWriter.close();

        IndexReader indexReader = DirectoryReader.open(directory);
        IndexSearcher indexSearcher = newIndexSearcher(indexReader);

        MappedFieldType valueFieldType = new NumberFieldMapper.NumberFieldType(valueField, NumberFieldMapper.NumberType.LONG);

        // Setup the composite agg
        TermsValuesSourceBuilder terms
                = new TermsValuesSourceBuilder("the_terms." + TermsAggregationBuilder.NAME).field(valueField);
        CompositeAggregationBuilder compositeBuilder = new CompositeAggregationBuilder(RollupIndexer.AGGREGATION_NAME,
                singletonList(terms));

        MetricConfig metricConfig = new MetricConfig(valueField, singletonList("max"));
        List<AggregationBuilder> metricAgg = createAggregationBuilders(singletonList(metricConfig));
        metricAgg.forEach(compositeBuilder::subAggregation);

        Aggregator aggregator = createAggregator(compositeBuilder, indexSearcher, valueFieldType);
        aggregator.preCollection();
        indexSearcher.search(new MatchAllDocsQuery(), aggregator);
        aggregator.postCollection();
        CompositeAggregation composite = (CompositeAggregation) aggregator.buildTopLevel();
        indexReader.close();
        directory.close();

        final GroupConfig groupConfig = randomGroupConfig(random());
        List<IndexRequest> docs = IndexerUtils.processBuckets(composite, indexName, stats, groupConfig, "foo").collect(Collectors.toList());

        assertThat(docs.size(), equalTo(numDocs));
        for (IndexRequest doc : docs) {
            Map<String, Object> map = doc.sourceAsMap();
            assertNotNull( map.get(valueField + "." + MaxAggregationBuilder.NAME + "." + RollupField.VALUE));
            assertThat(map.get("the_terms." + TermsAggregationBuilder.NAME + "." + RollupField.COUNT_FIELD), equalTo(1));
        }
    }

    public void testEmptyCounts() throws IOException {
        String indexName = randomAlphaOfLengthBetween(1, 10);
        RollupIndexerJobStats stats = new RollupIndexerJobStats(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);

        String timestampField = "ts";
        String valueField = "the_avg";

        Directory directory = newDirectory();
        RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory);

        int numDocs = randomIntBetween(1,10);
        for (int i = 0; i < numDocs; i++) {
            Document document = new Document();
            long timestamp = new DateTime().minusDays(i).getMillis();
            document.add(new SortedNumericDocValuesField(timestampField, timestamp));
            document.add(new LongPoint(timestampField, timestamp));
            document.add(new SortedNumericDocValuesField(valueField, randomIntBetween(1,100)));
            indexWriter.addDocument(document);
        }

        indexWriter.close();

        IndexReader indexReader = DirectoryReader.open(directory);
        IndexSearcher indexSearcher = newIndexSearcher(indexReader);

        DateFieldMapper.DateFieldType timestampFieldType = new DateFieldMapper.DateFieldType(timestampField);
        MappedFieldType valueFieldType = new NumberFieldMapper.NumberFieldType(valueField, NumberFieldMapper.NumberType.LONG);

        // Setup the composite agg
        DateHistogramValuesSourceBuilder dateHisto
                = new DateHistogramValuesSourceBuilder("the_histo." + DateHistogramAggregationBuilder.NAME)
                    .field(timestampField)
                    .calendarInterval(new DateHistogramInterval("1d"));

        CompositeAggregationBuilder compositeBuilder = new CompositeAggregationBuilder(RollupIndexer.AGGREGATION_NAME,
                singletonList(dateHisto));

        MetricConfig metricConfig = new MetricConfig("another_field", Arrays.asList("avg", "sum"));
        List<AggregationBuilder> metricAgg = createAggregationBuilders(singletonList(metricConfig));
        metricAgg.forEach(compositeBuilder::subAggregation);

        Aggregator aggregator = createAggregator(compositeBuilder, indexSearcher, timestampFieldType, valueFieldType);
        aggregator.preCollection();
        indexSearcher.search(new MatchAllDocsQuery(), aggregator);
        aggregator.postCollection();
        CompositeAggregation composite = (CompositeAggregation) aggregator.buildTopLevel();
        indexReader.close();
        directory.close();

        final GroupConfig groupConfig = randomGroupConfig(random());
        List<IndexRequest> docs = IndexerUtils.processBuckets(composite, indexName, stats, groupConfig, "foo").collect(Collectors.toList());

        assertThat(docs.size(), equalTo(numDocs));
        for (IndexRequest doc : docs) {
            Map<String, Object> map = doc.sourceAsMap();
            assertNull(map.get("another_field." + AvgAggregationBuilder.NAME + "." + RollupField.VALUE));
            assertNotNull(map.get("another_field." + SumAggregationBuilder.NAME + "." + RollupField.VALUE));
            assertThat(map.get("the_histo." + DateHistogramAggregationBuilder.NAME + "." + RollupField.COUNT_FIELD), equalTo(1));
        }
    }

    public void testKeyOrdering() {
        CompositeAggregation composite = mock(CompositeAggregation.class);

        when(composite.getBuckets()).thenAnswer((Answer<List<CompositeAggregation.Bucket>>) invocationOnMock -> {
            List<CompositeAggregation.Bucket> foos = new ArrayList<>();

            CompositeAggregation.Bucket bucket = mock(CompositeAggregation.Bucket.class);
            LinkedHashMap<String, Object> keys = new LinkedHashMap<>(3);
            keys.put("foo.date_histogram", 123L);
            keys.put("bar.terms", "baz");
            keys.put("abc.histogram", 1.9);
            keys = shuffleMap(keys, Collections.emptySet());
            when(bucket.getKey()).thenReturn(keys);

            List<Aggregation> list = new ArrayList<>(3);
            InternalNumericMetricsAggregation.SingleValue mockAgg = mock(InternalNumericMetricsAggregation.SingleValue.class);
            when(mockAgg.getName()).thenReturn("123");
            list.add(mockAgg);

            InternalNumericMetricsAggregation.SingleValue mockAgg2 = mock(InternalNumericMetricsAggregation.SingleValue.class);
            when(mockAgg2.getName()).thenReturn("abc");
            list.add(mockAgg2);

            InternalNumericMetricsAggregation.SingleValue mockAgg3 = mock(InternalNumericMetricsAggregation.SingleValue.class);
            when(mockAgg3.getName()).thenReturn("yay");
            list.add(mockAgg3);

            Collections.shuffle(list, random());

            Aggregations aggs = new Aggregations(list);
            when(bucket.getAggregations()).thenReturn(aggs);
            when(bucket.getDocCount()).thenReturn(1L);

            foos.add(bucket);

            return foos;
        });

        GroupConfig groupConfig = new GroupConfig(randomDateHistogramGroupConfig(random()), new HistogramGroupConfig(1L, "abc"), null);
        List<IndexRequest> docs = IndexerUtils.processBuckets(composite, "foo", new RollupIndexerJobStats(), groupConfig, "foo")
            .collect(Collectors.toList());
        assertThat(docs.size(), equalTo(1));
        assertThat(docs.get(0).id(), equalTo("foo$c9LcrFqeFW92uN_Z7sv1hA"));
    }

    /*
        A test to make sure very long keys don't break the hash
     */
    public void testKeyOrderingLong() {
        CompositeAggregation composite = mock(CompositeAggregation.class);

        when(composite.getBuckets()).thenAnswer((Answer<List<CompositeAggregation.Bucket>>) invocationOnMock -> {
            List<CompositeAggregation.Bucket> foos = new ArrayList<>();

            CompositeAggregation.Bucket bucket = mock(CompositeAggregation.Bucket.class);
            LinkedHashMap<String, Object> keys = new LinkedHashMap<>(3);
            keys.put("foo.date_histogram", 123L);

            char[] charArray = new char[IndexWriter.MAX_TERM_LENGTH];
            Arrays.fill(charArray, 'a');
            keys.put("bar.terms", new String(charArray));
            keys.put("abc.histogram", 1.9);
            keys = shuffleMap(keys, Collections.emptySet());
            when(bucket.getKey()).thenReturn(keys);

            List<Aggregation> list = new ArrayList<>(3);
            InternalNumericMetricsAggregation.SingleValue mockAgg = mock(InternalNumericMetricsAggregation.SingleValue.class);
            when(mockAgg.getName()).thenReturn("123");
            list.add(mockAgg);

            InternalNumericMetricsAggregation.SingleValue mockAgg2 = mock(InternalNumericMetricsAggregation.SingleValue.class);
            when(mockAgg2.getName()).thenReturn("abc");
            list.add(mockAgg2);

            InternalNumericMetricsAggregation.SingleValue mockAgg3 = mock(InternalNumericMetricsAggregation.SingleValue.class);
            when(mockAgg3.getName()).thenReturn("yay");
            list.add(mockAgg3);

            Collections.shuffle(list, random());

            Aggregations aggs = new Aggregations(list);
            when(bucket.getAggregations()).thenReturn(aggs);
            when(bucket.getDocCount()).thenReturn(1L);

            foos.add(bucket);

            return foos;
        });

        GroupConfig groupConfig = new GroupConfig(randomDateHistogramGroupConfig(random()), new HistogramGroupConfig(1, "abc"), null);
        List<IndexRequest> docs = IndexerUtils.processBuckets(composite, "foo", new RollupIndexerJobStats(), groupConfig, "foo")
            .collect(Collectors.toList());
        assertThat(docs.size(), equalTo(1));
        assertThat(docs.get(0).id(), equalTo("foo$VAFKZpyaEqYRPLyic57_qw"));
    }

    public void testNullKeys() {
        CompositeAggregation composite = mock(CompositeAggregation.class);

        when(composite.getBuckets()).thenAnswer((Answer<List<CompositeAggregation.Bucket>>) invocationOnMock -> {
            List<CompositeAggregation.Bucket> foos = new ArrayList<>();

            CompositeAggregation.Bucket bucket = mock(CompositeAggregation.Bucket.class);
            LinkedHashMap<String, Object> keys = new LinkedHashMap<>(3);
            keys.put("bar.terms", null);
            keys.put("abc.histogram", null);
            when(bucket.getKey()).thenReturn(keys);

            Aggregations aggs = new Aggregations(Collections.emptyList());
            when(bucket.getAggregations()).thenReturn(aggs);
            when(bucket.getDocCount()).thenReturn(1L);

            foos.add(bucket);

            return foos;
        });

        GroupConfig groupConfig = new GroupConfig(randomDateHistogramGroupConfig(random()), randomHistogramGroupConfig(random()), null);
        List<IndexRequest> docs = IndexerUtils.processBuckets(composite, "foo", new RollupIndexerJobStats(), groupConfig, "foo")
            .collect(Collectors.toList());
        assertThat(docs.size(), equalTo(1));
        assertFalse(Strings.isNullOrEmpty(docs.get(0).id()));
    }

    public void testMissingBuckets() throws IOException {
        String indexName = randomAlphaOfLengthBetween(1, 10);
        RollupIndexerJobStats stats = new RollupIndexerJobStats(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);

        String metricField = "metric_field";
        String valueField = "value_field";

        Directory directory = newDirectory();
        RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory);

        int numDocs = 10;

        for (int i = 0; i < numDocs; i++) {
            Document document = new Document();

            // Every other doc omit the valueField, so that we get some null buckets
            if (i % 2 == 0) {
                document.add(new SortedNumericDocValuesField(valueField, i));
                document.add(new LongPoint(valueField, i));
            }
            document.add(new SortedNumericDocValuesField(metricField, i));
            document.add(new LongPoint(metricField, i));
            indexWriter.addDocument(document);
        }

        indexWriter.close();

        IndexReader indexReader = DirectoryReader.open(directory);
        IndexSearcher indexSearcher = newIndexSearcher(indexReader);

        MappedFieldType valueFieldType = new NumberFieldMapper.NumberFieldType(valueField, NumberFieldMapper.NumberType.LONG);
        MappedFieldType metricFieldType = new NumberFieldMapper.NumberFieldType(metricField, NumberFieldMapper.NumberType.LONG);

        // Setup the composite agg
        TermsGroupConfig termsGroupConfig = new TermsGroupConfig(valueField);
        CompositeAggregationBuilder compositeBuilder =
            new CompositeAggregationBuilder(RollupIndexer.AGGREGATION_NAME, RollupIndexer.createValueSourceBuilders(termsGroupConfig))
                .size(numDocs*2);

        MetricConfig metricConfig = new MetricConfig(metricField, singletonList("max"));
        List<AggregationBuilder> metricAgg = createAggregationBuilders(singletonList(metricConfig));
        metricAgg.forEach(compositeBuilder::subAggregation);

        Aggregator aggregator = createAggregator(compositeBuilder, indexSearcher, valueFieldType, metricFieldType);
        aggregator.preCollection();
        indexSearcher.search(new MatchAllDocsQuery(), aggregator);
        aggregator.postCollection();
        CompositeAggregation composite = (CompositeAggregation) aggregator.buildTopLevel();
        indexReader.close();
        directory.close();

        final GroupConfig groupConfig = randomGroupConfig(random());
        List<IndexRequest> docs = IndexerUtils.processBuckets(composite, indexName, stats, groupConfig, "foo").collect(Collectors.toList());

        assertThat(docs.size(), equalTo(6));
        for (IndexRequest doc : docs) {
            Map<String, Object> map = doc.sourceAsMap();
            Object value = map.get(valueField + "." + TermsAggregationBuilder.NAME + "." + RollupField.VALUE);
            if (value == null) {
                assertThat(map.get(valueField + "." + TermsAggregationBuilder.NAME + "." + RollupField.COUNT_FIELD), equalTo(5));
            } else {
                assertThat(map.get(valueField + "." + TermsAggregationBuilder.NAME + "." + RollupField.COUNT_FIELD), equalTo(1));
            }
        }
    }

    public void testTimezone() throws IOException {
        String indexName = randomAlphaOfLengthBetween(1, 10);
        RollupIndexerJobStats stats = new RollupIndexerJobStats(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);

        String timestampField = "the_histo";
        String valueField = "the_avg";

        Directory directory = newDirectory();
        RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory);

        {
            Document document = new Document();
            long timestamp = 1443659400000L; // 2015-10-01T00:30:00Z
            document.add(new SortedNumericDocValuesField(timestampField, timestamp));
            document.add(new LongPoint(timestampField, timestamp));
            document.add(new SortedNumericDocValuesField(valueField, randomIntBetween(1, 100)));
            indexWriter.addDocument(document);
        }
        {
            Document document = new Document();
            long timestamp = 1443663000000L; // 2015-10-01T01:30:00Z
            document.add(new SortedNumericDocValuesField(timestampField, timestamp));
            document.add(new LongPoint(timestampField, timestamp));
            document.add(new SortedNumericDocValuesField(valueField, randomIntBetween(1, 100)));
            indexWriter.addDocument(document);
        }
        indexWriter.close();

        IndexReader indexReader = DirectoryReader.open(directory);
        IndexSearcher indexSearcher = newIndexSearcher(indexReader);

        DateFieldMapper.DateFieldType timestampFieldType = new DateFieldMapper.DateFieldType(timestampField);
        MappedFieldType valueFieldType = new NumberFieldMapper.NumberFieldType(valueField, NumberFieldMapper.NumberType.LONG);

        // Setup the composite agg
        DateHistogramValuesSourceBuilder dateHisto
            = new DateHistogramValuesSourceBuilder("the_histo." + DateHistogramAggregationBuilder.NAME)
            .field(timestampField)
            .calendarInterval(new DateHistogramInterval("1d"))
            .timeZone(ZoneId.of("-01:00", ZoneId.SHORT_IDS));  // adds a timezone so that we aren't on default UTC

        CompositeAggregationBuilder compositeBuilder = new CompositeAggregationBuilder(RollupIndexer.AGGREGATION_NAME,
            singletonList(dateHisto));

        MetricConfig metricConfig = new MetricConfig(valueField, singletonList("max"));
        List<AggregationBuilder> metricAgg = createAggregationBuilders(singletonList(metricConfig));
        metricAgg.forEach(compositeBuilder::subAggregation);

        Aggregator aggregator = createAggregator(compositeBuilder, indexSearcher, timestampFieldType, valueFieldType);
        aggregator.preCollection();
        indexSearcher.search(new MatchAllDocsQuery(), aggregator);
        aggregator.postCollection();
        CompositeAggregation composite = (CompositeAggregation) aggregator.buildTopLevel();
        indexReader.close();
        directory.close();

        final GroupConfig groupConfig = randomGroupConfig(random());
        List<IndexRequest> docs = IndexerUtils.processBuckets(composite, indexName, stats, groupConfig, "foo").collect(Collectors.toList());

        assertThat(docs.size(), equalTo(2));

        Map<String, Object> map = docs.get(0).sourceAsMap();
        assertNotNull(map.get(valueField + "." + MaxAggregationBuilder.NAME + "." + RollupField.VALUE));
        assertThat(map.get("the_histo." + DateHistogramAggregationBuilder.NAME + "." + RollupField.COUNT_FIELD), equalTo(1));
        assertThat(map.get("the_histo." + DateHistogramAggregationBuilder.NAME + "." + RollupField.TIMESTAMP),
            equalTo(1443574800000L)); // 2015-09-30T00:00:00.000-01:00

        map = docs.get(1).sourceAsMap();
        assertNotNull(map.get(valueField + "." + MaxAggregationBuilder.NAME + "." + RollupField.VALUE));
        assertThat(map.get("the_histo." + DateHistogramAggregationBuilder.NAME + "." + RollupField.COUNT_FIELD), equalTo(1));
        assertThat(map.get("the_histo." + DateHistogramAggregationBuilder.NAME + "." + RollupField.TIMESTAMP),
            equalTo(1443661200000L)); // 2015-10-01T00:00:00.000-01:00


    }

    interface Mock {
        List<? extends CompositeAggregation.Bucket> getBuckets();
    }
}
