/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.analytics.topmetrics;

import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.mapper.NumberFieldMapper.NumberType;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.support.MultiValuesSourceFieldConfig;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;
import java.util.Arrays;

import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notANumber;

public class TopMetricsAggregatorTests extends AggregatorTestCase {
    public void testNoDocs() throws IOException {
        InternalTopMetrics result = collect(simpleBuilder(), new MatchAllDocsQuery(), writer -> {},
                numberField(NumberType.DOUBLE, "s"), numberField(NumberType.DOUBLE, "m"));
        assertThat(result.getSortFormat(), equalTo(DocValueFormat.RAW));
        assertThat(result.getSortOrder(), equalTo(SortOrder.ASC));
        assertThat(result.getSortValue(), notANumber());
        assertThat(result.getMetricValue(), notANumber());
    }

    public void testUnmappedMetric() throws IOException {
        InternalTopMetrics result = collect(simpleBuilder(), new MatchAllDocsQuery(), writer -> {
                    writer.addDocument(singletonList(doubleField("s", 1.0)));
                },
                numberField(NumberType.DOUBLE, "s"));
        assertThat(result.getSortValue(), notANumber());
        assertThat(result.getMetricValue(), notANumber());
    }

    public void testMissingValueForMetric() throws IOException {
        InternalTopMetrics result = collect(simpleBuilder(), new MatchAllDocsQuery(), writer -> {
                    writer.addDocument(singletonList(doubleField("s", 1.0)));
                },
                numberField(NumberType.DOUBLE, "s"), numberField(NumberType.DOUBLE, "m"));
        assertThat(result.getSortValue(), equalTo(1.0d));
        assertThat(result.getMetricValue(), notANumber());
    }

    public void testActualValueForMetric() throws IOException {
        InternalTopMetrics result = collect(simpleBuilder(), new MatchAllDocsQuery(), writer -> {
                    writer.addDocument(Arrays.asList(doubleField("s", 1.0), doubleField("m", 2.0)));
                },
                numberField(NumberType.DOUBLE, "s"), numberField(NumberType.DOUBLE, "m"));
        assertThat(result.getSortValue(), equalTo(1.0d));
        assertThat(result.getMetricValue(), equalTo(2.0d));
    }

    public void testAscending() throws IOException {
        TopMetricsAggregationBuilder builder = simpleBuilder();
        builder.getSortBuilders().get(0).order(SortOrder.ASC);
        InternalTopMetrics empty = collect(builder, new MatchAllDocsQuery(), writer -> {
                    writer.addDocument(Arrays.asList(doubleField("s", 1.0), doubleField("m", 2.0)));
                    writer.addDocument(Arrays.asList(doubleField("s", 2.0), doubleField("m", 3.0)));
                },
                numberField(NumberType.DOUBLE, "s"), numberField(NumberType.DOUBLE, "m"));
        assertThat(empty.getSortValue(), equalTo(1.0d));
        assertThat(empty.getMetricValue(), equalTo(2.0d));
    }

    public void testDescending() throws IOException {
        TopMetricsAggregationBuilder builder = simpleBuilder();
        builder.getSortBuilders().get(0).order(SortOrder.DESC);
        InternalTopMetrics empty = collect(builder, new MatchAllDocsQuery(), writer -> {
                    writer.addDocument(Arrays.asList(doubleField("s", 1.0), doubleField("m", 2.0)));
                    writer.addDocument(Arrays.asList(doubleField("s", 2.0), doubleField("m", 3.0)));
                },
                numberField(NumberType.DOUBLE, "s"), numberField(NumberType.DOUBLE, "m"));
        assertThat(empty.getSortValue(), equalTo(2.0d));
        assertThat(empty.getMetricValue(), equalTo(3.0d));
    }

    private TopMetricsAggregationBuilder simpleBuilder() {
        return new TopMetricsAggregationBuilder("test", singletonList(new FieldSortBuilder("s")),
                new MultiValuesSourceFieldConfig.Builder().setFieldName("m").build());
    }

    private MappedFieldType numberField(NumberType numberType, String name) {
        NumberFieldMapper.NumberFieldType type = new NumberFieldMapper.NumberFieldType(numberType);
        type.setName(name);
        return type;
    }

    private IndexableField doubleField(String name, double value) {
        return new SortedNumericDocValuesField(name, NumericUtils.doubleToSortableLong(value));
    }

    private InternalTopMetrics collect(TopMetricsAggregationBuilder builder, Query query,
            CheckedConsumer<RandomIndexWriter, IOException> buildIndex, MappedFieldType... fields) throws IOException {
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {
                buildIndex.accept(indexWriter);
            }

            try (IndexReader indexReader = DirectoryReader.open(directory)) {
                IndexSearcher indexSearcher = newSearcher(indexReader, true, true);
                TopMetricsAggregator aggregator = createAggregator(builder, indexSearcher, fields);
                aggregator.preCollection();
                indexSearcher.search(query, aggregator);
                aggregator.postCollection();
                InternalTopMetrics result = (InternalTopMetrics) aggregator.buildAggregation(0L);
                assertThat(result.getSortFormat(), equalTo(DocValueFormat.RAW));
                assertThat(result.getSortOrder(), equalTo(builder.getSortBuilders().get(0).order()));
                assertThat(result.getMetricName(), equalTo(builder.getMetricField().getFieldName()));
                return result;
            }
        }
    }
}
