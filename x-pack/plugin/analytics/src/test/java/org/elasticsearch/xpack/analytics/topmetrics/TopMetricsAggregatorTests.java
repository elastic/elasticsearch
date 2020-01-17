/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.analytics.topmetrics;

import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
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

import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notANumber;

public class TopMetricsAggregatorTests extends AggregatorTestCase {
    public void testNoDocs() throws IOException {
        InternalTopMetrics empty = collect(simpleBuilder(), new MatchAllDocsQuery(), writer -> {},
                numberField(NumberType.DOUBLE, "s"), numberField(NumberType.DOUBLE, "m"));
        assertThat(empty.getSortFormat(), equalTo(DocValueFormat.RAW));
        assertThat(empty.getSortOrder(), equalTo(SortOrder.ASC));
        assertThat(empty.getSortValue(), notANumber());
        assertThat(empty.getMetricName(), equalTo("m"));
        assertThat(empty.getMetricValue(), notANumber());
    }

    public void testUnmappedMetric() throws IOException {
        InternalTopMetrics empty = collect(simpleBuilder(), new MatchAllDocsQuery(), writer -> {
                    writer.addDocument(singletonList(
                            new SortedNumericDocValuesField("s", NumericUtils.doubleToSortableLong(1.0))
                    ));
                },
                numberField(NumberType.DOUBLE, "s"));
        assertThat(empty.getSortFormat(), equalTo(DocValueFormat.RAW));
        assertThat(empty.getSortOrder(), equalTo(SortOrder.ASC));
        assertThat(empty.getSortValue(), notANumber());
        assertThat(empty.getMetricName(), equalTo("m"));
        assertThat(empty.getMetricValue(), notANumber());
    }

    public void testMissingValueForMetric() throws IOException {
        InternalTopMetrics empty = collect(simpleBuilder(), new MatchAllDocsQuery(), writer -> {
                    writer.addDocument(singletonList(
                            new SortedNumericDocValuesField("s", NumericUtils.doubleToSortableLong(1.0))
                    ));
                },
                numberField(NumberType.DOUBLE, "s"), numberField(NumberType.DOUBLE, "m"));
        assertThat(empty.getSortFormat(), equalTo(DocValueFormat.RAW));
        assertThat(empty.getSortOrder(), equalTo(SortOrder.ASC));
        assertThat(empty.getSortValue(), equalTo(1.0d));
        assertThat(empty.getMetricName(), equalTo("m"));
        assertThat(empty.getMetricValue(), notANumber());
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
                return (InternalTopMetrics) aggregator.buildAggregation(0L);
            }
        }
    }
}
