/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.pipeline;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateFormatters;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.script.MockScriptEngine;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptEngine;
import org.elasticsearch.script.ScriptModule;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.histogram.InternalDateHistogram;
import org.elasticsearch.search.aggregations.metrics.AvgAggregationBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.equalTo;

public class MovFnAggrgatorTests extends AggregatorTestCase {

    private static final String DATE_FIELD = "date";
    private static final String INSTANT_FIELD = "instant";
    private static final String VALUE_FIELD = "value_field";

    private static final List<String> datasetTimes = Arrays.asList(
        "2017-01-01T01:07:45",
        "2017-01-02T03:43:34",
        "2017-01-03T04:11:00",
        "2017-01-04T05:11:31",
        "2017-01-05T08:24:05",
        "2017-01-06T13:09:32",
        "2017-01-07T13:47:43",
        "2017-01-08T16:14:34",
        "2017-01-09T17:09:50",
        "2017-01-10T22:55:46"
    );

    private static final List<Integer> datasetValues = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

    @Override
    protected ScriptService getMockScriptService() {
        MockScriptEngine scriptEngine = new MockScriptEngine(
            MockScriptEngine.NAME,
            Collections.singletonMap("test", script -> MovingFunctions.max((double[]) script.get("_values"))),
            Collections.emptyMap()
        );
        Map<String, ScriptEngine> engines = Collections.singletonMap(scriptEngine.getType(), scriptEngine);

        return new ScriptService(Settings.EMPTY, engines, ScriptModule.CORE_CONTEXTS, () -> 1L);
    }

    public void testMatchAllDocs() throws IOException {
        check(0, 3, List.of(Double.NaN, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0));
    }

    public void testShift() throws IOException {
        check(1, 3, List.of(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0));
        check(5, 3, List.of(5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 10.0, 10.0, Double.NaN, Double.NaN));
        check(-5, 3, List.of(Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN, 1.0, 2.0, 3.0, 4.0));
    }

    public void testWideWindow() throws IOException {
        check(50, 100, List.of(10.0, 10.0, 10.0, 10.0, 10.0, 10.0, 10.0, 10.0, 10.0, 10.0));
    }

    private void check(int shift, int window, List<Double> expected) throws IOException {
        Script script = new Script(ScriptType.INLINE, MockScriptEngine.NAME, "test", Collections.emptyMap());
        MovFnPipelineAggregationBuilder builder = new MovFnPipelineAggregationBuilder("mov_fn", "avg", script, window);
        builder.setShift(shift);

        Query query = new MatchAllDocsQuery();
        DateHistogramAggregationBuilder aggBuilder = new DateHistogramAggregationBuilder("histo");
        aggBuilder.calendarInterval(DateHistogramInterval.DAY).field(DATE_FIELD);
        aggBuilder.subAggregation(new AvgAggregationBuilder("avg").field(VALUE_FIELD));
        aggBuilder.subAggregation(builder);

        executeTestCase(query, aggBuilder, histogram -> {
            List<? extends Histogram.Bucket> buckets = histogram.getBuckets();
            List<Double> actual = buckets.stream()
                .map(bucket -> ((InternalSimpleValue) (bucket.getAggregations().get("mov_fn"))).value())
                .toList();
            assertThat(actual, equalTo(expected));
        });
    }

    private void executeTestCase(Query query, DateHistogramAggregationBuilder aggBuilder, Consumer<Histogram> verify) throws IOException {

        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {
                Document document = new Document();
                int counter = 0;
                for (String date : datasetTimes) {
                    long instant = asLong(date);
                    document.add(new SortedNumericDocValuesField(DATE_FIELD, instant));
                    document.add(new LongPoint(INSTANT_FIELD, instant));
                    document.add(new NumericDocValuesField(VALUE_FIELD, datasetValues.get(counter)));
                    indexWriter.addDocument(document);
                    document.clear();
                    counter += 1;
                }
            }

            try (IndexReader indexReader = DirectoryReader.open(directory)) {
                IndexSearcher indexSearcher = newSearcher(indexReader, true, true);

                DateFieldMapper.DateFieldType fieldType = new DateFieldMapper.DateFieldType(aggBuilder.field());
                MappedFieldType valueFieldType = new NumberFieldMapper.NumberFieldType("value_field", NumberFieldMapper.NumberType.LONG);

                InternalDateHistogram histogram;
                histogram = searchAndReduce(indexSearcher, query, aggBuilder, 1000, new MappedFieldType[] { fieldType, valueFieldType });
                verify.accept(histogram);
            }
        }
    }

    private static long asLong(String dateTime) {
        return DateFormatters.from(DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parse(dateTime)).toInstant().toEpochMilli();
    }
}
