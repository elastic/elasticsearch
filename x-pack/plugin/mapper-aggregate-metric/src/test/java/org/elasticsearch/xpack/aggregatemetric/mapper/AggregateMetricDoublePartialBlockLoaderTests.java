/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.aggregatemetric.mapper;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.mapper.TestBlock;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matcher;

import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;

import static org.elasticsearch.xpack.aggregatemetric.mapper.AggregateMetricDoubleFieldMapper.subfieldName;
import static org.hamcrest.Matchers.hasToString;

public class AggregateMetricDoublePartialBlockLoaderTests extends ESTestCase {
    @ParametersFactory(argumentFormatting = "missingValues=%s, loadMin=%s, loadMax=%s, loadSum=%s, loadCount=%s")
    public static List<Object[]> parameters() throws IOException {
        List<Object[]> parameters = new ArrayList<>();
        for (boolean missingValues : new boolean[] { true, false }) {
            for (boolean loadMin : new boolean[] { true, false }) {
                for (boolean loadMax : new boolean[] { true, false }) {
                    for (boolean loadSum : new boolean[] { true, false }) {
                        for (boolean loadCount : new boolean[] { true, false }) {
                            parameters.add(new Object[] { missingValues, loadMin, loadMax, loadSum, loadCount });
                        }
                    }
                }
            }
        }
        return parameters;
    }

    protected final boolean missingValues;
    protected final boolean loadMin;
    protected final boolean loadMax;
    protected final boolean loadSum;
    protected final boolean loadCount;

    public AggregateMetricDoublePartialBlockLoaderTests(
        boolean missingValues,
        boolean loadMin,
        boolean loadMax,
        boolean loadSum,
        boolean loadCount
    ) {
        this.missingValues = missingValues;
        this.loadMin = loadMin;
        this.loadMax = loadMax;
        this.loadSum = loadSum;
        this.loadCount = loadCount;
    }

    public void test() throws IOException {
        try (Directory dir = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), dir)) {
            int docCount = 10_000;
            for (int i = 0; i < docCount; i++) {
                List<NumericDocValuesField> doc = new ArrayList<>(4);
                doc.add(new NumericDocValuesField("field.min", Double.doubleToLongBits(i)));
                doc.add(new NumericDocValuesField("field.max", Double.doubleToLongBits(i)));
                doc.add(new NumericDocValuesField("field.sum", Double.doubleToLongBits(i)));
                doc.add(new NumericDocValuesField("field.value_count", i));
                iw.addDocument(doc);
            }
            if (missingValues) {
                iw.addDocument(List.of());
            }
            iw.forceMerge(1);
            try (DirectoryReader dr = iw.getReader()) {
                LeafReaderContext ctx = getOnlyLeafReader(dr).getContext();
                innerTest(ctx);
            }
        }
    }

    private void innerTest(LeafReaderContext ctx) throws IOException {
        var allMetricFields = generateMetricFields();
        var metricFieldsToLoad = generateMetricsToLoad(allMetricFields);
        AggregateMetricDoubleBlockLoader aggMetricPartialLoader = new AggregateMetricDoubleBlockLoader(metricFieldsToLoad);
        AggregateMetricDoubleBlockLoader aggMetricAllLoader = new AggregateMetricDoubleBlockLoader(allMetricFields);
        var aggMetricPartialReader = aggMetricPartialLoader.reader(ctx);
        var aggMetricAllReader = aggMetricAllLoader.reader(ctx);
        assertThat(aggMetricPartialReader, readerMatcher());
        assertThat(aggMetricAllReader, readerMatcher());

        BlockLoader.Docs docs = TestBlock.docs(ctx);
        try (
            TestBlock aggMetricsPartial = read(aggMetricPartialLoader, aggMetricPartialReader, ctx, docs);
            TestBlock aggMetricsAll = read(aggMetricAllLoader, aggMetricAllReader, ctx, docs)
        ) {
            checkBlocks(aggMetricsAll, aggMetricsPartial);
        }

        aggMetricPartialReader = aggMetricPartialLoader.reader(ctx);
        aggMetricAllReader = aggMetricAllLoader.reader(ctx);
        for (int i = 0; i < ctx.reader().numDocs(); i += 10) {
            int[] docsArray = new int[Math.min(10, ctx.reader().numDocs() - i)];
            for (int d = 0; d < docsArray.length; d++) {
                docsArray[d] = i + d;
            }
            docs = TestBlock.docs(docsArray);
            try (
                TestBlock aggMetricsPartial = read(aggMetricPartialLoader, aggMetricPartialReader, ctx, docs);
                TestBlock aggMetricsAll = read(aggMetricAllLoader, aggMetricAllReader, ctx, docs)
            ) {
                checkBlocks(aggMetricsAll, aggMetricsPartial);
            }
        }
    }

    private EnumMap<AggregateMetricDoubleFieldMapper.Metric, NumberFieldMapper.NumberFieldType> generateMetricsToLoad(
        EnumMap<AggregateMetricDoubleFieldMapper.Metric, NumberFieldMapper.NumberFieldType> allMetricFields
    ) {
        EnumMap<AggregateMetricDoubleFieldMapper.Metric, NumberFieldMapper.NumberFieldType> metricFieldsToLoad = new EnumMap<>(
            AggregateMetricDoubleFieldMapper.Metric.class
        );
        if (loadMin) {
            metricFieldsToLoad.put(
                AggregateMetricDoubleFieldMapper.Metric.min,
                allMetricFields.get(AggregateMetricDoubleFieldMapper.Metric.min)
            );
        }
        if (loadMax) {
            metricFieldsToLoad.put(
                AggregateMetricDoubleFieldMapper.Metric.max,
                allMetricFields.get(AggregateMetricDoubleFieldMapper.Metric.max)
            );
        }
        if (loadSum) {
            metricFieldsToLoad.put(
                AggregateMetricDoubleFieldMapper.Metric.sum,
                allMetricFields.get(AggregateMetricDoubleFieldMapper.Metric.sum)
            );
        }
        if (loadCount) {
            metricFieldsToLoad.put(
                AggregateMetricDoubleFieldMapper.Metric.value_count,
                allMetricFields.get(AggregateMetricDoubleFieldMapper.Metric.value_count)
            );
        }
        return metricFieldsToLoad;
    }

    private EnumMap<AggregateMetricDoubleFieldMapper.Metric, NumberFieldMapper.NumberFieldType> generateMetricFields() {
        EnumMap<AggregateMetricDoubleFieldMapper.Metric, NumberFieldMapper.NumberFieldType> metricFields = new EnumMap<>(
            AggregateMetricDoubleFieldMapper.Metric.class
        );
        for (AggregateMetricDoubleFieldMapper.Metric m : List.of(AggregateMetricDoubleFieldMapper.Metric.values())) {
            String subfieldName = subfieldName("field", m);
            NumberFieldMapper.NumberFieldType subfield = new NumberFieldMapper.NumberFieldType(
                subfieldName,
                m == AggregateMetricDoubleFieldMapper.Metric.value_count
                    ? NumberFieldMapper.NumberType.INTEGER
                    : NumberFieldMapper.NumberType.DOUBLE
            );
            metricFields.put(m, subfield);
        }
        return metricFields;
    }

    private Matcher<Object> readerMatcher() {
        return hasToString("BlockDocValuesReader.AggregateMetricDouble");
    }

    @SuppressWarnings("unchecked")
    private void checkBlocks(TestBlock allMetrics, TestBlock partialMetrics) {
        for (int i = 0; i < allMetrics.size(); i++) {
            var all = (HashMap<String, Number>) (allMetrics.get(i));
            var partial = (HashMap<String, Number>) (partialMetrics.get(i));
            checkMetric(loadMin, AggregateMetricDoubleFieldMapper.Metric.min.name(), all, partial);
            checkMetric(loadMax, AggregateMetricDoubleFieldMapper.Metric.max.name(), all, partial);
            checkMetric(loadSum, AggregateMetricDoubleFieldMapper.Metric.sum.name(), all, partial);
            checkMetric(loadCount, AggregateMetricDoubleFieldMapper.Metric.value_count.name(), all, partial);
        }
    }

    private void checkMetric(boolean loadField, String field, HashMap<String, Number> allMetrics, HashMap<String, Number> partialMetrics) {
        if (loadField) {
            assertEquals(allMetrics.get(field), partialMetrics.get(field));
        } else {
            assertNull(partialMetrics.get(field));
        }
    }

    protected final TestBlock read(BlockLoader loader, BlockLoader.AllReader reader, LeafReaderContext ctx, BlockLoader.Docs docs)
        throws IOException {
        return (TestBlock) reader.read(TestBlock.factory(), docs, 0, false);
    }
}
