/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.aggregatemetric.mapper;

import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.mapper.TestBlock;
import org.elasticsearch.indices.CrankyCircuitBreakerService;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;

import static org.elasticsearch.xpack.aggregatemetric.mapper.AggregateMetricDoubleFieldMapper.subfieldName;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class AggregateMetricDoubleAvgBlockLoaderTests extends ESTestCase {

    private static final String FIELD = "field";

    /**
     * Drive {@code AvgBlockLoader.reader} under a cranky breaker many times. Whether or not the breaker trips on a given attempt, once the
     * returned reader (if any) is closed the breaker must always be back to zero - a non-zero reading means an acquired reservation leaked.
     */
    public void testReaderUnderCrankyBreakerDoesNotLeak() throws IOException {
        try (Directory dir = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), dir)) {
            int docCount = randomIntBetween(1, 50);
            for (int i = 0; i < docCount; i++) {
                iw.addDocument(
                    List.of(
                        new NumericDocValuesField(
                            subfieldName(FIELD, AggregateMetricDoubleFieldMapper.Metric.sum),
                            Double.doubleToLongBits(randomDouble())
                        ),
                        new NumericDocValuesField(
                            subfieldName(FIELD, AggregateMetricDoubleFieldMapper.Metric.value_count),
                            randomIntBetween(1, 1000)
                        )
                    )
                );
            }
            iw.forceMerge(1);

            try (DirectoryReader reader = iw.getReader()) {
                LeafReaderContext ctx = getOnlyLeafReader(reader).getContext();
                var loader = new AggregateMetricDoubleBlockLoader.AvgBlockLoader(metricFields(), (cls, msg) -> {});
                var cranky = new CrankyCircuitBreakerService.CrankyCircuitBreaker();
                BlockLoader.Docs docs = TestBlock.docs(ctx);

                // The breaker trips one twentieth of the time per acquisition, so iterate enough that the "trip on the second acquisition"
                // case is hit many times over.
                for (int attempt = 0; attempt < 2000; attempt++) {
                    try (BlockLoader.ColumnAtATimeReader r = loader.reader(cranky, ctx)) {
                        try (TestBlock ignored = (TestBlock) r.read(TestBlock.factory(), docs, 0, false)) {
                            // reading exercises the reader; the block is built with the test factory, not the cranky breaker
                        }
                    } catch (CircuitBreakingException e) {
                        // expected on some attempts - the reservation handling under the trip is exactly what we assert below
                    }
                    assertThat("breaker leaked on attempt " + attempt, cranky.getUsed(), equalTo(0L));
                }
            }
        }
    }

    public void testZeroCountProducesNullWithWarning() throws IOException {
        try (Directory dir = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), dir)) {
            iw.addDocument(
                List.of(
                    new NumericDocValuesField(
                        subfieldName(FIELD, AggregateMetricDoubleFieldMapper.Metric.sum),
                        Double.doubleToLongBits(randomDouble())
                    ),
                    new NumericDocValuesField(subfieldName(FIELD, AggregateMetricDoubleFieldMapper.Metric.value_count), 0)
                )
            );
            iw.forceMerge(1);

            try (DirectoryReader reader = iw.getReader()) {
                LeafReaderContext ctx = getOnlyLeafReader(reader).getContext();
                List<String> capturedMessages = new ArrayList<>();
                var loader = new AggregateMetricDoubleBlockLoader.AvgBlockLoader(metricFields(), (cls, msg) -> capturedMessages.add(msg));
                var breaker = new NoopCircuitBreaker("test");
                BlockLoader.Docs docs = TestBlock.docs(ctx);

                try (BlockLoader.ColumnAtATimeReader r = loader.reader(breaker, ctx)) {
                    try (TestBlock block = (TestBlock) r.read(TestBlock.factory(), docs, 0, false)) {
                        assertNull("expected null for zero-count doc", block.get(0));
                    }
                }
                assertThat(capturedMessages.size(), equalTo(1));
                assertThat(capturedMessages.getFirst(), containsString("fields has a non-positive count [value_count="));
            }
        }
    }

    public void testNegativeCountProducesNullWithWarning() throws IOException {
        try (Directory dir = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), dir)) {
            iw.addDocument(
                List.of(
                    new NumericDocValuesField(
                        subfieldName(FIELD, AggregateMetricDoubleFieldMapper.Metric.sum),
                        Double.doubleToLongBits(randomDouble())
                    ),
                    new NumericDocValuesField(
                        subfieldName(FIELD, AggregateMetricDoubleFieldMapper.Metric.value_count),
                        randomIntBetween(-100, -1)
                    )
                )
            );
            iw.forceMerge(1);

            try (DirectoryReader reader = iw.getReader()) {
                LeafReaderContext ctx = getOnlyLeafReader(reader).getContext();
                List<String> capturedMessages = new ArrayList<>();
                var loader = new AggregateMetricDoubleBlockLoader.AvgBlockLoader(metricFields(), (cls, msg) -> capturedMessages.add(msg));
                var breaker = new NoopCircuitBreaker("test");
                BlockLoader.Docs docs = TestBlock.docs(ctx);

                try (BlockLoader.ColumnAtATimeReader r = loader.reader(breaker, ctx)) {
                    try (TestBlock block = (TestBlock) r.read(TestBlock.factory(), docs, 0, false)) {
                        assertNull("expected null for zero-count doc", block.get(0));
                    }
                }
                assertThat(capturedMessages.size(), equalTo(1));
                assertThat(capturedMessages.getFirst(), containsString("fields has a non-positive count [value_count="));
            }
        }
    }

    private static EnumMap<AggregateMetricDoubleFieldMapper.Metric, NumberFieldMapper.NumberFieldType> metricFields() {
        var metricFields = new EnumMap<AggregateMetricDoubleFieldMapper.Metric, NumberFieldMapper.NumberFieldType>(
            AggregateMetricDoubleFieldMapper.Metric.class
        );
        metricFields.put(
            AggregateMetricDoubleFieldMapper.Metric.sum,
            new NumberFieldMapper.NumberFieldType(
                subfieldName(FIELD, AggregateMetricDoubleFieldMapper.Metric.sum),
                NumberFieldMapper.NumberType.DOUBLE
            )
        );
        metricFields.put(
            AggregateMetricDoubleFieldMapper.Metric.value_count,
            new NumberFieldMapper.NumberFieldType(
                subfieldName(FIELD, AggregateMetricDoubleFieldMapper.Metric.value_count),
                NumberFieldMapper.NumberType.INTEGER
            )
        );
        return metricFields;
    }
}
