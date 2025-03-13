/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.analytics.topmetrics;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.time.DateUtils;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.support.SamplingContext;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.search.sort.SortValue;
import org.elasticsearch.test.InternalAggregationTestCase;
import org.elasticsearch.xpack.analytics.AnalyticsPlugin;
import org.elasticsearch.xpack.analytics.topmetrics.InternalTopMetrics.MetricValue;

import java.io.IOException;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notANumber;
import static org.mockito.Mockito.mock;

public class InternalTopMetricsTests extends InternalAggregationTestCase<InternalTopMetrics> {
    /**
     * Sort order to use for randomly generated instances. This is fixed
     * for each test method so that randomly generated instances can be
     * merged. If it weren't fixed {@link InternalAggregationTestCase#testReduceRandom()}
     * would fail because the instances that it attempts to reduce don't
     * have their results in the same order.
     */
    private final SortOrder sortOrder = randomFrom(SortOrder.values());
    private final InternalTopMetrics.MetricValue metricOneDouble = new InternalTopMetrics.MetricValue(
        DocValueFormat.RAW,
        SortValue.from(1.0)
    );
    private final InternalTopMetrics.MetricValue metricOneLong = new InternalTopMetrics.MetricValue(DocValueFormat.RAW, SortValue.from(1));

    @Override
    protected SearchPlugin registerPlugin() {
        return new AnalyticsPlugin();
    }

    @Override
    protected boolean supportsSampling() {
        return true;
    }

    @Override
    protected void assertSampled(InternalTopMetrics sampled, InternalTopMetrics reduced, SamplingContext samplingContext) {
        assertThat(sampled.getTopMetrics(), equalTo(reduced.getTopMetrics()));
    }

    public void testEmptyIsNotMapped() {
        InternalTopMetrics empty = InternalTopMetrics.buildEmptyAggregation(randomAlphaOfLength(5), randomMetricNames(between(1, 5)), null);
        assertFalse(empty.canLeadReduction());
    }

    public void testNonEmptyIsMapped() {
        InternalTopMetrics nonEmpty = randomValueOtherThanMany(i -> i.getTopMetrics().isEmpty(), this::createTestInstance);
        assertTrue(nonEmpty.canLeadReduction());
    }

    public void testToXContentDoubleSortValue() throws IOException {
        List<InternalTopMetrics.TopMetric> top = singletonList(
            new InternalTopMetrics.TopMetric(DocValueFormat.RAW, SortValue.from(1.0), singletonList(metricOneDouble))
        );
        InternalTopMetrics tm = new InternalTopMetrics("test", sortOrder, singletonList("test"), 1, top, null);
        assertThat(Strings.toString(tm, true, true), equalTo("""
            {
              "test" : {
                "top" : [
                  {
                    "sort" : [
                      1.0
                    ],
                    "metrics" : {
                      "test" : 1.0
                    }
                  }
                ]
              }
            }"""));
    }

    public void testToXContentDateSortValue() throws IOException {
        SortValue sortValue = SortValue.from(ZonedDateTime.parse("2007-12-03T10:15:30Z").toInstant().toEpochMilli());
        List<InternalTopMetrics.TopMetric> top = singletonList(
            new InternalTopMetrics.TopMetric(strictDateTime(), sortValue, singletonList(metricOneDouble))
        );
        InternalTopMetrics tm = new InternalTopMetrics("test", sortOrder, singletonList("test"), 1, top, null);
        assertThat(Strings.toString(tm, true, true), equalTo("""
            {
              "test" : {
                "top" : [
                  {
                    "sort" : [
                      "2007-12-03T10:15:30.000Z"
                    ],
                    "metrics" : {
                      "test" : 1.0
                    }
                  }
                ]
              }
            }"""));
    }

    public void testToXContentLongMetricValue() throws IOException {
        List<InternalTopMetrics.TopMetric> top = singletonList(
            new InternalTopMetrics.TopMetric(DocValueFormat.RAW, SortValue.from(1.0), singletonList(metricOneLong))
        );
        InternalTopMetrics tm = new InternalTopMetrics("test", sortOrder, singletonList("test"), 1, top, null);
        assertThat(Strings.toString(tm, true, true), equalTo("""
            {
              "test" : {
                "top" : [
                  {
                    "sort" : [
                      1.0
                    ],
                    "metrics" : {
                      "test" : 1
                    }
                  }
                ]
              }
            }"""));
    }

    public void testToXContentDateMetricValue() throws IOException {
        InternalTopMetrics.MetricValue metricValue = new InternalTopMetrics.MetricValue(
            strictDateTime(),
            SortValue.from(ZonedDateTime.parse("2007-12-03T10:15:30Z").toInstant().toEpochMilli())
        );
        List<InternalTopMetrics.TopMetric> top = singletonList(
            new InternalTopMetrics.TopMetric(DocValueFormat.RAW, SortValue.from(1.0), singletonList(metricValue))
        );
        InternalTopMetrics tm = new InternalTopMetrics("test", sortOrder, singletonList("test"), 1, top, null);
        assertThat(Strings.toString(tm, true, true), equalTo("""
            {
              "test" : {
                "top" : [
                  {
                    "sort" : [
                      1.0
                    ],
                    "metrics" : {
                      "test" : "2007-12-03T10:15:30.000Z"
                    }
                  }
                ]
              }
            }"""));
    }

    public void testToXContentManyMetrics() throws IOException {
        List<InternalTopMetrics.TopMetric> top = singletonList(
            new InternalTopMetrics.TopMetric(
                DocValueFormat.RAW,
                SortValue.from(1.0),
                List.of(metricOneDouble, metricOneLong, metricOneDouble)
            )
        );
        InternalTopMetrics tm = new InternalTopMetrics("test", sortOrder, List.of("foo", "bar", "baz"), 1, top, null);
        assertThat(Strings.toString(tm, true, true), equalTo("""
            {
              "test" : {
                "top" : [
                  {
                    "sort" : [
                      1.0
                    ],
                    "metrics" : {
                      "foo" : 1.0,
                      "bar" : 1,
                      "baz" : 1.0
                    }
                  }
                ]
              }
            }"""));
    }

    public void testToXContentManyTopMetrics() throws IOException {
        List<InternalTopMetrics.TopMetric> top = List.of(
            new InternalTopMetrics.TopMetric(DocValueFormat.RAW, SortValue.from(1.0), singletonList(metricOneDouble)),
            new InternalTopMetrics.TopMetric(DocValueFormat.RAW, SortValue.from(2.0), singletonList(metricOneLong))
        );
        InternalTopMetrics tm = new InternalTopMetrics("test", sortOrder, singletonList("test"), 2, top, null);
        assertThat(Strings.toString(tm, true, true), equalTo("""
            {
              "test" : {
                "top" : [
                  {
                    "sort" : [
                      1.0
                    ],
                    "metrics" : {
                      "test" : 1.0
                    }
                  },
                  {
                    "sort" : [
                      2.0
                    ],
                    "metrics" : {
                      "test" : 1
                    }
                  }
                ]
              }
            }"""));
    }

    public void testGetProperty() {
        InternalTopMetrics metrics = resultWithAllTypes();
        assertThat(metrics.getProperty("int"), equalTo(1L));
        assertThat(metrics.getProperty("double"), equalTo(5.0));
        assertThat(metrics.getProperty("bytes"), equalTo(new BytesRef("cat")));
        assertThat((Double) metrics.getProperty("null"), notANumber());
    }

    public void testGetValuesAsStrings() {
        InternalTopMetrics metrics = resultWithAllTypes();
        assertThat(metrics.getValuesAsStrings("int"), equalTo(Collections.singletonList("1")));
        assertThat(metrics.getValuesAsStrings("double"), equalTo(Collections.singletonList("5.0")));
        assertThat(metrics.getValuesAsStrings("bytes"), equalTo(Collections.singletonList("cat")));
        assertThat(metrics.getValuesAsStrings("null"), equalTo(Collections.singletonList("null")));
    }

    private InternalTopMetrics resultWithAllTypes() {
        return new InternalTopMetrics(
            "test",
            SortOrder.ASC,
            List.of("int", "double", "bytes", "null"),
            1,
            List.of(
                new InternalTopMetrics.TopMetric(
                    DocValueFormat.RAW,
                    SortValue.from(1),
                    Arrays.asList(
                        new MetricValue(DocValueFormat.RAW, SortValue.from(1)),   // int
                        new MetricValue(DocValueFormat.RAW, SortValue.from(5.0)), // double
                        new MetricValue(DocValueFormat.RAW, SortValue.from(new BytesRef("cat"))), // str
                        null                                                      // null
                    )
                )
            ),
            null
        );
    }

    @Override
    protected InternalTopMetrics createTestInstance(String name, Map<String, Object> metadata) {
        return createTestInstance(
            name,
            metadata,
            InternalAggregationTestCase::randomNumericDocValueFormat,
            InternalTopMetricsTests::randomSortValue
        );
    }

    private InternalTopMetrics createTestInstance(
        String name,
        Map<String, Object> metadata,
        Supplier<DocValueFormat> randomDocValueFormat,
        Function<DocValueFormat, SortValue> sortValueSupplier
    ) {
        int metricCount = between(1, 5);
        List<String> metricNames = randomMetricNames(metricCount);
        int size = between(1, 100);
        List<InternalTopMetrics.TopMetric> topMetrics = randomTopMetrics(
            randomDocValueFormat,
            between(0, size),
            metricCount,
            sortValueSupplier
        );
        return new InternalTopMetrics(name, sortOrder, metricNames, size, topMetrics, metadata);
    }

    @Override
    protected InternalTopMetrics mutateInstance(InternalTopMetrics instance) {
        String name = instance.getName();
        SortOrder instanceSortOrder = instance.getSortOrder();
        List<String> metricNames = instance.getMetricNames();
        int size = instance.getSize();
        List<InternalTopMetrics.TopMetric> topMetrics = instance.getTopMetrics();
        switch (randomInt(4)) {
            case 0 -> name = randomAlphaOfLength(6);
            case 1 -> {
                instanceSortOrder = instanceSortOrder == SortOrder.ASC ? SortOrder.DESC : SortOrder.ASC;
                Collections.reverse(topMetrics);
            }
            case 2 -> {
                metricNames = new ArrayList<>(metricNames);
                metricNames.set(randomInt(metricNames.size() - 1), randomAlphaOfLength(6));
            }
            case 3 -> size = randomValueOtherThan(size, () -> between(1, 100));
            case 4 -> {
                int fixedSize = size;
                int fixedMetricsSize = metricNames.size();
                topMetrics = randomValueOtherThan(
                    topMetrics,
                    () -> randomTopMetrics(
                        InternalAggregationTestCase::randomNumericDocValueFormat,
                        between(1, fixedSize),
                        fixedMetricsSize,
                        InternalTopMetricsTests::randomSortValue
                    )
                );
            }
            default -> throw new IllegalArgumentException("bad mutation");
        }
        return new InternalTopMetrics(name, instanceSortOrder, metricNames, size, topMetrics, instance.getMetadata());
    }

    @Override
    protected BuilderAndToReduce<InternalTopMetrics> randomResultsToReduce(String name, int size) {
        InternalTopMetrics prototype = createTestInstance();
        return new BuilderAndToReduce<>(
            mock(AggregationBuilder.class),
            randomList(
                size,
                size,
                () -> new InternalTopMetrics(
                    prototype.getName(),
                    prototype.getSortOrder(),
                    prototype.getMetricNames(),
                    prototype.getSize(),
                    randomTopMetrics(
                        InternalAggregationTestCase::randomNumericDocValueFormat,
                        between(0, prototype.getSize()),
                        prototype.getMetricNames().size(),
                        InternalTopMetricsTests::randomSortValue
                    ),
                    prototype.getMetadata()
                )
            )
        );
    }

    @Override
    protected void assertReduced(InternalTopMetrics reduced, List<InternalTopMetrics> inputs) {
        InternalTopMetrics first = inputs.get(0);
        List<InternalTopMetrics.TopMetric> metrics = new ArrayList<>();
        for (InternalTopMetrics input : inputs) {
            metrics.addAll(input.getTopMetrics());
        }
        Collections.sort(metrics, (lhs, rhs) -> first.getSortOrder().reverseMul() * lhs.getSortValue().compareTo(rhs.getSortValue()));
        List<InternalTopMetrics.TopMetric> winners = metrics.size() > first.getSize() ? metrics.subList(0, first.getSize()) : metrics;
        assertThat(reduced.getName(), equalTo(first.getName()));
        assertThat(reduced.getSortOrder(), equalTo(first.getSortOrder()));
        assertThat(reduced.getMetricNames(), equalTo(first.getMetricNames()));
        assertThat(reduced.getTopMetrics(), equalTo(winners));
    }

    private List<InternalTopMetrics.TopMetric> randomTopMetrics(
        Supplier<DocValueFormat> randomDocValueFormat,
        int length,
        int metricCount,
        Function<DocValueFormat, SortValue> sortValueSupplier
    ) {
        return IntStream.range(0, length).mapToObj(i -> {
            DocValueFormat docValueFormat = randomDocValueFormat.get();
            return new InternalTopMetrics.TopMetric(
                docValueFormat,
                sortValueSupplier.apply(docValueFormat),
                randomMetricValues(randomDocValueFormat, metricCount, sortValueSupplier)
            );
        }).sorted((lhs, rhs) -> sortOrder.reverseMul() * lhs.getSortValue().compareTo(rhs.getSortValue())).collect(toList());
    }

    static List<String> randomMetricNames(int metricCount) {
        Set<String> names = Sets.newHashSetWithExpectedSize(metricCount);
        while (names.size() < metricCount) {
            names.add(randomAlphaOfLength(5));
        }
        return new ArrayList<>(names);
    }

    private List<InternalTopMetrics.MetricValue> randomMetricValues(
        Supplier<DocValueFormat> randomDocValueFormat,
        int metricCount,
        Function<DocValueFormat, SortValue> sortValueSupplier
    ) {
        return IntStream.range(0, metricCount).mapToObj(i -> {
            DocValueFormat format = randomDocValueFormat.get();
            return new InternalTopMetrics.MetricValue(format, sortValueSupplier.apply(format));
        }).collect(toList());
    }

    private static DocValueFormat strictDateTime() {
        return new DocValueFormat.DateTime(
            DateFormatter.forPattern("strict_date_time"),
            ZoneId.of("UTC"),
            DateFieldMapper.Resolution.MILLISECONDS
        );
    }

    private static SortValue randomSortValue() {
        if (randomBoolean()) {
            return SortValue.from(randomLong());
        }
        return SortValue.from(randomDouble());
    }

    private static SortValue randomSortValue(DocValueFormat docValueFormat) {
        if (docValueFormat instanceof DocValueFormat.DateTime) {
            if (randomBoolean()) {
                return SortValue.from(randomLongBetween(DateUtils.MAX_MILLIS_BEFORE_MINUS_9999, DateUtils.MAX_MILLIS_BEFORE_9999));
            }
            return SortValue.from(randomDoubleBetween(DateUtils.MAX_MILLIS_BEFORE_MINUS_9999, DateUtils.MAX_MILLIS_BEFORE_9999, true));
        }
        return randomSortValue();
    }
}
