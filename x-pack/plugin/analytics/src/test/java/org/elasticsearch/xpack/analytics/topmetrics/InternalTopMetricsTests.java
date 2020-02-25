/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.analytics.topmetrics;

import org.elasticsearch.client.analytics.ParsedTopMetrics;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.ParsedAggregation;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.search.sort.SortValue;
import org.elasticsearch.test.InternalAggregationTestCase;

import java.io.IOException;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class InternalTopMetricsTests extends InternalAggregationTestCase<InternalTopMetrics> {
    public void testEmptyIsNotMapped() {
        InternalTopMetrics empty = InternalTopMetrics.buildEmptyAggregation(
                randomAlphaOfLength(5), randomAlphaOfLength(2), emptyList(), null);
        assertFalse(empty.isMapped());
    }

    public void testNonEmptyIsMapped() {
        InternalTopMetrics nonEmpty = randomValueOtherThanMany(tm -> tm.getSortValue() == null, this::createTestInstance);
        assertTrue(nonEmpty.isMapped());
    }

    public void testToXContentDoubleSortValue() throws IOException {
        InternalTopMetrics tm = new InternalTopMetrics("test", DocValueFormat.RAW, randomFrom(SortOrder.values()), SortValue.from(1.0),
                "test", 1.0, emptyList(), null);
        assertThat(Strings.toString(tm, true, true), equalTo(
                "{\n" +
                "  \"test\" : {\n" +
                "    \"top\" : [\n" +
                "      {\n" +
                "        \"sort\" : [\n" +
                "          1.0\n" +
                "        ],\n" +
                "        \"metrics\" : {\n" +
                "          \"test\" : 1.0\n" +
                "        }\n" +
                "      }\n" +
                "    ]\n" +
                "  }\n" +
                "}"));
    }

    public void testToXConentDateSortValue() throws IOException {
        DocValueFormat sortFormat = new DocValueFormat.DateTime(DateFormatter.forPattern("strict_date_time"), ZoneId.of("UTC"),
                DateFieldMapper.Resolution.MILLISECONDS);
        SortValue sortValue = SortValue.from(ZonedDateTime.parse("2007-12-03T10:15:30Z").toInstant().toEpochMilli());
        InternalTopMetrics tm = new InternalTopMetrics("test", sortFormat, randomFrom(SortOrder.values()), sortValue, "test", 1.0,
                emptyList(), null);
        assertThat(Strings.toString(tm, true, true), equalTo(
                "{\n" +
                "  \"test\" : {\n" +
                "    \"top\" : [\n" +
                "      {\n" +
                "        \"sort\" : [\n" +
                "          \"2007-12-03T10:15:30.000Z\"\n" +
                "        ],\n" +
                "        \"metrics\" : {\n" +
                "          \"test\" : 1.0\n" +
                "        }\n" +
                "      }\n" +
                "    ]\n" +
                "  }\n" +
                "}"));
    }

    @Override
    protected List<NamedXContentRegistry.Entry> getNamedXContents() {
        List<NamedXContentRegistry.Entry> result = new ArrayList<>(super.getNamedXContents());
        result.add(new NamedXContentRegistry.Entry(Aggregation.class, new ParseField(TopMetricsAggregationBuilder.NAME),
                (p, c) -> ParsedTopMetrics.PARSER.parse(p, (String) c)));
        return result;
    }

    @Override
    protected InternalTopMetrics createTestInstance(String name, List<PipelineAggregator> pipelineAggregators,
            Map<String, Object> metaData) {
        DocValueFormat sortFormat = randomNumericDocValueFormat();
        SortOrder sortOrder = randomFrom(SortOrder.values());
        SortValue sortValue = randomSortValue();
        String metricName = randomAlphaOfLength(5);
        double metricValue = randomDouble();
        return new InternalTopMetrics(name, sortFormat, sortOrder, sortValue, metricName, metricValue, pipelineAggregators, metaData);
    }

    @Override
    protected InternalTopMetrics mutateInstance(InternalTopMetrics instance) throws IOException {
        String name = instance.getName();
        DocValueFormat sortFormat = instance.getSortFormat();
        SortOrder sortOrder = instance.getSortOrder();
        SortValue sortValue = instance.getSortValue();
        String metricName = instance.getMetricName();
        double metricValue = instance.getMetricValue();
        switch (randomInt(5)) {
        case 0:
            name = randomAlphaOfLength(6);
            break;
        case 1:
            sortFormat = randomValueOtherThan(sortFormat, InternalAggregationTestCase::randomNumericDocValueFormat);
            break;
        case 2:
            sortOrder = sortOrder == SortOrder.ASC ? SortOrder.DESC : SortOrder.ASC;
            break;
        case 3:
            sortValue = randomValueOtherThan(sortValue, InternalTopMetricsTests::randomSortValue);
            break;
        case 4:
            metricName = randomAlphaOfLength(6);
            break;
        case 5:
            metricValue = randomValueOtherThan(metricValue, () -> randomDouble());
            break;
        default:
            throw new IllegalArgumentException("bad mutation");
        }
        return new InternalTopMetrics(name, sortFormat, sortOrder, sortValue, metricName, metricValue, emptyList(), null);
    }

    @Override
    protected Reader<InternalTopMetrics> instanceReader() {
        return InternalTopMetrics::new;
    }

    @Override
    protected void assertFromXContent(InternalTopMetrics aggregation, ParsedAggregation parsedAggregation) throws IOException {
        ParsedTopMetrics parsed = (ParsedTopMetrics) parsedAggregation;
        assertThat(parsed.getName(), equalTo(aggregation.getName()));
        if (false == aggregation.isMapped()) {
            assertThat(parsed.getTopMetrics(), hasSize(0));
            return;
        }
        assertThat(parsed.getTopMetrics(), hasSize(1));
        ParsedTopMetrics.TopMetrics parsedTop = parsed.getTopMetrics().get(0); 
        Object expectedSort = aggregation.getSortFormat() == DocValueFormat.RAW ?
                aggregation.getSortValue().getKey() : aggregation.getFormattedSortValue();
        assertThat(parsedTop.getSort(), equalTo(singletonList(expectedSort)));
        assertThat(parsedTop.getMetrics(), equalTo(singletonMap(aggregation.getMetricName(), aggregation.getMetricValue())));
    }

    @Override
    protected void assertReduced(InternalTopMetrics reduced, List<InternalTopMetrics> inputs) {
        InternalTopMetrics first = inputs.get(0);
        Optional<InternalTopMetrics> winner = inputs.stream()
                .filter(tm -> tm.isMapped())
                .min((lhs, rhs) -> first.getSortOrder().reverseMul() * lhs.getSortValue().compareTo(rhs.getSortValue()));

        assertThat(reduced.getName(), equalTo(first.getName()));
        assertThat(reduced.getSortOrder(), equalTo(first.getSortOrder()));
        assertThat(reduced.getMetricName(), equalTo(first.getMetricName()));
        if (winner.isPresent()) {
            assertThat(reduced.getSortValue(), equalTo(winner.get().getSortValue()));
            assertThat(reduced.getSortFormat(), equalTo(winner.get().getSortFormat()));
            assertThat(reduced.getMetricValue(), equalTo(winner.get().getMetricValue()));
        } else {
            // Reduced only unmapped metrics
            assertThat(reduced.getSortValue(), equalTo(first.getSortValue()));
            assertThat(reduced.getSortFormat(), equalTo(first.getSortFormat()));
            assertThat(reduced.getMetricValue(), equalTo(first.getMetricValue()));
        }
    }

    private static SortValue randomSortValue() {
        switch (between(0, 2)) {
        case 0:
            return null;
        case 1:
            return SortValue.from(randomLong());
        case 2:
            return SortValue.from(randomDouble());
        default:
            throw new IllegalArgumentException("unsupported random sort");
        }
    }

    @Override
    protected Predicate<String> excludePathsFromXContentInsertion() {
        return path -> path.endsWith(".metrics");
    }
}
