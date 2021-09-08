/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.pipeline;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.Aggregation.CommonFields;
import org.elasticsearch.search.aggregations.ParsedAggregation;
import org.elasticsearch.search.aggregations.metrics.Percentile;
import org.elasticsearch.test.InternalAggregationTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import static org.elasticsearch.search.aggregations.metrics.InternalPercentilesTestCase.randomPercents;
import static org.hamcrest.Matchers.equalTo;

public class InternalPercentilesBucketTests extends InternalAggregationTestCase<InternalPercentilesBucket> {

    @Override
    protected InternalPercentilesBucket createTestInstance(String name, Map<String, Object> metadata) {
        return createTestInstance(name, metadata, randomPercents(), true);
    }

    private static InternalPercentilesBucket createTestInstance(
        String name,
        Map<String, Object> metadata,
        double[] percents,
        boolean keyed
    ) {
        final double[] percentiles = new double[percents.length];
        for (int i = 0; i < percents.length; ++i) {
            percentiles[i] = frequently() ? randomDouble() : Double.NaN;
        }
        return createTestInstance(name, metadata, percents, percentiles, keyed);
    }

    private static InternalPercentilesBucket createTestInstance(
        String name,
        Map<String, Object> metadata,
        double[] percents,
        double[] percentiles,
        boolean keyed
    ) {
        DocValueFormat format = randomNumericDocValueFormat();
        return new InternalPercentilesBucket(name, percents, percentiles, keyed, format, metadata);
    }

    @Override
    public void testReduceRandom() {
        expectThrows(UnsupportedOperationException.class, () -> createTestInstance("name", null).reduce(null, null));
    }

    @Override
    protected void assertReduced(InternalPercentilesBucket reduced, List<InternalPercentilesBucket> inputs) {
        // no test since reduce operation is unsupported
    }

    @Override
    protected final void assertFromXContent(InternalPercentilesBucket aggregation, ParsedAggregation parsedAggregation) {
        assertTrue(parsedAggregation instanceof ParsedPercentilesBucket);
        ParsedPercentilesBucket parsedPercentiles = (ParsedPercentilesBucket) parsedAggregation;

        for (Percentile percentile : aggregation) {
            Double percent = percentile.getPercent();
            assertEquals(aggregation.percentile(percent), parsedPercentiles.percentile(percent), 0);
            // we cannot ensure we get the same as_string output for Double.NaN values since they are rendered as
            // null and we don't have a formatted string representation in the rest output
            if (Double.isNaN(aggregation.percentile(percent)) == false) {
                assertEquals(aggregation.percentileAsString(percent), parsedPercentiles.percentileAsString(percent));
            }
        }
    }

    /**
     * check that we don't rely on the percent array order and that the iterator returns the values in the original order
     */
    public void testPercentOrder() {
        final double[] percents = new double[] { 0.50, 0.25, 0.01, 0.99, 0.60 };
        InternalPercentilesBucket aggregation = createTestInstance("test", Collections.emptyMap(), percents, randomBoolean());
        Iterator<Percentile> iterator = aggregation.iterator();
        Iterator<String> nameIterator = aggregation.valueNames().iterator();
        for (double percent : percents) {
            assertTrue(iterator.hasNext());
            assertTrue(nameIterator.hasNext());

            Percentile percentile = iterator.next();
            String percentileName = nameIterator.next();

            assertEquals(percent, percentile.getPercent(), 0.0d);
            assertEquals(percent, Double.valueOf(percentileName), 0.0d);

            assertEquals(aggregation.percentile(percent), percentile.getValue(), 0.0d);
        }
        assertFalse(iterator.hasNext());
        assertFalse(nameIterator.hasNext());
    }

    public void testErrorOnDifferentArgumentSize() {
        final double[] percents = new double[] { 0.1, 0.2, 0.3 };
        final double[] percentiles = new double[] { 0.10, 0.2 };
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new InternalPercentilesBucket("test", percents, percentiles, randomBoolean(), DocValueFormat.RAW, Collections.emptyMap())
        );
        assertEquals(
            "The number of provided percents and percentiles didn't match. percents: [0.1, 0.2, 0.3], percentiles: [0.1, 0.2]",
            e.getMessage()
        );
    }

    public void testParsedAggregationIteratorOrder() throws IOException {
        final InternalPercentilesBucket aggregation = createTestInstance();
        final Iterable<Percentile> parsedAggregation = parseAndAssert(aggregation, false, false);
        Iterator<Percentile> it = aggregation.iterator();
        Iterator<Percentile> parsedIt = parsedAggregation.iterator();
        while (it.hasNext()) {
            assertEquals(it.next(), parsedIt.next());
        }
    }

    public void testEmptyRanksXContent() throws IOException {
        double[] percents = new double[] { 1, 2, 3 };
        double[] percentiles = new double[3];
        for (int i = 0; i < 3; ++i) {
            percentiles[i] = randomBoolean() ? Double.NaN : Double.POSITIVE_INFINITY;
        }
        boolean keyed = randomBoolean();

        InternalPercentilesBucket agg = createTestInstance("test", Collections.emptyMap(), percents, percentiles, keyed);

        XContentBuilder builder = JsonXContent.contentBuilder().prettyPrint();
        builder.startObject();
        agg.doXContentBody(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        String expected;
        if (keyed) {
            expected = "{\n"
                + "  \"values\" : {\n"
                + "    \"1.0\" : null,\n"
                + "    \"2.0\" : null,\n"
                + "    \"3.0\" : null\n"
                + "  }\n"
                + "}";
        } else {
            expected = "{\n"
                + "  \"values\" : [\n"
                + "    {\n"
                + "      \"key\" : 1.0,\n"
                + "      \"value\" : null\n"
                + "    },\n"
                + "    {\n"
                + "      \"key\" : 2.0,\n"
                + "      \"value\" : null\n"
                + "    },\n"
                + "    {\n"
                + "      \"key\" : 3.0,\n"
                + "      \"value\" : null\n"
                + "    }\n"
                + "  ]\n"
                + "}";
        }

        assertThat(Strings.toString(builder), equalTo(expected));
    }

    @Override
    protected Predicate<String> excludePathsFromXContentInsertion() {
        return path -> path.endsWith(CommonFields.VALUES.getPreferredName());
    }

    @Override
    protected InternalPercentilesBucket mutateInstance(InternalPercentilesBucket instance) {
        String name = instance.getName();
        double[] percents = extractPercents(instance);
        double[] percentiles = extractPercentiles(instance);
        DocValueFormat formatter = instance.formatter();
        Map<String, Object> metadata = instance.getMetadata();
        switch (between(0, 3)) {
            case 0:
                name += randomAlphaOfLength(5);
                break;
            case 1:
                percents = Arrays.copyOf(percents, percents.length);
                percents[percents.length - 1] = randomDouble();
                break;
            case 2:
                percentiles = Arrays.copyOf(percentiles, percentiles.length);
                percentiles[percentiles.length - 1] = randomDouble();
                break;
            case 3:
                if (metadata == null) {
                    metadata = new HashMap<>(1);
                } else {
                    metadata = new HashMap<>(instance.getMetadata());
                }
                metadata.put(randomAlphaOfLength(15), randomInt());
                break;
            default:
                throw new AssertionError("Illegal randomisation branch");
        }
        return new InternalPercentilesBucket(name, percents, percentiles, randomBoolean(), formatter, metadata);
    }

    private double[] extractPercentiles(InternalPercentilesBucket instance) {
        List<Double> values = new ArrayList<>();
        instance.iterator().forEachRemaining(percentile -> values.add(percentile.getValue()));
        double[] valuesArray = new double[values.size()];
        for (int i = 0; i < values.size(); i++) {
            valuesArray[i] = values.get(i);
        }
        return valuesArray;
    }

    private double[] extractPercents(InternalPercentilesBucket instance) {
        List<Double> percents = new ArrayList<>();
        instance.iterator().forEachRemaining(percentile -> percents.add(percentile.getPercent()));
        double[] percentArray = new double[percents.size()];
        for (int i = 0; i < percents.size(); i++) {
            percentArray[i] = percents.get(i);
        }
        return percentArray;
    }
}
