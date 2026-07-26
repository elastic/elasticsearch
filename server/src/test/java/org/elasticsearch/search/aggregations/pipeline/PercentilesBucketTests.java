/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.aggregations.pipeline;

import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.global.GlobalAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.search.aggregations.pipeline.BucketHelpers.GapPolicy.SKIP;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class PercentilesBucketTests extends AbstractBucketMetricsTestCase<PercentilesBucketPipelineAggregationBuilder> {

    @Override
    protected PercentilesBucketPipelineAggregationBuilder doCreateTestAggregatorFactory(String name, String bucketsPath) {
        PercentilesBucketPipelineAggregationBuilder factory = new PercentilesBucketPipelineAggregationBuilder(name, bucketsPath);
        if (randomBoolean()) {
            int numPercents = randomIntBetween(1, 20);
            double[] percents = new double[numPercents];
            for (int i = 0; i < numPercents; i++) {
                percents[i] = randomDoubleBetween(0.0, 100.0, false);
            }
            factory.setPercents(percents);
        }
        if (randomBoolean()) {
            factory.setInterpolation(randomFrom(PercentilesBucketPipelineAggregationBuilder.Interpolation.values()));
        }
        return factory;
    }

    public void testPercentsFromMixedArray() throws Exception {
        XContentBuilder content = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("name")
            .startObject("percentiles_bucket")
            .field("buckets_path", "test")
            .array("percents", 0, 20.0, 50, 75.99)
            .endObject()
            .endObject()
            .endObject();

        PercentilesBucketPipelineAggregationBuilder builder = (PercentilesBucketPipelineAggregationBuilder) parse(createParser(content));

        assertThat(builder.getPercents(), equalTo(new double[] { 0.0, 20.0, 50.0, 75.99 }));
    }

    public void testInterpolationFromXContent() throws Exception {
        XContentBuilder content = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("name")
            .startObject("percentiles_bucket")
            .field("buckets_path", "test")
            .field("interpolation", "linear")
            .endObject()
            .endObject()
            .endObject();

        PercentilesBucketPipelineAggregationBuilder builder = (PercentilesBucketPipelineAggregationBuilder) parse(createParser(content));

        assertThat(builder.getInterpolation(), equalTo(PercentilesBucketPipelineAggregationBuilder.Interpolation.LINEAR));
    }

    public void testUnknownInterpolationIsRejected() {
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> PercentilesBucketPipelineAggregationBuilder.Interpolation.fromString("cubic")
        );

        assertThat(exception.getMessage(), equalTo("Unknown interpolation [cubic]; expected [none] or [linear]"));
    }

    public void testDefaultInterpolationPreservesNearestValueBehavior() {
        InternalPercentilesBucket aggregation = aggregate(
            new double[] { 40.0, 10.0, 30.0, 20.0 },
            new double[] { 25.0, 50.0, 75.0 },
            PercentilesBucketPipelineAggregationBuilder.Interpolation.NONE
        );

        assertEquals(20.0, aggregation.percentile(25.0), 0.0);
        assertEquals(30.0, aggregation.percentile(50.0), 0.0);
        assertEquals(30.0, aggregation.percentile(75.0), 0.0);
    }

    public void testLinearInterpolationUsesBoundariesAndExactRanks() {
        InternalPercentilesBucket aggregation = aggregate(
            new double[] { 40.0, 10.0, 30.0, 20.0 },
            new double[] { 0.0, 25.0, 50.0, 100.0 },
            PercentilesBucketPipelineAggregationBuilder.Interpolation.LINEAR
        );

        assertEquals(10.0, aggregation.percentile(0.0), 0.0);
        assertEquals(17.5, aggregation.percentile(25.0), 0.0);
        assertEquals(25.0, aggregation.percentile(50.0), 0.0);
        assertEquals(40.0, aggregation.percentile(100.0), 0.0);
    }

    public void testLinearInterpolationReturnsNaNForEmptyData() {
        InternalPercentilesBucket aggregation = aggregate(
            new double[0],
            new double[] { 0.0, 50.0, 100.0 },
            PercentilesBucketPipelineAggregationBuilder.Interpolation.LINEAR
        );

        assertTrue(Double.isNaN(aggregation.percentile(0.0)));
        assertTrue(Double.isNaN(aggregation.percentile(50.0)));
        assertTrue(Double.isNaN(aggregation.percentile(100.0)));
    }

    private static InternalPercentilesBucket aggregate(
        double[] values,
        double[] percents,
        PercentilesBucketPipelineAggregationBuilder.Interpolation interpolation
    ) {
        PercentilesBucketPipelineAggregator aggregator = new PercentilesBucketPipelineAggregator(
            "percentiles",
            percents,
            true,
            new String[] { "path" },
            SKIP,
            DocValueFormat.RAW,
            Collections.emptyMap(),
            interpolation
        );
        aggregator.preCollection();
        for (double value : values) {
            aggregator.collectBucketValue("bucket", value);
        }
        return (InternalPercentilesBucket) aggregator.buildAggregation(Map.of());
    }

    public void testValidate() {
        AggregationBuilder singleBucketAgg = new GlobalAggregationBuilder("global");
        AggregationBuilder multiBucketAgg = new TermsAggregationBuilder("terms").userValueTypeHint(ValueType.STRING);
        final Set<AggregationBuilder> aggBuilders = new HashSet<>();
        aggBuilders.add(singleBucketAgg);
        aggBuilders.add(multiBucketAgg);

        // First try to point to a non-existent agg
        assertThat(
            validate(aggBuilders, new PercentilesBucketPipelineAggregationBuilder("name", "invalid_agg>metric")),
            equalTo(
                "Validation Failed: 1: "
                    + PipelineAggregator.Parser.BUCKETS_PATH.getPreferredName()
                    + " aggregation does not exist for aggregation [name]: invalid_agg>metric;"
            )
        );

        // Now try to point to a single bucket agg
        assertThat(
            validate(aggBuilders, new PercentilesBucketPipelineAggregationBuilder("name", "global>metric")),
            equalTo(
                "Validation Failed: 1: Unable to find unqualified multi-bucket aggregation in "
                    + PipelineAggregator.Parser.BUCKETS_PATH.getPreferredName()
                    + ". Path must include a multi-bucket aggregation for aggregation [name] found :"
                    + GlobalAggregationBuilder.class.getName()
                    + " for buckets path: global>metric;"
            )
        );

        // Now try to point to a valid multi-bucket agg
        assertThat(validate(aggBuilders, new PercentilesBucketPipelineAggregationBuilder("name", "terms>metric")), nullValue());
    }
}
