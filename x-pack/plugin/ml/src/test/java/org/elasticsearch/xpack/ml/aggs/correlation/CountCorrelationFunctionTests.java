/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.correlation;

import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.PipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ml.aggs.MlAggsHelper;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;

public class CountCorrelationFunctionTests extends ESTestCase {

    private static final Aggregations EMPTY_AGGS = new Aggregations(Collections.emptyList());

    public void testExecute() {
        AtomicLong xs = new AtomicLong(1);
        CountCorrelationIndicator x = new CountCorrelationIndicator(
            Stream.generate(xs::incrementAndGet).limit(100).mapToDouble(l -> (double) l).toArray(),
            null,
            1000
        );
        CountCorrelationFunction countCorrelationFunction = new CountCorrelationFunction(x);
        AtomicLong ys = new AtomicLong(0);
        MlAggsHelper.DoubleBucketValues yValues = new MlAggsHelper.DoubleBucketValues(
            Stream.generate(() -> 10L).limit(100).mapToLong(l -> l).toArray(),
            Stream.generate(() -> Math.min(ys.incrementAndGet(), 10)).limit(100).mapToDouble(l -> (double) l).toArray()
        );
        double value = countCorrelationFunction.execute(yValues, EMPTY_AGGS);
        assertThat(value, greaterThan(0.0));

        AtomicLong otherYs = new AtomicLong(0);
        MlAggsHelper.DoubleBucketValues lesserYValues = new MlAggsHelper.DoubleBucketValues(
            Stream.generate(() -> 10L).limit(100).mapToLong(l -> l).toArray(),
            Stream.generate(() -> Math.min(otherYs.incrementAndGet(), 5)).limit(100).mapToDouble(l -> (double) l).toArray()
        );
        assertThat(countCorrelationFunction.execute(lesserYValues, EMPTY_AGGS), allOf(lessThan(value), greaterThan(0.0)));
    }

    public void testValidation() {
        AggregationBuilder multiBucketAgg = new TermsAggregationBuilder("terms").userValueTypeHint(ValueType.STRING);
        final Set<AggregationBuilder> aggBuilders = new HashSet<>();
        aggBuilders.add(multiBucketAgg);
        CountCorrelationFunction function = new CountCorrelationFunction(CountCorrelationIndicatorTests.randomInstance());
        PipelineAggregationBuilder.ValidationContext validationContext = PipelineAggregationBuilder.ValidationContext.forTreeRoot(
            aggBuilders,
            Collections.emptyList(),
            null
        );
        function.validate(validationContext, "terms>metric_agg", "foo");

        assertThat(
            validationContext.getValidationException().getMessage(),
            containsString("count correlation requires that bucket_path points to bucket [_count]")
        );
    }
}
