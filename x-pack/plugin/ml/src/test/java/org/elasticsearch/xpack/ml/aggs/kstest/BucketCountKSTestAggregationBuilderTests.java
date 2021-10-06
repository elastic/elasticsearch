/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.kstest;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.BasePipelineAggregationTestCase;
import org.elasticsearch.search.aggregations.bucket.global.GlobalAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ml.MachineLearning;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.containsString;

public class BucketCountKSTestAggregationBuilderTests extends BasePipelineAggregationTestCase<BucketCountKSTestAggregationBuilder> {

    private static final String NAME = "ks-test-agg";

    @Override
    protected List<SearchPlugin> plugins() {
        return Collections.singletonList(new MachineLearning(Settings.EMPTY, null));
    }

    @Override
    protected BucketCountKSTestAggregationBuilder createTestAggregatorFactory() {
        return new BucketCountKSTestAggregationBuilder(
            NAME,
            randomAlphaOfLength(10),
            Stream.generate(ESTestCase::randomDouble).limit(100).collect(Collectors.toList()),
            Stream.generate(() -> randomFrom(Alternative.GREATER, Alternative.LESS, Alternative.TWO_SIDED))
                .limit(4)
                .map(Alternative::toString)
                .collect(Collectors.toList()),
            randomFrom(new SamplingMethod.UpperTail(), new SamplingMethod.LowerTail(), new SamplingMethod.Uniform())
        );
    }

    public void testValidate() {
        AggregationBuilder singleBucketAgg = new GlobalAggregationBuilder("global");
        AggregationBuilder multiBucketAgg = new TermsAggregationBuilder("terms").userValueTypeHint(ValueType.STRING);
        final Set<AggregationBuilder> aggBuilders = new HashSet<>();
        aggBuilders.add(singleBucketAgg);
        aggBuilders.add(multiBucketAgg);

        // First try to point to a non-existent agg
        assertThat(
            validate(
                aggBuilders,
                new BucketCountKSTestAggregationBuilder(
                    NAME,
                    "missing>metric",
                    Stream.generate(ESTestCase::randomDouble).limit(100).collect(Collectors.toList()),
                    Stream.generate(() -> randomFrom(Alternative.GREATER, Alternative.LESS, Alternative.TWO_SIDED))
                        .limit(4)
                        .map(Alternative::toString)
                        .collect(Collectors.toList()),
                    new SamplingMethod.UpperTail()
                )
            ),
            containsString("aggregation does not exist for aggregation")
        );

        // Now validate with a single bucket agg
        assertThat(
            validate(
                aggBuilders,
                new BucketCountKSTestAggregationBuilder(
                    NAME,
                    "global>metric",
                    Stream.generate(ESTestCase::randomDouble).limit(100).collect(Collectors.toList()),
                    Stream.generate(() -> randomFrom(Alternative.GREATER, Alternative.LESS, Alternative.TWO_SIDED))
                        .limit(4)
                        .map(Alternative::toString)
                        .collect(Collectors.toList()),
                    new SamplingMethod.UpperTail()
                )
            ),
            containsString("must be a multi-bucket aggregation for aggregation")
        );
    }

}
