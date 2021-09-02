/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.pipeline;

import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.global.GlobalAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValueType;

import java.util.HashSet;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class SumBucketTests extends AbstractBucketMetricsTestCase<SumBucketPipelineAggregationBuilder> {

    @Override
    protected SumBucketPipelineAggregationBuilder doCreateTestAggregatorFactory(String name, String bucketsPath) {
        return new SumBucketPipelineAggregationBuilder(name, bucketsPath);
    }

    public void testValidate() {
        AggregationBuilder singleBucketAgg = new GlobalAggregationBuilder("global");
        AggregationBuilder multiBucketAgg = new TermsAggregationBuilder("terms").userValueTypeHint(ValueType.STRING);
        final Set<AggregationBuilder> aggBuilders = new HashSet<>();
        aggBuilders.add(singleBucketAgg);
        aggBuilders.add(multiBucketAgg);

        // First try to point to a non-existent agg
        assertThat(
            validate(aggBuilders, new SumBucketPipelineAggregationBuilder("name", "invalid_agg>metric")),
            equalTo(
                "Validation Failed: 1: "
                    + PipelineAggregator.Parser.BUCKETS_PATH.getPreferredName()
                    + " aggregation does not exist for aggregation [name]: invalid_agg>metric;"
            )
        );

        // Now try to point to a single bucket agg
        assertThat(
            validate(aggBuilders, new SumBucketPipelineAggregationBuilder("name", "global>metric")),
            equalTo(
                "Validation Failed: 1: The first aggregation in "
                    + PipelineAggregator.Parser.BUCKETS_PATH.getPreferredName()
                    + " must be a multi-bucket aggregation for aggregation [name] found :"
                    + GlobalAggregationBuilder.class.getName()
                    + " for buckets path: global>metric;"
            )
        );

        // Now try to point to a valid multi-bucket agg
        assertThat(validate(aggBuilders, new SumBucketPipelineAggregationBuilder("name", "terms>metric")), nullValue());
    }

}
