/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.correlation;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.BasePipelineAggregationTestCase;
import org.elasticsearch.search.aggregations.bucket.global.GlobalAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.xpack.ml.MachineLearning;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;

public class BucketCorrelationAggregationBuilderTests extends BasePipelineAggregationTestCase<BucketCorrelationAggregationBuilder> {

    private static final String NAME = "correlation-agg";

    @Override
    protected List<SearchPlugin> plugins() {
        return Collections.singletonList(new MachineLearning(Settings.EMPTY, null));
    }

    @Override
    protected List<NamedXContentRegistry.Entry> additionalNamedContents() {
        return new CorrelationNamedContentProvider().getNamedXContentParsers();
    }

    @Override
    protected List<NamedWriteableRegistry.Entry> additionalNamedWriteables() {
        return new CorrelationNamedContentProvider().getNamedWriteables();
    }

    @Override
    protected BucketCorrelationAggregationBuilder createTestAggregatorFactory() {
        CorrelationFunction function = new CountCorrelationFunction(CountCorrelationIndicatorTests.randomInstance());
        return new BucketCorrelationAggregationBuilder(
            NAME,
            randomAlphaOfLength(8),
            function
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
                new BucketCorrelationAggregationBuilder(
                    NAME,
                    "missing>metric",
                    new CountCorrelationFunction(CountCorrelationIndicatorTests.randomInstance())
                )
            ),
            containsString("aggregation does not exist for aggregation")
        );

        // Now validate with a single bucket agg
        assertThat(
            validate(
                aggBuilders,
                new BucketCorrelationAggregationBuilder(
                    NAME,
                    "global>metric",
                    new CountCorrelationFunction(CountCorrelationIndicatorTests.randomInstance())
                )
            ),
            containsString("must be a multi-bucket aggregation for aggregation")
        );
    }

}
