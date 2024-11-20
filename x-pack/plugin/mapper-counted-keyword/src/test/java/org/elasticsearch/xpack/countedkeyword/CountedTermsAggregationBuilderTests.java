/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.countedkeyword;

import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.aggregations.BaseAggregationTestCase;
import org.elasticsearch.search.aggregations.bucket.countedterms.CountedTermsAggregationBuilder;

import java.util.Collection;
import java.util.Collections;

public class CountedTermsAggregationBuilderTests extends BaseAggregationTestCase<CountedTermsAggregationBuilder> {
    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Collections.singletonList(CountedKeywordMapperPlugin.class);
    }

    @Override
    protected CountedTermsAggregationBuilder createTestAggregatorBuilder() {
        return new CountedTermsAggregationBuilder(randomAlphaOfLengthBetween(1, 10)).field(randomAlphaOfLength(7));
    }
}
