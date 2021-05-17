/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.transform.transforms.pivot;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import static java.util.Collections.emptyList;

public class AggregationConfigTests extends AbstractXContentTestCase<AggregationConfig> {

    public static AggregationConfig randomAggregationConfig() {
        AggregatorFactories.Builder builder = new AggregatorFactories.Builder();
        Set<String> names = new HashSet<>();
        int numAggs = randomIntBetween(1, 4);
        for (int i = 0; i < numAggs; ++i) {
            AggregationBuilder aggBuilder = getRandomSupportedAggregation();
            if (names.add(aggBuilder.getName())) {
                builder.addAggregator(aggBuilder);
            }
        }
        return new AggregationConfig(builder);
    }

    @Override
    protected AggregationConfig createTestInstance() {
        return randomAggregationConfig();
    }

    @Override
    protected AggregationConfig doParseInstance(XContentParser parser) throws IOException {
        return AggregationConfig.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, emptyList());
        return new NamedXContentRegistry(searchModule.getNamedXContents());
    }

    private static AggregationBuilder getRandomSupportedAggregation() {
        final int numberOfSupportedAggs = 4;
        switch (randomIntBetween(1, numberOfSupportedAggs)) {
            case 1:
                return AggregationBuilders.avg(randomAlphaOfLengthBetween(1, 10)).field(randomAlphaOfLengthBetween(1, 10));
            case 2:
                return AggregationBuilders.min(randomAlphaOfLengthBetween(1, 10)).field(randomAlphaOfLengthBetween(1, 10));
            case 3:
                return AggregationBuilders.max(randomAlphaOfLengthBetween(1, 10)).field(randomAlphaOfLengthBetween(1, 10));
            case 4:
                return AggregationBuilders.sum(randomAlphaOfLengthBetween(1, 10)).field(randomAlphaOfLengthBetween(1, 10));
        }

        return null;
    }
}
