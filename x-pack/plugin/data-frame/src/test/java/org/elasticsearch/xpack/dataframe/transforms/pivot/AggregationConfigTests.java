/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.dataframe.transforms.pivot;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.xpack.dataframe.transforms.AbstractSerializingDataFrameTestCase;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;

public class AggregationConfigTests extends AbstractSerializingDataFrameTestCase<AggregationConfig> {

    public static AggregationConfig randomAggregationConfig() {
        AggregatorFactories.Builder builder = new AggregatorFactories.Builder();

        // ensure that the unlikely does not happen: 2 aggs share the same name
        Set<String> names = new HashSet<>();
        for (int i = 1; i < randomIntBetween(1, 20); ++i) {
            AggregationBuilder aggBuilder = getRandomSupportedAggregation();
            if (names.add(aggBuilder.getName())) {
                builder.addAggregator(aggBuilder);
            }
        }

        return new AggregationConfig(builder);
    }

    @Override
    protected AggregationConfig doParseInstance(XContentParser parser) throws IOException {
        // parseAggregators expects to be already inside the xcontent object
        assertThat(parser.nextToken(), equalTo(XContentParser.Token.START_OBJECT));
        return AggregationConfig.fromXContent(parser);
    }

    @Override
    protected AggregationConfig createTestInstance() {
        return randomAggregationConfig();
    }

    @Override
    protected Reader<AggregationConfig> instanceReader() {
        return AggregationConfig::new;
    }

    private static AggregationBuilder getRandomSupportedAggregation() {
        final int numberOfSupportedAggs = 4;
        switch (randomIntBetween(1, numberOfSupportedAggs)) {
        case 1:
            return AggregationBuilders.avg(randomAlphaOfLengthBetween(1, 10));
        case 2:
            return AggregationBuilders.min(randomAlphaOfLengthBetween(1, 10));
        case 3:
            return AggregationBuilders.max(randomAlphaOfLengthBetween(1, 10));
        case 4:
            return AggregationBuilders.sum(randomAlphaOfLengthBetween(1, 10));
        }

        return null;
    }
}
