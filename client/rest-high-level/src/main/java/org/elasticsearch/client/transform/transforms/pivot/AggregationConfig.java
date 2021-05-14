/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.transform.transforms.pivot;

import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories;

import java.io.IOException;
import java.util.Collection;
import java.util.Objects;

public class AggregationConfig implements ToXContentObject {
    private final AggregatorFactories.Builder aggregations;

    public static AggregationConfig fromXContent(XContentParser parser) throws IOException {
        if (parser.currentToken() == null) {
            parser.nextToken();
        }
        AggregatorFactories.Builder aggregations = AggregatorFactories.parseAggregators(parser);
        return new AggregationConfig(aggregations);
    }

    public AggregationConfig(AggregatorFactories.Builder aggregations) {
        this.aggregations = aggregations;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        aggregations.toXContent(builder, params);
        return builder;
    }

    public Collection<AggregationBuilder> getAggregatorFactories() {
        return aggregations.getAggregatorFactories();
    }

    @Override
    public int hashCode() {
        return Objects.hash(aggregations);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        final AggregationConfig that = (AggregationConfig) other;

        return Objects.equals(this.aggregations, that.aggregations);
    }

}
