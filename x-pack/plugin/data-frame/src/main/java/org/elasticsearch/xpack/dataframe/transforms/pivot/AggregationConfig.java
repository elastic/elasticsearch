/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.dataframe.transforms.pivot;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories;

import java.io.IOException;
import java.util.Collection;
import java.util.Objects;

/*
 * Wrapper for the aggregations config part of a composite aggregation.
 *
 * For now just wraps aggregations from composite aggs.
 *
 */
public class AggregationConfig implements Writeable, ToXContentObject {

    private final AggregatorFactories.Builder aggregatorFactoryBuilder;

    public AggregationConfig(AggregatorFactories.Builder aggregatorFactoryBuilder) {
        this.aggregatorFactoryBuilder = aggregatorFactoryBuilder;
    }

    public AggregationConfig(final StreamInput in) throws IOException {
        aggregatorFactoryBuilder = new AggregatorFactories.Builder(in);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return aggregatorFactoryBuilder.toXContent(builder, params);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        aggregatorFactoryBuilder.writeTo(out);
    }

    public Collection<AggregationBuilder> getAggregatorFactories() {
        return aggregatorFactoryBuilder.getAggregatorFactories();
    }

    public static AggregationConfig fromXContent(final XContentParser parser) throws IOException {
        return new AggregationConfig(AggregatorFactories.parseAggregators(parser));
    }

    @Override
    public int hashCode() {
        return Objects.hash(aggregatorFactoryBuilder);
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

        return Objects.equals(this.aggregatorFactoryBuilder, that.aggregatorFactoryBuilder);
    }
}
