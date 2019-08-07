/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.client.dataframe.transforms.pivot;

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
