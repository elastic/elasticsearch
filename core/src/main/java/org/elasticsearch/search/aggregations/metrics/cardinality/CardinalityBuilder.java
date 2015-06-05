/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.aggregations.metrics.cardinality;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.metrics.ValuesSourceMetricsAggregationBuilder;

import java.io.IOException;

/**
 * Builder for the {@link Cardinality} aggregation.
 */
public class CardinalityBuilder extends ValuesSourceMetricsAggregationBuilder<CardinalityBuilder> {

    private Long precisionThreshold;
    private Boolean rehash;

    /**
     * Sole constructor.
     */
    public CardinalityBuilder(String name) {
        super(name, InternalCardinality.TYPE.name());
    }

    /**
     * Set a precision threshold. Higher values improve accuracy but also
     * increase memory usage.
     */
    public CardinalityBuilder precisionThreshold(long precisionThreshold) {
        this.precisionThreshold = precisionThreshold;
        return this;
    }

    /**
     * Expert: set to false in case values of this field can already be treated
     * as 64-bits hash values.
     */
    public CardinalityBuilder rehash(boolean rehash) {
        this.rehash = rehash;
        return this;
    }

    @Override
    protected void internalXContent(XContentBuilder builder, Params params) throws IOException {
        super.internalXContent(builder, params);
        if (precisionThreshold != null) {
            builder.field("precision_threshold", precisionThreshold);
        }
        if (rehash != null) {
            builder.field("rehash", rehash);
        }
    }

}
