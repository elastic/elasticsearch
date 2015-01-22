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

package org.elasticsearch.search.aggregations.metrics.stats.extended;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.metrics.ValuesSourceMetricsAggregationBuilder;

import java.io.IOException;

/**
 * Builder for the {@link ExtendedStats} aggregation.
 */
public class ExtendedStatsBuilder extends ValuesSourceMetricsAggregationBuilder<ExtendedStatsBuilder> {

    private Double sigma;

    /**
     * Sole constructor.
     */
    public ExtendedStatsBuilder(String name) {
        super(name, InternalExtendedStats.TYPE.name());
    }

    public ExtendedStatsBuilder sigma(double sigma) {
        this.sigma = sigma;
        return this;
    }

    @Override
    protected void internalXContent(XContentBuilder builder, Params params) throws IOException {
        super.internalXContent(builder, params);

        if (sigma != null) {
            builder.field("sigma", sigma);
        }

    }
}
