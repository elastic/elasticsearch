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
package org.elasticsearch.search.aggregations.metrics.percentiles;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.metrics.ValuesSourceMetricsAggregationBuilder;

import java.io.IOException;

/**
 * Builder for the {@link PercentileRanks} aggregation.
 */
public class PercentileRanksBuilder extends ValuesSourceMetricsAggregationBuilder<PercentileRanksBuilder> {

    private double[] values;
    private Double compression;

    /**
     * Sole constructor.
     */
    public PercentileRanksBuilder(String name) {
        super(name, InternalPercentileRanks.TYPE.name());
    }

    /**
     * Set the values to compute percentiles from.
     */
    public PercentileRanksBuilder percentiles(double... values) {
        this.values = values;
        return this;
    }

    /**
     * Expert: set the compression. Higher values improve accuracy but also memory usage.
     */
    public PercentileRanksBuilder compression(double compression) {
        this.compression = compression;
        return this;
    }

    @Override
    protected void internalXContent(XContentBuilder builder, Params params) throws IOException {
        super.internalXContent(builder, params);

        if (values != null) {
            builder.field("values", values);
        }

        if (compression != null) {
            builder.field("compression", compression);
        }
    }
}
