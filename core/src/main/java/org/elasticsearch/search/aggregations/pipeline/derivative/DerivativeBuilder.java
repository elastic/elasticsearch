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

package org.elasticsearch.search.aggregations.pipeline.derivative;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregatorBuilder;
import org.elasticsearch.search.aggregations.pipeline.BucketHelpers.GapPolicy;

import java.io.IOException;

public class DerivativeBuilder extends PipelineAggregatorBuilder<DerivativeBuilder> {

    private String format;
    private GapPolicy gapPolicy;
    private String unit;

    public DerivativeBuilder(String name) {
        super(name, DerivativePipelineAggregator.TYPE.name());
    }

    public DerivativeBuilder format(String format) {
        this.format = format;
        return this;
    }

    public DerivativeBuilder gapPolicy(GapPolicy gapPolicy) {
        this.gapPolicy = gapPolicy;
        return this;
    }

    public DerivativeBuilder unit(String unit) {
        this.unit = unit;
        return this;
    }

    /**
     * Sets the unit using the provided {@link DateHistogramInterval}. This
     * method is only useful when calculating the derivative using a
     * `date_histogram`
     */
    public DerivativeBuilder unit(DateHistogramInterval unit) {
        this.unit = unit.toString();
        return this;
    }

    @Override
    protected XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {
        if (format != null) {
            builder.field(DerivativeParser.FORMAT.getPreferredName(), format);
        }
        if (gapPolicy != null) {
            builder.field(DerivativeParser.GAP_POLICY.getPreferredName(), gapPolicy.getName());
        }
        if (unit != null) {
            builder.field(DerivativeParser.UNIT.getPreferredName(), unit);
        }
        return builder;
    }

}
