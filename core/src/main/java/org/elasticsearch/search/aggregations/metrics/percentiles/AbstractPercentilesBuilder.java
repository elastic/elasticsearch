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

abstract class AbstractPercentilesBuilder<PB extends AbstractPercentilesBuilder<PB>> extends
        ValuesSourceMetricsAggregationBuilder<PB> {

    private Double compression;
    private PercentilesMethod method;
    private Integer numberOfSignificantValueDigits;

    public AbstractPercentilesBuilder(String name, String type) {
        super(name, type);
    }

    /**
     * Expert: Set the method to use to compute the percentiles.
     */
    public PB method(PercentilesMethod method) {
        this.method = method;
        return (PB) this;
    }

    /**
     * Expert: set the compression. Higher values improve accuracy but also
     * memory usage. Only relevant when using {@link PercentilesMethod#TDIGEST}.
     */
    public PB compression(double compression) {
        this.compression = compression;
        return (PB) this;
    }

    /**
     * Expert: set the number of significant digits in the values. Only relevant
     * when using {@link PercentilesMethod#HDR}.
     */
    public PB numberOfSignificantValueDigits(int numberOfSignificantValueDigits) {
        this.numberOfSignificantValueDigits = numberOfSignificantValueDigits;
        return (PB) this;
    }

    @Override
    protected void internalXContent(XContentBuilder builder, Params params) throws IOException {
        super.internalXContent(builder, params);

        doInternalXContent(builder, params);

        if (method != null) {
            builder.startObject(method.getName());

            if (compression != null) {
                builder.field(AbstractPercentilesParser.COMPRESSION_FIELD.getPreferredName(), compression);
            }

            if (numberOfSignificantValueDigits != null) {
                builder.field(AbstractPercentilesParser.NUMBER_SIGNIFICANT_DIGITS_FIELD.getPreferredName(), numberOfSignificantValueDigits);
            }

            builder.endObject();
        }
    }

    protected abstract void doInternalXContent(XContentBuilder builder, Params params) throws IOException;

}