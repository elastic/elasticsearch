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

package org.elasticsearch.search.aggregations.bucket.histogram;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories.Builder;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource.Numeric;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;

import java.io.IOException;
import java.util.Objects;

public class DateHistogramAggregationBuilder extends AbstractHistogramBuilder<DateHistogramAggregationBuilder> {

    public static final String NAME = InternalDateHistogram.TYPE.name();
    public static final ParseField AGGREGATION_NAME_FIELD = new ParseField(NAME);

    private DateHistogramInterval dateHistogramInterval;

    public DateHistogramAggregationBuilder(String name) {
        super(name, InternalDateHistogram.HISTOGRAM_FACTORY);
    }

    /**
     * Read from a stream.
     */
    public DateHistogramAggregationBuilder(StreamInput in) throws IOException {
        super(in, InternalDateHistogram.HISTOGRAM_FACTORY);
        dateHistogramInterval = in.readOptionalWriteable(DateHistogramInterval::new);
    }

    @Override
    protected void innerWriteTo(StreamOutput out) throws IOException {
        super.innerWriteTo(out);
        out.writeOptionalWriteable(dateHistogramInterval);
    }

    /**
     * Set the interval.
     */
    public DateHistogramAggregationBuilder dateHistogramInterval(DateHistogramInterval dateHistogramInterval) {
        if (dateHistogramInterval == null) {
            throw new IllegalArgumentException("[dateHistogramInterval] must not be null: [" + name + "]");
        }
        this.dateHistogramInterval = dateHistogramInterval;
        return this;
    }

    public DateHistogramAggregationBuilder offset(String offset) {
        if (offset == null) {
            throw new IllegalArgumentException("[offset] must not be null: [" + name + "]");
        }
        return offset(parseStringOffset(offset));
    }

    protected static long parseStringOffset(String offset) {
        if (offset.charAt(0) == '-') {
            return -TimeValue
                    .parseTimeValue(offset.substring(1), null, DateHistogramAggregationBuilder.class.getSimpleName() + ".parseOffset")
                    .millis();
        }
        int beginIndex = offset.charAt(0) == '+' ? 1 : 0;
        return TimeValue
                .parseTimeValue(offset.substring(beginIndex), null, DateHistogramAggregationBuilder.class.getSimpleName() + ".parseOffset")
                .millis();
    }

    public DateHistogramInterval dateHistogramInterval() {
        return dateHistogramInterval;
    }

    @Override
    protected DateHistogramAggregatorFactory innerBuild(AggregationContext context, ValuesSourceConfig<Numeric> config,
            AggregatorFactory<?> parent, Builder subFactoriesBuilder) throws IOException {
        return new DateHistogramAggregatorFactory(name, type, config, interval, dateHistogramInterval, offset, order, keyed, minDocCount,
                extendedBounds, context, parent, subFactoriesBuilder, metaData);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    protected XContentBuilder doXContentInterval(XContentBuilder builder, Params params) throws IOException {
        if (dateHistogramInterval == null) {
            super.doXContentInterval(builder, params);
        } else {
            builder.value(dateHistogramInterval.toString());
        }
        return builder;
    }

    @Override
    protected int innerHashCode() {
        return Objects.hash(super.innerHashCode(), dateHistogramInterval);
    }

    @Override
    protected boolean innerEquals(Object obj) {
        DateHistogramAggregationBuilder other = (DateHistogramAggregationBuilder) obj;
        return super.innerEquals(obj) && Objects.equals(dateHistogramInterval, other.dateHistogramInterval);
    }
}
