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

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.AggregatorFactories.Builder;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.ValuesSource.Numeric;

import java.io.IOException;
import java.util.Objects;

public class DateHistogramAggregatorBuilder extends AbstractHistogramBuilder<DateHistogramAggregatorBuilder> {

    public static final DateHistogramAggregatorBuilder PROTOTYPE = new DateHistogramAggregatorBuilder("");

    private DateHistogramInterval dateHistogramInterval;

    public DateHistogramAggregatorBuilder(String name) {
        super(name, InternalDateHistogram.TYPE, InternalDateHistogram.HISTOGRAM_FACTORY);
    }

    /**
     * Set the interval.
     */
    public DateHistogramAggregatorBuilder dateHistogramInterval(DateHistogramInterval dateHistogramInterval) {
        if (dateHistogramInterval == null) {
            throw new IllegalArgumentException("[dateHistogramInterval] must not be null: [" + name + "]");
        }
        this.dateHistogramInterval = dateHistogramInterval;
        return this;
    }

    public DateHistogramAggregatorBuilder offset(String offset) {
        if (offset == null) {
            throw new IllegalArgumentException("[offset] must not be null: [" + name + "]");
        }
        return offset(parseStringOffset(offset));
    }

    protected static long parseStringOffset(String offset) {
        if (offset.charAt(0) == '-') {
            return -TimeValue
                    .parseTimeValue(offset.substring(1), null, DateHistogramAggregatorBuilder.class.getSimpleName() + ".parseOffset")
                    .millis();
        }
        int beginIndex = offset.charAt(0) == '+' ? 1 : 0;
        return TimeValue
                .parseTimeValue(offset.substring(beginIndex), null, DateHistogramAggregatorBuilder.class.getSimpleName() + ".parseOffset")
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
    protected XContentBuilder doXContentInterval(XContentBuilder builder, Params params) throws IOException {
        if (dateHistogramInterval == null) {
            super.doXContentInterval(builder, params);
        } else {
            builder.value(dateHistogramInterval.toString());
        }
        return builder;
    }

    @Override
    protected DateHistogramAggregatorBuilder createFactoryFromStream(String name, StreamInput in) throws IOException {
        DateHistogramAggregatorBuilder factory = new DateHistogramAggregatorBuilder(name);
        if (in.readBoolean()) {
            factory.dateHistogramInterval = DateHistogramInterval.readFromStream(in);
        }
        return factory;
    }

    @Override
    protected void writeFactoryToStream(StreamOutput out) throws IOException {
        boolean hasDateInterval = dateHistogramInterval != null;
        out.writeBoolean(hasDateInterval);
        if (hasDateInterval) {
            dateHistogramInterval.writeTo(out);
        }
    }

    @Override
    protected int innerHashCode() {
        return Objects.hash(super.innerHashCode(), dateHistogramInterval);
    }

    @Override
    protected boolean innerEquals(Object obj) {
        DateHistogramAggregatorBuilder other = (DateHistogramAggregatorBuilder) obj;
        return super.innerEquals(obj) && Objects.equals(dateHistogramInterval, other.dateHistogramInterval);
    }
}