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

import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.ValuesSourceAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilderException;
import org.joda.time.DateTime;

import java.io.IOException;

/**
 * Builder for the {@link DateHistogram} aggregation.
 */
public class DateHistogramBuilder extends ValuesSourceAggregationBuilder<DateHistogramBuilder> {

    private Object interval;
    private Histogram.Order order;
    private Long minDocCount;
    private Object extendedBoundsMin;
    private Object extendedBoundsMax;
    private String timeZone;
    private String format;
    private String offset;

    /**
     * Sole constructor.
     */
    public DateHistogramBuilder(String name) {
        super(name, InternalDateHistogram.TYPE.name());
    }

    /**
     * Set the interval in milliseconds.
     */
    public DateHistogramBuilder interval(long interval) {
        this.interval = interval;
        return this;
    }

    /**
     * Set the interval.
     */
    public DateHistogramBuilder interval(DateHistogramInterval interval) {
        this.interval = interval;
        return this;
    }

    /**
     * Set the order by which the buckets will be returned.
     */
    public DateHistogramBuilder order(Histogram.Order order) {
        this.order = order;
        return this;
    }

    /**
     * Set the minimum document count per bucket. Buckets with less documents
     * than this min value will not be returned.
     */
    public DateHistogramBuilder minDocCount(long minDocCount) {
        this.minDocCount = minDocCount;
        return this;
    }

    /**
     * Set the timezone in which to translate dates before computing buckets.
     */
    public DateHistogramBuilder timeZone(String timeZone) {
        this.timeZone = timeZone;
        return this;
    }

    /**
     * @param offset sets the offset of time intervals in this histogram
     * @return the current builder
     */
    public DateHistogramBuilder offset(String offset) {
       this.offset = offset;
       return this;
    }

    /**
     * Set the format to use for dates.
     */
    public DateHistogramBuilder format(String format) {
        this.format = format;
        return this;
    }

    /**
     * Set extended bounds for the histogram. In case the lower value in the
     * histogram would be greater than <code>min</code> or the upper value would
     * be less than <code>max</code>, empty buckets will be generated.
     */
    public DateHistogramBuilder extendedBounds(Long min, Long max) {
        extendedBoundsMin = min;
        extendedBoundsMax = max;
        return this;
    }

    /**
     * Set extended bounds for the histogram. In case the lower value in the
     * histogram would be greater than <code>min</code> or the upper value would
     * be less than <code>max</code>, empty buckets will be generated.
     */
    public DateHistogramBuilder extendedBounds(String min, String max) {
        extendedBoundsMin = min;
        extendedBoundsMax = max;
        return this;
    }

    /**
     * Set extended bounds for the histogram. In case the lower value in the
     * histogram would be greater than <code>min</code> or the upper value would
     * be less than <code>max</code>, empty buckets will be generated.
     */
    public DateHistogramBuilder extendedBounds(DateTime min, DateTime max) {
        extendedBoundsMin = min;
        extendedBoundsMax = max;
        return this;
    }

    @Override
    protected XContentBuilder doInternalXContent(XContentBuilder builder, Params params) throws IOException {
        if (interval == null) {
            throw new SearchSourceBuilderException("[interval] must be defined for histogram aggregation [" + getName() + "]");
        }
        if (interval instanceof Number) {
            interval = TimeValue.timeValueMillis(((Number) interval).longValue()).toString();
        }
        builder.field("interval", interval);

        if (minDocCount != null) {
            builder.field("min_doc_count", minDocCount);
        }

        if (order != null) {
            builder.field("order");
            order.toXContent(builder, params);
        }

        if (timeZone != null) {
            builder.field("time_zone", timeZone);
        }

        if (offset != null) {
            builder.field("offset", offset);
        }

        if (format != null) {
            builder.field("format", format);
        }

        if (extendedBoundsMin != null || extendedBoundsMax != null) {
            builder.startObject(DateHistogramParser.EXTENDED_BOUNDS.getPreferredName());
            if (extendedBoundsMin != null) {
                builder.field("min", extendedBoundsMin);
            }
            if (extendedBoundsMax != null) {
                builder.field("max", extendedBoundsMax);
            }
            builder.endObject();
        }

        return builder;
    }
}
