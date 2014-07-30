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
 *
 */
public class DateHistogramBuilder extends ValuesSourceAggregationBuilder<DateHistogramBuilder> {

    private Object interval;
    private Histogram.Order order;
    private Long minDocCount;
    private Object extendedBoundsMin;
    private Object extendedBoundsMax;
    private String preZone;
    private String postZone;
    private boolean preZoneAdjustLargeInterval;
    private String format;
    private String preOffset;
    private String postOffset;
    float factor = 1.0f;

    public DateHistogramBuilder(String name) {
        super(name, InternalDateHistogram.TYPE.name());
    }

    public DateHistogramBuilder interval(long interval) {
        this.interval = interval;
        return this;
    }

    public DateHistogramBuilder interval(DateHistogram.Interval interval) {
        this.interval = interval;
        return this;
    }

    public DateHistogramBuilder order(DateHistogram.Order order) {
        this.order = order;
        return this;
    }

    public DateHistogramBuilder minDocCount(long minDocCount) {
        this.minDocCount = minDocCount;
        return this;
    }

    public DateHistogramBuilder preZone(String preZone) {
        this.preZone = preZone;
        return this;
    }

    public DateHistogramBuilder postZone(String postZone) {
        this.postZone = postZone;
        return this;
    }

    public DateHistogramBuilder preZoneAdjustLargeInterval(boolean preZoneAdjustLargeInterval) {
        this.preZoneAdjustLargeInterval = preZoneAdjustLargeInterval;
        return this;
    }

    public DateHistogramBuilder preOffset(String preOffset) {
        this.preOffset = preOffset;
        return this;
    }

    public DateHistogramBuilder postOffset(String postOffset) {
        this.postOffset = postOffset;
        return this;
    }

    public DateHistogramBuilder factor(float factor) {
        this.factor = factor;
        return this;
    }

    public DateHistogramBuilder format(String format) {
        this.format = format;
        return this;
    }

    public DateHistogramBuilder extendedBounds(Long min, Long max) {
        extendedBoundsMin = min;
        extendedBoundsMax = max;
        return this;
    }

    public DateHistogramBuilder extendedBounds(String min, String max) {
        extendedBoundsMin = min;
        extendedBoundsMax = max;
        return this;
    }

    public DateHistogramBuilder extendedBounds(DateTime min, DateTime max) {
        extendedBoundsMin = min;
        extendedBoundsMax = max;
        return this;
    }

    @Override
    protected XContentBuilder doInternalXContent(XContentBuilder builder, Params params) throws IOException {
        if (interval == null) {
            throw new SearchSourceBuilderException("[interval] must be defined for histogram aggregation [" + name + "]");
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

        if (preZone != null) {
            builder.field("pre_zone", preZone);
        }

        if (postZone != null) {
            builder.field("post_zone", postZone);
        }

        if (preZoneAdjustLargeInterval) {
            builder.field("pre_zone_adjust_large_interval", true);
        }

        if (preOffset != null) {
            builder.field("pre_offset", preOffset);
        }

        if (postOffset != null) {
            builder.field("post_offset", postOffset);
        }

        if (factor != 1.0f) {
            builder.field("factor", factor);
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
