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
package org.elasticsearch.search.aggregations.bucket.range.date;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.bucket.range.AbstractRangeBuilder;

import java.io.IOException;

/**
 * Builder for the {@code DateRange} aggregation.
 */
public class DateRangeBuilder extends AbstractRangeBuilder<DateRangeBuilder> {

    private String format;

    /**
     * Sole constructor.
     */
    public DateRangeBuilder(String name) {
        super(name, InternalDateRange.TYPE.name());
    }

    /**
     * Add a new range to this aggregation.
     *
     * @param key  the key to use for this range in the response
     * @param from the lower bound on the distances, inclusive
     * @param to   the upper bound on the distances, exclusive
     */
    public DateRangeBuilder addRange(String key, Object from, Object to) {
        ranges.add(new Range(key, from, to));
        return this;
    }

    /**
     * Same as {@link #addRange(String, Object, Object)} but the key will be
     * automatically generated based on <code>from</code> and <code>to</code>.
     */
    public DateRangeBuilder addRange(Object from, Object to) {
        return addRange(null, from, to);
    }

    /**
     * Add a new range with no lower bound.
     *
     * @param key the key to use for this range in the response
     * @param to  the upper bound on the distances, exclusive
     */
    public DateRangeBuilder addUnboundedTo(String key, Object to) {
        ranges.add(new Range(key, null, to));
        return this;
    }

    /**
     * Same as {@link #addUnboundedTo(String, Object)} but the key will be
     * computed automatically.
     */
    public DateRangeBuilder addUnboundedTo(Object to) {
        return addUnboundedTo(null, to);
    }

    /**
     * Add a new range with no upper bound.
     *
     * @param key  the key to use for this range in the response
     * @param from the lower bound on the distances, inclusive
     */
    public DateRangeBuilder addUnboundedFrom(String key, Object from) {
        ranges.add(new Range(key, from, null));
        return this;
    }

    /**
     * Same as {@link #addUnboundedFrom(String, Object)} but the key will be
     * computed automatically.
     */
    public DateRangeBuilder addUnboundedFrom(Object from) {
        return addUnboundedFrom(null, from);
    }

    /**
     * Set the format to use to display values.
     */
    public DateRangeBuilder format(String format) {
        this.format = format;
        return this;
    }

    @Override
    protected XContentBuilder doInternalXContent(XContentBuilder builder, Params params) throws IOException {
        super.doInternalXContent(builder, params);
        if (format != null) {
            builder.field("format", format);
        }
        return builder;
    }
}
