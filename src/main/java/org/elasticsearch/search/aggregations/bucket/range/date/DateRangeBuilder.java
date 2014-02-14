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
 *
 */
public class DateRangeBuilder extends AbstractRangeBuilder<DateRangeBuilder> {

    private String format;

    public DateRangeBuilder(String name) {
        super(name, InternalDateRange.TYPE.name());
    }

    public DateRangeBuilder addRange(String key, Object from, Object to) {
        ranges.add(new Range(key, from, to));
        return this;
    }

    public DateRangeBuilder addRange(Object from, Object to) {
        return addRange(null, from, to);
    }

    public DateRangeBuilder addUnboundedTo(String key, Object to) {
        ranges.add(new Range(key, null, to));
        return this;
    }

    public DateRangeBuilder addUnboundedTo(Object to) {
        return addUnboundedTo(null, to);
    }

    public DateRangeBuilder addUnboundedFrom(String key, Object from) {
        ranges.add(new Range(key, from, null));
        return this;
    }

    public DateRangeBuilder addUnboundedFrom(Object from) {
        return addUnboundedFrom(null, from);
    }

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
