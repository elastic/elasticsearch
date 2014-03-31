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
package org.elasticsearch.search.aggregations.bucket.range;

import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

/**
 *
 */
public class RangeBuilder extends AbstractRangeBuilder<RangeBuilder> {

    private String format;

    public RangeBuilder(String name) {
        super(name, InternalRange.TYPE.name());
    }

    public RangeBuilder addRange(String key, double from, double to) {
        ranges.add(new Range(key, from, to));
        return this;
    }

    public RangeBuilder addRange(double from, double to) {
        return addRange(null, from, to);
    }

    public RangeBuilder addUnboundedTo(String key, double to) {
        ranges.add(new Range(key, null, to));
        return this;
    }

    public RangeBuilder addUnboundedTo(double to) {
        return addUnboundedTo(null, to);
    }

    public RangeBuilder addUnboundedFrom(String key, double from) {
        ranges.add(new Range(key, from, null));
        return this;
    }

    public RangeBuilder addUnboundedFrom(double from) {
        return addUnboundedFrom(null, from);
    }

    public RangeBuilder format(String format) {
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
