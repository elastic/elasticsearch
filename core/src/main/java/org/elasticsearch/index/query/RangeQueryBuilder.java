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

package org.elasticsearch.index.query;

import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * A Query that matches documents within an range of terms.
 */
public class RangeQueryBuilder extends MultiTermQueryBuilder implements BoostableQueryBuilder<RangeQueryBuilder> {

    private final String name;
    private Object from;
    private Object to;
    private String timeZone;
    private boolean includeLower = true;
    private boolean includeUpper = true;
    private float boost = -1;
    private String queryName;
    private String format;

    /**
     * A Query that matches documents within an range of terms.
     *
     * @param name The field name
     */
    public RangeQueryBuilder(String name) {
        this.name = name;
    }

    /**
     * The from part of the range query. Null indicates unbounded.
     */
    public RangeQueryBuilder from(Object from) {
        this.from = from;
        return this;
    }

    /**
     * The from part of the range query. Null indicates unbounded.
     */
    public RangeQueryBuilder from(String from) {
        this.from = from;
        return this;
    }

    /**
     * The from part of the range query. Null indicates unbounded.
     */
    public RangeQueryBuilder from(int from) {
        this.from = from;
        return this;
    }

    /**
     * The from part of the range query. Null indicates unbounded.
     */
    public RangeQueryBuilder from(long from) {
        this.from = from;
        return this;
    }

    /**
     * The from part of the range query. Null indicates unbounded.
     */
    public RangeQueryBuilder from(float from) {
        this.from = from;
        return this;
    }

    /**
     * The from part of the range query. Null indicates unbounded.
     */
    public RangeQueryBuilder from(double from) {
        this.from = from;
        return this;
    }

    /**
     * The from part of the range query. Null indicates unbounded.
     */
    public RangeQueryBuilder gt(String from) {
        this.from = from;
        this.includeLower = false;
        return this;
    }

    /**
     * The from part of the range query. Null indicates unbounded.
     */
    public RangeQueryBuilder gt(Object from) {
        this.from = from;
        this.includeLower = false;
        return this;
    }

    /**
     * The from part of the range query. Null indicates unbounded.
     */
    public RangeQueryBuilder gt(int from) {
        this.from = from;
        this.includeLower = false;
        return this;
    }

    /**
     * The from part of the range query. Null indicates unbounded.
     */
    public RangeQueryBuilder gt(long from) {
        this.from = from;
        this.includeLower = false;
        return this;
    }

    /**
     * The from part of the range query. Null indicates unbounded.
     */
    public RangeQueryBuilder gt(float from) {
        this.from = from;
        this.includeLower = false;
        return this;
    }

    /**
     * The from part of the range query. Null indicates unbounded.
     */
    public RangeQueryBuilder gt(double from) {
        this.from = from;
        this.includeLower = false;
        return this;
    }

    /**
     * The from part of the range query. Null indicates unbounded.
     */
    public RangeQueryBuilder gte(String from) {
        this.from = from;
        this.includeLower = true;
        return this;
    }

    /**
     * The from part of the range query. Null indicates unbounded.
     */
    public RangeQueryBuilder gte(Object from) {
        this.from = from;
        this.includeLower = true;
        return this;
    }

    /**
     * The from part of the range query. Null indicates unbounded.
     */
    public RangeQueryBuilder gte(int from) {
        this.from = from;
        this.includeLower = true;
        return this;
    }

    /**
     * The from part of the range query. Null indicates unbounded.
     */
    public RangeQueryBuilder gte(long from) {
        this.from = from;
        this.includeLower = true;
        return this;
    }

    /**
     * The from part of the range query. Null indicates unbounded.
     */
    public RangeQueryBuilder gte(float from) {
        this.from = from;
        this.includeLower = true;
        return this;
    }

    /**
     * The from part of the range query. Null indicates unbounded.
     */
    public RangeQueryBuilder gte(double from) {
        this.from = from;
        this.includeLower = true;
        return this;
    }

    /**
     * The to part of the range query. Null indicates unbounded.
     */
    public RangeQueryBuilder to(Object to) {
        this.to = to;
        return this;
    }

    /**
     * The to part of the range query. Null indicates unbounded.
     */
    public RangeQueryBuilder to(String to) {
        this.to = to;
        return this;
    }

    /**
     * The to part of the range query. Null indicates unbounded.
     */
    public RangeQueryBuilder to(int to) {
        this.to = to;
        return this;
    }

    /**
     * The to part of the range query. Null indicates unbounded.
     */
    public RangeQueryBuilder to(long to) {
        this.to = to;
        return this;
    }

    /**
     * The to part of the range query. Null indicates unbounded.
     */
    public RangeQueryBuilder to(float to) {
        this.to = to;
        return this;
    }

    /**
     * The to part of the range query. Null indicates unbounded.
     */
    public RangeQueryBuilder to(double to) {
        this.to = to;
        return this;
    }

    /**
     * The to part of the range query. Null indicates unbounded.
     */
    public RangeQueryBuilder lt(String to) {
        this.to = to;
        this.includeUpper = false;
        return this;
    }

    /**
     * The to part of the range query. Null indicates unbounded.
     */
    public RangeQueryBuilder lt(Object to) {
        this.to = to;
        this.includeUpper = false;
        return this;
    }

    /**
     * The to part of the range query. Null indicates unbounded.
     */
    public RangeQueryBuilder lt(int to) {
        this.to = to;
        this.includeUpper = false;
        return this;
    }

    /**
     * The to part of the range query. Null indicates unbounded.
     */
    public RangeQueryBuilder lt(long to) {
        this.to = to;
        this.includeUpper = false;
        return this;
    }

    /**
     * The to part of the range query. Null indicates unbounded.
     */
    public RangeQueryBuilder lt(float to) {
        this.to = to;
        this.includeUpper = false;
        return this;
    }

    /**
     * The to part of the range query. Null indicates unbounded.
     */
    public RangeQueryBuilder lt(double to) {
        this.to = to;
        this.includeUpper = false;
        return this;
    }

    /**
     * The to part of the range query. Null indicates unbounded.
     */
    public RangeQueryBuilder lte(String to) {
        this.to = to;
        this.includeUpper = true;
        return this;
    }

    /**
     * The to part of the range query. Null indicates unbounded.
     */
    public RangeQueryBuilder lte(Object to) {
        this.to = to;
        this.includeUpper = true;
        return this;
    }

    /**
     * The to part of the range query. Null indicates unbounded.
     */
    public RangeQueryBuilder lte(int to) {
        this.to = to;
        this.includeUpper = true;
        return this;
    }

    /**
     * The to part of the range query. Null indicates unbounded.
     */
    public RangeQueryBuilder lte(long to) {
        this.to = to;
        this.includeUpper = true;
        return this;
    }

    /**
     * The to part of the range query. Null indicates unbounded.
     */
    public RangeQueryBuilder lte(float to) {
        this.to = to;
        this.includeUpper = true;
        return this;
    }

    /**
     * The to part of the range query. Null indicates unbounded.
     */
    public RangeQueryBuilder lte(double to) {
        this.to = to;
        this.includeUpper = true;
        return this;
    }

    /**
     * Should the lower bound be included or not. Defaults to <tt>true</tt>.
     */
    public RangeQueryBuilder includeLower(boolean includeLower) {
        this.includeLower = includeLower;
        return this;
    }

    /**
     * Should the upper bound be included or not. Defaults to <tt>true</tt>.
     */
    public RangeQueryBuilder includeUpper(boolean includeUpper) {
        this.includeUpper = includeUpper;
        return this;
    }

    /**
     * Sets the boost for this query.  Documents matching this query will (in addition to the normal
     * weightings) have their score multiplied by the boost provided.
     */
    @Override
    public RangeQueryBuilder boost(float boost) {
        this.boost = boost;
        return this;
    }

    /**
     * Sets the query name for the filter that can be used when searching for matched_filters per hit.
     */
    public RangeQueryBuilder queryName(String queryName) {
        this.queryName = queryName;
        return this;
    }

    /**
     * In case of date field, we can adjust the from/to fields using a timezone
     */
    public RangeQueryBuilder timeZone(String timezone) {
        this.timeZone = timezone;
        return this;
    }

    /**
     * In case of date field, we can set the format to be used instead of the mapper format
     */
    public RangeQueryBuilder format(String format) {
        this.format = format;
        return this;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(RangeQueryParser.NAME);
        builder.startObject(name);
        builder.field("from", from);
        builder.field("to", to);
        if (timeZone != null) {
            builder.field("time_zone", timeZone);
        }
        if (format != null) {
            builder.field("format", format);
        }
        builder.field("include_lower", includeLower);
        builder.field("include_upper", includeUpper);
        if (boost != -1) {
            builder.field("boost", boost);
        }
        builder.endObject();
        if (queryName != null) {
            builder.field("_name", queryName);
        }
        builder.endObject();
    }
}
