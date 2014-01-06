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
 * A filter that restricts search results to values that are within the given numeric range.
 * <p/>
 * <p>Uses the field data cache (loading all the values for the specified field into memory).
 *
 * @deprecated This filter will be removed at some point in time in favor for the range filter with the execution
 *             mode <code>fielddata</code>.
 */
@Deprecated
public class NumericRangeFilterBuilder extends BaseFilterBuilder {

    private final String name;

    private Object from;

    private Object to;

    private boolean includeLower = true;

    private boolean includeUpper = true;

    private Boolean cache;
    private String cacheKey;

    private String filterName;

    /**
     * A filter that restricts search results to values that are within the given range.
     *
     * @param name The field name
     */
    public NumericRangeFilterBuilder(String name) {
        this.name = name;
    }

    /**
     * The from part of the filter query. Null indicates unbounded.
     */
    public NumericRangeFilterBuilder from(Object from) {
        this.from = from;
        return this;
    }

    /**
     * The from part of the filter query. Null indicates unbounded.
     */
    public NumericRangeFilterBuilder from(int from) {
        this.from = from;
        return this;
    }

    /**
     * The from part of the filter query. Null indicates unbounded.
     */
    public NumericRangeFilterBuilder from(long from) {
        this.from = from;
        return this;
    }

    /**
     * The from part of the filter query. Null indicates unbounded.
     */
    public NumericRangeFilterBuilder from(float from) {
        this.from = from;
        return this;
    }

    /**
     * The from part of the filter query. Null indicates unbounded.
     */
    public NumericRangeFilterBuilder from(double from) {
        this.from = from;
        return this;
    }

    /**
     * The from part of the filter query. Null indicates unbounded.
     */
    public NumericRangeFilterBuilder gt(Object from) {
        this.from = from;
        this.includeLower = false;
        return this;
    }

    /**
     * The from part of the filter query. Null indicates unbounded.
     */
    public NumericRangeFilterBuilder gt(int from) {
        this.from = from;
        this.includeLower = false;
        return this;
    }

    /**
     * The from part of the filter query. Null indicates unbounded.
     */
    public NumericRangeFilterBuilder gt(long from) {
        this.from = from;
        this.includeLower = false;
        return this;
    }

    /**
     * The from part of the filter query. Null indicates unbounded.
     */
    public NumericRangeFilterBuilder gt(float from) {
        this.from = from;
        this.includeLower = false;
        return this;
    }

    /**
     * The from part of the filter query. Null indicates unbounded.
     */
    public NumericRangeFilterBuilder gt(double from) {
        this.from = from;
        this.includeLower = false;
        return this;
    }

    /**
     * The from part of the filter query. Null indicates unbounded.
     */
    public NumericRangeFilterBuilder gte(Object from) {
        this.from = from;
        this.includeLower = true;
        return this;
    }

    /**
     * The from part of the filter query. Null indicates unbounded.
     */
    public NumericRangeFilterBuilder gte(int from) {
        this.from = from;
        this.includeLower = true;
        return this;
    }

    /**
     * The from part of the filter query. Null indicates unbounded.
     */
    public NumericRangeFilterBuilder gte(long from) {
        this.from = from;
        this.includeLower = true;
        return this;
    }

    /**
     * The from part of the filter query. Null indicates unbounded.
     */
    public NumericRangeFilterBuilder gte(float from) {
        this.from = from;
        this.includeLower = true;
        return this;
    }

    /**
     * The from part of the filter query. Null indicates unbounded.
     */
    public NumericRangeFilterBuilder gte(double from) {
        this.from = from;
        this.includeLower = true;
        return this;
    }

    /**
     * The to part of the filter query. Null indicates unbounded.
     */
    public NumericRangeFilterBuilder to(Object to) {
        this.to = to;
        return this;
    }

    /**
     * The to part of the filter query. Null indicates unbounded.
     */
    public NumericRangeFilterBuilder to(int to) {
        this.to = to;
        return this;
    }

    /**
     * The to part of the filter query. Null indicates unbounded.
     */
    public NumericRangeFilterBuilder to(long to) {
        this.to = to;
        return this;
    }

    /**
     * The to part of the filter query. Null indicates unbounded.
     */
    public NumericRangeFilterBuilder to(float to) {
        this.to = to;
        return this;
    }

    /**
     * The to part of the filter query. Null indicates unbounded.
     */
    public NumericRangeFilterBuilder to(double to) {
        this.to = to;
        return this;
    }

    /**
     * The to part of the filter query. Null indicates unbounded.
     */
    public NumericRangeFilterBuilder lt(Object to) {
        this.to = to;
        this.includeUpper = false;
        return this;
    }

    /**
     * The to part of the filter query. Null indicates unbounded.
     */
    public NumericRangeFilterBuilder lt(int to) {
        this.to = to;
        this.includeUpper = false;
        return this;
    }

    /**
     * The to part of the filter query. Null indicates unbounded.
     */
    public NumericRangeFilterBuilder lt(long to) {
        this.to = to;
        this.includeUpper = false;
        return this;
    }

    /**
     * The to part of the filter query. Null indicates unbounded.
     */
    public NumericRangeFilterBuilder lt(float to) {
        this.to = to;
        this.includeUpper = false;
        return this;
    }

    /**
     * The to part of the filter query. Null indicates unbounded.
     */
    public NumericRangeFilterBuilder lt(double to) {
        this.to = to;
        this.includeUpper = false;
        return this;
    }

    /**
     * The to part of the filter query. Null indicates unbounded.
     */
    public NumericRangeFilterBuilder lte(Object to) {
        this.to = to;
        this.includeUpper = true;
        return this;
    }

    /**
     * The to part of the filter query. Null indicates unbounded.
     */
    public NumericRangeFilterBuilder lte(int to) {
        this.to = to;
        this.includeUpper = true;
        return this;
    }

    /**
     * The to part of the filter query. Null indicates unbounded.
     */
    public NumericRangeFilterBuilder lte(long to) {
        this.to = to;
        this.includeUpper = true;
        return this;
    }

    /**
     * The to part of the filter query. Null indicates unbounded.
     */
    public NumericRangeFilterBuilder lte(float to) {
        this.to = to;
        this.includeUpper = true;
        return this;
    }

    /**
     * The to part of the filter query. Null indicates unbounded.
     */
    public NumericRangeFilterBuilder lte(double to) {
        this.to = to;
        this.includeUpper = true;
        return this;
    }

    /**
     * Should the lower bound be included or not. Defaults to <tt>true</tt>.
     */
    public NumericRangeFilterBuilder includeLower(boolean includeLower) {
        this.includeLower = includeLower;
        return this;
    }

    /**
     * Should the upper bound be included or not. Defaults to <tt>true</tt>.
     */
    public NumericRangeFilterBuilder includeUpper(boolean includeUpper) {
        this.includeUpper = includeUpper;
        return this;
    }

    /**
     * Sets the filter name for the filter that can be used when searching for matched_filters per hit.
     */
    public NumericRangeFilterBuilder filterName(String filterName) {
        this.filterName = filterName;
        return this;
    }

    /**
     * Should the filter be cached or not. Defaults to <tt>false</tt>.
     */
    public NumericRangeFilterBuilder cache(boolean cache) {
        this.cache = cache;
        return this;
    }

    public NumericRangeFilterBuilder cacheKey(String cacheKey) {
        this.cacheKey = cacheKey;
        return this;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NumericRangeFilterParser.NAME);

        builder.startObject(name);
        builder.field("from", from);
        builder.field("to", to);
        builder.field("include_lower", includeLower);
        builder.field("include_upper", includeUpper);
        builder.endObject();

        if (filterName != null) {
            builder.field("_name", filterName);
        }
        if (cache != null) {
            builder.field("_cache", cache);
        }
        if (cacheKey != null) {
            builder.field("_cache_key", cacheKey);
        }

        builder.endObject();
    }
}