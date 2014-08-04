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
 * A filter that restricts search results to values that are within the given range.
 *
 *
 */
public class RangeFilterBuilder extends BaseFilterBuilder {

    private final String name;

    private Object from;

    private Object to;
    private String timeZone;

    private boolean includeLower = true;

    private boolean includeUpper = true;

    private Boolean cache;
    private String cacheKey;

    private String filterName;

    private String execution;

    /**
     * A filter that restricts search results to values that are within the given range.
     *
     * @param name The field name
     */
    public RangeFilterBuilder(String name) {
        this.name = name;
    }

    /**
     * The from part of the filter query. Null indicates unbounded.
     */
    public RangeFilterBuilder from(Object from) {
        this.from = from;
        return this;
    }

    /**
     * The from part of the filter query. Null indicates unbounded.
     */
    public RangeFilterBuilder from(int from) {
        this.from = from;
        return this;
    }

    /**
     * The from part of the filter query. Null indicates unbounded.
     */
    public RangeFilterBuilder from(long from) {
        this.from = from;
        return this;
    }

    /**
     * The from part of the filter query. Null indicates unbounded.
     */
    public RangeFilterBuilder from(float from) {
        this.from = from;
        return this;
    }

    /**
     * The from part of the filter query. Null indicates unbounded.
     */
    public RangeFilterBuilder from(double from) {
        this.from = from;
        return this;
    }

    /**
     * The from part of the filter query. Null indicates unbounded.
     */
    public RangeFilterBuilder gt(Object from) {
        this.from = from;
        this.includeLower = false;
        return this;
    }

    /**
     * The from part of the filter query. Null indicates unbounded.
     */
    public RangeFilterBuilder gt(int from) {
        this.from = from;
        this.includeLower = false;
        return this;
    }

    /**
     * The from part of the filter query. Null indicates unbounded.
     */
    public RangeFilterBuilder gt(long from) {
        this.from = from;
        this.includeLower = false;
        return this;
    }

    /**
     * The from part of the filter query. Null indicates unbounded.
     */
    public RangeFilterBuilder gt(float from) {
        this.from = from;
        this.includeLower = false;
        return this;
    }

    /**
     * The from part of the filter query. Null indicates unbounded.
     */
    public RangeFilterBuilder gt(double from) {
        this.from = from;
        this.includeLower = false;
        return this;
    }

    /**
     * The from part of the filter query. Null indicates unbounded.
     */
    public RangeFilterBuilder gte(Object from) {
        this.from = from;
        this.includeLower = true;
        return this;
    }

    /**
     * The from part of the filter query. Null indicates unbounded.
     */
    public RangeFilterBuilder gte(int from) {
        this.from = from;
        this.includeLower = true;
        return this;
    }

    /**
     * The from part of the filter query. Null indicates unbounded.
     */
    public RangeFilterBuilder gte(long from) {
        this.from = from;
        this.includeLower = true;
        return this;
    }

    /**
     * The from part of the filter query. Null indicates unbounded.
     */
    public RangeFilterBuilder gte(float from) {
        this.from = from;
        this.includeLower = true;
        return this;
    }

    /**
     * The from part of the filter query. Null indicates unbounded.
     */
    public RangeFilterBuilder gte(double from) {
        this.from = from;
        this.includeLower = true;
        return this;
    }

    /**
     * The to part of the filter query. Null indicates unbounded.
     */
    public RangeFilterBuilder to(Object to) {
        this.to = to;
        return this;
    }

    /**
     * The to part of the filter query. Null indicates unbounded.
     */
    public RangeFilterBuilder to(int to) {
        this.to = to;
        return this;
    }

    /**
     * The to part of the filter query. Null indicates unbounded.
     */
    public RangeFilterBuilder to(long to) {
        this.to = to;
        return this;
    }

    /**
     * The to part of the filter query. Null indicates unbounded.
     */
    public RangeFilterBuilder to(float to) {
        this.to = to;
        return this;
    }

    /**
     * The to part of the filter query. Null indicates unbounded.
     */
    public RangeFilterBuilder to(double to) {
        this.to = to;
        return this;
    }

    /**
     * The to part of the filter query. Null indicates unbounded.
     */
    public RangeFilterBuilder lt(Object to) {
        this.to = to;
        this.includeUpper = false;
        return this;
    }

    /**
     * The to part of the filter query. Null indicates unbounded.
     */
    public RangeFilterBuilder lt(int to) {
        this.to = to;
        this.includeUpper = false;
        return this;
    }

    /**
     * The to part of the filter query. Null indicates unbounded.
     */
    public RangeFilterBuilder lt(long to) {
        this.to = to;
        this.includeUpper = false;
        return this;
    }

    /**
     * The to part of the filter query. Null indicates unbounded.
     */
    public RangeFilterBuilder lt(float to) {
        this.to = to;
        this.includeUpper = false;
        return this;
    }

    /**
     * The to part of the filter query. Null indicates unbounded.
     */
    public RangeFilterBuilder lt(double to) {
        this.to = to;
        this.includeUpper = false;
        return this;
    }

    /**
     * The to part of the filter query. Null indicates unbounded.
     */
    public RangeFilterBuilder lte(int to) {
        this.to = to;
        this.includeUpper = true;
        return this;
    }

    /**
     * The to part of the filter query. Null indicates unbounded.
     */
    public RangeFilterBuilder lte(long to) {
        this.to = to;
        this.includeUpper = true;
        return this;
    }

    /**
     * The to part of the filter query. Null indicates unbounded.
     */
    public RangeFilterBuilder lte(float to) {
        this.to = to;
        this.includeUpper = true;
        return this;
    }

    /**
     * The to part of the filter query. Null indicates unbounded.
     */
    public RangeFilterBuilder lte(double to) {
        this.to = to;
        this.includeUpper = true;
        return this;
    }

    /**
     * The to part of the filter query. Null indicates unbounded.
     */
    public RangeFilterBuilder lte(Object to) {
        this.to = to;
        this.includeUpper = true;
        return this;
    }

    /**
     * Should the lower bound be included or not. Defaults to <tt>true</tt>.
     */
    public RangeFilterBuilder includeLower(boolean includeLower) {
        this.includeLower = includeLower;
        return this;
    }

    /**
     * Should the upper bound be included or not. Defaults to <tt>true</tt>.
     */
    public RangeFilterBuilder includeUpper(boolean includeUpper) {
        this.includeUpper = includeUpper;
        return this;
    }

    /**
     * Sets the filter name for the filter that can be used when searching for matched_filters per hit.
     */
    public RangeFilterBuilder filterName(String filterName) {
        this.filterName = filterName;
        return this;
    }

    /**
     * Should the filter be cached or not. Defaults to <tt>true</tt>.
     */
    public RangeFilterBuilder cache(boolean cache) {
        this.cache = cache;
        return this;
    }

    public RangeFilterBuilder cacheKey(String cacheKey) {
        this.cacheKey = cacheKey;
        return this;
    }

    /**
     * Sets the execution mode that controls how the range filter is executed. Valid values are: "index" and "fielddata".
     * <ol>
     * <li> The <code>index</code> execution uses the field's inverted in order to determine of documents fall with in
     *      the range filter's from and to range.
     * <li> The <code>fielddata</code> execution uses field data in order to determine of documents fall with in the
     *      range filter's from and to range. Since field data is an in memory data structure, you need to have
     *      sufficient memory on your nodes in order to use this execution mode.
     * </ol>
     *
     * In general for small ranges the <code>index</code> execution is faster and for longer ranges the
     * <code>fielddata</code> execution is faster.
     */
    public RangeFilterBuilder setExecution(String execution) {
        this.execution = execution;
        return this;
    }

    /**
     * In case of date field, we can adjust the from/to fields using a timezone
     */
    public RangeFilterBuilder timeZone(String timeZone) {
        this.timeZone = timeZone;
        return this;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(RangeFilterParser.NAME);

        builder.startObject(name);
        builder.field("from", from);
        builder.field("to", to);
        if (timeZone != null) {
            builder.field("time_zone", timeZone);
        }
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
        if (execution != null) {
            builder.field("execution", execution);
        }

        builder.endObject();
    }
}
