/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.index.query.xcontent;

import org.elasticsearch.util.xcontent.builder.XContentBuilder;

import java.io.IOException;

/**
 * A filter that restricts search results to values that are within the given range.
 *
 * @author kimchy (shay.banon)
 */
public class RangeFilterBuilder extends BaseFilterBuilder {

    private final String name;

    private Object from;

    private Object to;

    private boolean includeLower = true;

    private boolean includeUpper = true;

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
    public RangeFilterBuilder from(String from) {
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
    public RangeFilterBuilder gt(String from) {
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
    public RangeFilterBuilder gte(String from) {
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
    public RangeFilterBuilder to(String to) {
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
    public RangeFilterBuilder lt(String to) {
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
    public RangeFilterBuilder lte(String to) {
        this.to = to;
        this.includeUpper = true;
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

    @Override protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(RangeFilterParser.NAME);
        builder.startObject(name);
        builder.field("from", from);
        builder.field("to", to);
        builder.field("include_lower", includeLower);
        builder.field("include_upper", includeUpper);
        builder.endObject();
        builder.endObject();
    }
}