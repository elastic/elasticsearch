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

package org.elasticsearch.index.query.json;

import org.elasticsearch.util.json.JsonBuilder;

import java.io.IOException;

/**
 * A filter that restricts search results to values that are within the given range.
 *
 * @author kimchy (shay.banon)
 */
public class RangeJsonFilterBuilder extends BaseJsonFilterBuilder {

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
    public RangeJsonFilterBuilder(String name) {
        this.name = name;
    }

    /**
     * The from part of the filter query. Null indicates unbounded.
     */
    public RangeJsonFilterBuilder from(String from) {
        this.from = from;
        return this;
    }

    /**
     * The from part of the filter query. Null indicates unbounded.
     */
    public RangeJsonFilterBuilder from(int from) {
        this.from = from;
        return this;
    }

    /**
     * The from part of the filter query. Null indicates unbounded.
     */
    public RangeJsonFilterBuilder from(long from) {
        this.from = from;
        return this;
    }

    /**
     * The from part of the filter query. Null indicates unbounded.
     */
    public RangeJsonFilterBuilder from(float from) {
        this.from = from;
        return this;
    }

    /**
     * The from part of the filter query. Null indicates unbounded.
     */
    public RangeJsonFilterBuilder from(double from) {
        this.from = from;
        return this;
    }

    /**
     * The from part of the filter query. Null indicates unbounded.
     */
    public RangeJsonFilterBuilder gt(String from) {
        this.from = from;
        this.includeLower = false;
        return this;
    }

    /**
     * The from part of the filter query. Null indicates unbounded.
     */
    public RangeJsonFilterBuilder gt(int from) {
        this.from = from;
        this.includeLower = false;
        return this;
    }

    /**
     * The from part of the filter query. Null indicates unbounded.
     */
    public RangeJsonFilterBuilder gt(long from) {
        this.from = from;
        this.includeLower = false;
        return this;
    }

    /**
     * The from part of the filter query. Null indicates unbounded.
     */
    public RangeJsonFilterBuilder gt(float from) {
        this.from = from;
        this.includeLower = false;
        return this;
    }

    /**
     * The from part of the filter query. Null indicates unbounded.
     */
    public RangeJsonFilterBuilder gt(double from) {
        this.from = from;
        this.includeLower = false;
        return this;
    }

    /**
     * The from part of the filter query. Null indicates unbounded.
     */
    public RangeJsonFilterBuilder gte(String from) {
        this.from = from;
        this.includeLower = true;
        return this;
    }

    /**
     * The from part of the filter query. Null indicates unbounded.
     */
    public RangeJsonFilterBuilder gte(int from) {
        this.from = from;
        this.includeLower = true;
        return this;
    }

    /**
     * The from part of the filter query. Null indicates unbounded.
     */
    public RangeJsonFilterBuilder gte(long from) {
        this.from = from;
        this.includeLower = true;
        return this;
    }

    /**
     * The from part of the filter query. Null indicates unbounded.
     */
    public RangeJsonFilterBuilder gte(float from) {
        this.from = from;
        this.includeLower = true;
        return this;
    }

    /**
     * The from part of the filter query. Null indicates unbounded.
     */
    public RangeJsonFilterBuilder gte(double from) {
        this.from = from;
        this.includeLower = true;
        return this;
    }

    /**
     * The to part of the filter query. Null indicates unbounded.
     */
    public RangeJsonFilterBuilder to(String to) {
        this.to = to;
        return this;
    }

    /**
     * The to part of the filter query. Null indicates unbounded.
     */
    public RangeJsonFilterBuilder to(int to) {
        this.to = to;
        return this;
    }

    /**
     * The to part of the filter query. Null indicates unbounded.
     */
    public RangeJsonFilterBuilder to(long to) {
        this.to = to;
        return this;
    }

    /**
     * The to part of the filter query. Null indicates unbounded.
     */
    public RangeJsonFilterBuilder to(float to) {
        this.to = to;
        return this;
    }

    /**
     * The to part of the filter query. Null indicates unbounded.
     */
    public RangeJsonFilterBuilder to(double to) {
        this.to = to;
        return this;
    }

    /**
     * The to part of the filter query. Null indicates unbounded.
     */
    public RangeJsonFilterBuilder lt(String to) {
        this.to = to;
        this.includeUpper = false;
        return this;
    }

    /**
     * The to part of the filter query. Null indicates unbounded.
     */
    public RangeJsonFilterBuilder lt(int to) {
        this.to = to;
        this.includeUpper = false;
        return this;
    }

    /**
     * The to part of the filter query. Null indicates unbounded.
     */
    public RangeJsonFilterBuilder lt(long to) {
        this.to = to;
        this.includeUpper = false;
        return this;
    }

    /**
     * The to part of the filter query. Null indicates unbounded.
     */
    public RangeJsonFilterBuilder lt(float to) {
        this.to = to;
        this.includeUpper = false;
        return this;
    }

    /**
     * The to part of the filter query. Null indicates unbounded.
     */
    public RangeJsonFilterBuilder lt(double to) {
        this.to = to;
        this.includeUpper = false;
        return this;
    }

    /**
     * The to part of the filter query. Null indicates unbounded.
     */
    public RangeJsonFilterBuilder lte(String to) {
        this.to = to;
        this.includeUpper = true;
        return this;
    }

    /**
     * The to part of the filter query. Null indicates unbounded.
     */
    public RangeJsonFilterBuilder lte(int to) {
        this.to = to;
        this.includeUpper = true;
        return this;
    }

    /**
     * The to part of the filter query. Null indicates unbounded.
     */
    public RangeJsonFilterBuilder lte(long to) {
        this.to = to;
        this.includeUpper = true;
        return this;
    }

    /**
     * The to part of the filter query. Null indicates unbounded.
     */
    public RangeJsonFilterBuilder lte(float to) {
        this.to = to;
        this.includeUpper = true;
        return this;
    }

    /**
     * The to part of the filter query. Null indicates unbounded.
     */
    public RangeJsonFilterBuilder lte(double to) {
        this.to = to;
        this.includeUpper = true;
        return this;
    }

    /**
     * Should the lower bound be included or not. Defaults to <tt>true</tt>.
     */
    public RangeJsonFilterBuilder includeLower(boolean includeLower) {
        this.includeLower = includeLower;
        return this;
    }

    /**
     * Should the upper bound be included or not. Defaults to <tt>true</tt>.
     */
    public RangeJsonFilterBuilder includeUpper(boolean includeUpper) {
        this.includeUpper = includeUpper;
        return this;
    }

    @Override protected void doJson(JsonBuilder builder, Params params) throws IOException {
        builder.startObject(RangeJsonFilterParser.NAME);
        builder.startObject(name);
        builder.field("from", from);
        builder.field("to", to);
        builder.field("include_lower", includeLower);
        builder.field("include_upper", includeUpper);
        builder.endObject();
        builder.endObject();
    }
}