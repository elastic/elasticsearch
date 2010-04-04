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
 * A Query that matches documents within an range of terms.
 *
 * @author kimchy (shay.banon)
 */
public class RangeJsonQueryBuilder extends BaseJsonQueryBuilder {

    private final String name;

    private Object from;

    private Object to;

    private boolean includeLower = true;

    private boolean includeUpper = true;

    private float boost = -1;

    /**
     * A Query that matches documents within an range of terms.
     *
     * @param name The field name
     */
    public RangeJsonQueryBuilder(String name) {
        this.name = name;
    }

    /**
     * The from part of the range query. Null indicates unbounded.
     */
    public RangeJsonQueryBuilder from(Object from) {
        this.from = from;
        return this;
    }

    /**
     * The from part of the range query. Null indicates unbounded.
     */
    public RangeJsonQueryBuilder from(String from) {
        this.from = from;
        return this;
    }

    /**
     * The from part of the range query. Null indicates unbounded.
     */
    public RangeJsonQueryBuilder from(int from) {
        this.from = from;
        return this;
    }

    /**
     * The from part of the range query. Null indicates unbounded.
     */
    public RangeJsonQueryBuilder from(long from) {
        this.from = from;
        return this;
    }

    /**
     * The from part of the range query. Null indicates unbounded.
     */
    public RangeJsonQueryBuilder from(float from) {
        this.from = from;
        return this;
    }

    /**
     * The from part of the range query. Null indicates unbounded.
     */
    public RangeJsonQueryBuilder from(double from) {
        this.from = from;
        return this;
    }

    /**
     * The from part of the range query. Null indicates unbounded.
     */
    public RangeJsonQueryBuilder gt(String from) {
        this.from = from;
        this.includeLower = false;
        return this;
    }

    /**
     * The from part of the range query. Null indicates unbounded.
     */
    public RangeJsonQueryBuilder gt(Object from) {
        this.from = from;
        this.includeLower = false;
        return this;
    }

    /**
     * The from part of the range query. Null indicates unbounded.
     */
    public RangeJsonQueryBuilder gt(int from) {
        this.from = from;
        this.includeLower = false;
        return this;
    }

    /**
     * The from part of the range query. Null indicates unbounded.
     */
    public RangeJsonQueryBuilder gt(long from) {
        this.from = from;
        this.includeLower = false;
        return this;
    }

    /**
     * The from part of the range query. Null indicates unbounded.
     */
    public RangeJsonQueryBuilder gt(float from) {
        this.from = from;
        this.includeLower = false;
        return this;
    }

    /**
     * The from part of the range query. Null indicates unbounded.
     */
    public RangeJsonQueryBuilder gt(double from) {
        this.from = from;
        this.includeLower = false;
        return this;
    }

    /**
     * The from part of the range query. Null indicates unbounded.
     */
    public RangeJsonQueryBuilder gte(String from) {
        this.from = from;
        this.includeLower = true;
        return this;
    }

    /**
     * The from part of the range query. Null indicates unbounded.
     */
    public RangeJsonQueryBuilder gte(Object from) {
        this.from = from;
        this.includeLower = true;
        return this;
    }

    /**
     * The from part of the range query. Null indicates unbounded.
     */
    public RangeJsonQueryBuilder gte(int from) {
        this.from = from;
        this.includeLower = true;
        return this;
    }

    /**
     * The from part of the range query. Null indicates unbounded.
     */
    public RangeJsonQueryBuilder gte(long from) {
        this.from = from;
        this.includeLower = true;
        return this;
    }

    /**
     * The from part of the range query. Null indicates unbounded.
     */
    public RangeJsonQueryBuilder gte(float from) {
        this.from = from;
        this.includeLower = true;
        return this;
    }

    /**
     * The from part of the range query. Null indicates unbounded.
     */
    public RangeJsonQueryBuilder gte(double from) {
        this.from = from;
        this.includeLower = true;
        return this;
    }

    /**
     * The to part of the range query. Null indicates unbounded.
     */
    public RangeJsonQueryBuilder to(Object to) {
        this.to = to;
        return this;
    }

    /**
     * The to part of the range query. Null indicates unbounded.
     */
    public RangeJsonQueryBuilder to(String to) {
        this.to = to;
        return this;
    }

    /**
     * The to part of the range query. Null indicates unbounded.
     */
    public RangeJsonQueryBuilder to(int to) {
        this.to = to;
        return this;
    }

    /**
     * The to part of the range query. Null indicates unbounded.
     */
    public RangeJsonQueryBuilder to(long to) {
        this.to = to;
        return this;
    }

    /**
     * The to part of the range query. Null indicates unbounded.
     */
    public RangeJsonQueryBuilder to(float to) {
        this.to = to;
        return this;
    }

    /**
     * The to part of the range query. Null indicates unbounded.
     */
    public RangeJsonQueryBuilder to(double to) {
        this.to = to;
        return this;
    }

    /**
     * The to part of the range query. Null indicates unbounded.
     */
    public RangeJsonQueryBuilder lt(String to) {
        this.to = to;
        this.includeUpper = false;
        return this;
    }

    /**
     * The to part of the range query. Null indicates unbounded.
     */
    public RangeJsonQueryBuilder lt(Object to) {
        this.to = to;
        this.includeUpper = false;
        return this;
    }

    /**
     * The to part of the range query. Null indicates unbounded.
     */
    public RangeJsonQueryBuilder lt(int to) {
        this.to = to;
        this.includeUpper = false;
        return this;
    }

    /**
     * The to part of the range query. Null indicates unbounded.
     */
    public RangeJsonQueryBuilder lt(long to) {
        this.to = to;
        this.includeUpper = false;
        return this;
    }

    /**
     * The to part of the range query. Null indicates unbounded.
     */
    public RangeJsonQueryBuilder lt(float to) {
        this.to = to;
        this.includeUpper = false;
        return this;
    }

    /**
     * The to part of the range query. Null indicates unbounded.
     */
    public RangeJsonQueryBuilder lt(double to) {
        this.to = to;
        this.includeUpper = false;
        return this;
    }

    /**
     * The to part of the range query. Null indicates unbounded.
     */
    public RangeJsonQueryBuilder lte(String to) {
        this.to = to;
        this.includeUpper = true;
        return this;
    }

    /**
     * The to part of the range query. Null indicates unbounded.
     */
    public RangeJsonQueryBuilder lte(Object to) {
        this.to = to;
        this.includeUpper = true;
        return this;
    }

    /**
     * The to part of the range query. Null indicates unbounded.
     */
    public RangeJsonQueryBuilder lte(int to) {
        this.to = to;
        this.includeUpper = true;
        return this;
    }

    /**
     * The to part of the range query. Null indicates unbounded.
     */
    public RangeJsonQueryBuilder lte(long to) {
        this.to = to;
        this.includeUpper = true;
        return this;
    }

    /**
     * The to part of the range query. Null indicates unbounded.
     */
    public RangeJsonQueryBuilder lte(float to) {
        this.to = to;
        this.includeUpper = true;
        return this;
    }

    /**
     * The to part of the range query. Null indicates unbounded.
     */
    public RangeJsonQueryBuilder lte(double to) {
        this.to = to;
        this.includeUpper = true;
        return this;
    }

    /**
     * Should the lower bound be included or not. Defaults to <tt>true</tt>.
     */
    public RangeJsonQueryBuilder includeLower(boolean includeLower) {
        this.includeLower = includeLower;
        return this;
    }

    /**
     * Should the upper bound be included or not. Defaults to <tt>true</tt>.
     */
    public RangeJsonQueryBuilder includeUpper(boolean includeUpper) {
        this.includeUpper = includeUpper;
        return this;
    }

    /**
     * Sets the boost for this query.  Documents matching this query will (in addition to the normal
     * weightings) have their score multiplied by the boost provided.
     */
    public RangeJsonQueryBuilder boost(float boost) {
        this.boost = boost;
        return this;
    }

    @Override protected void doJson(JsonBuilder builder, Params params) throws IOException {
        builder.startObject(RangeJsonQueryParser.NAME);
        builder.startObject(name);
        builder.field("from", from);
        builder.field("to", to);
        builder.field("include_lower", includeLower);
        builder.field("include_upper", includeUpper);
        if (boost != -1) {
            builder.field("boost", boost);
        }
        builder.endObject();
        builder.endObject();
    }
}
