/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
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

package org.elasticsearch.search.facet.range;

import com.google.common.collect.Lists;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilderException;
import org.elasticsearch.search.facet.FacetBuilder;

import java.io.IOException;
import java.util.List;

/**
 * A facet builder of range facets.
 */
public class RangeFacetBuilder extends FacetBuilder {

    private String keyFieldName;
    private String valueFieldName;

    private List<Entry> entries = Lists.newArrayList();

    /**
     * Constructs a new range facet with the provided facet logical name.
     *
     * @param name The logical name of the facet
     */
    public RangeFacetBuilder(String name) {
        super(name);
    }

    /**
     * The field name to perform the range facet. Translates to perform the range facet
     * using the provided field as both the {@link #keyField(String)} and {@link #valueField(String)}.
     */
    public RangeFacetBuilder field(String field) {
        this.keyFieldName = field;
        this.valueFieldName = field;
        return this;
    }

    /**
     * The field name to use in order to control where the hit will "fall into" within the range
     * entries. Essentially, using the key field numeric value, the hit will be "rounded" into the relevant
     * bucket controlled by the interval.
     */
    public RangeFacetBuilder keyField(String keyField) {
        this.keyFieldName = keyField;
        return this;
    }

    /**
     * The field name to use as the value of the hit to compute data based on values within the interval
     * (for example, total).
     */
    public RangeFacetBuilder valueField(String valueField) {
        this.valueFieldName = valueField;
        return this;
    }

    /**
     * Adds a range entry with explicit from and to.
     *
     * @param from The from range limit
     * @param to   The to range limit
     */
    public RangeFacetBuilder addRange(double from, double to) {
        entries.add(new Entry(from, to));
        return this;
    }

    public RangeFacetBuilder addRange(String from, String to) {
        entries.add(new Entry(from, to));
        return this;
    }

    /**
     * Adds a range entry with explicit from and unbounded to.
     *
     * @param from the from range limit, to is unbounded.
     */
    public RangeFacetBuilder addUnboundedTo(double from) {
        entries.add(new Entry(from, Double.POSITIVE_INFINITY));
        return this;
    }

    public RangeFacetBuilder addUnboundedTo(String from) {
        entries.add(new Entry(from, null));
        return this;
    }

    /**
     * Adds a range entry with explicit to and unbounded from.
     *
     * @param to the to range limit, from is unbounded.
     */
    public RangeFacetBuilder addUnboundedFrom(double to) {
        entries.add(new Entry(Double.NEGATIVE_INFINITY, to));
        return this;
    }

    public RangeFacetBuilder addUnboundedFrom(String to) {
        entries.add(new Entry(null, to));
        return this;
    }

    /**
     * Should the facet run in global mode (not bounded by the search query) or not (bounded by
     * the search query). Defaults to <tt>false</tt>.
     */
    public RangeFacetBuilder global(boolean global) {
        super.global(global);
        return this;
    }

    /**
     * An additional filter used to further filter down the set of documents the facet will run on.
     */
    public RangeFacetBuilder facetFilter(FilterBuilder filter) {
        this.facetFilter = filter;
        return this;
    }

    /**
     * Sets the nested path the facet will execute on. A match (root object) will then cause all the
     * nested objects matching the path to be computed into the facet.
     */
    public RangeFacetBuilder nested(String nested) {
        this.nested = nested;
        return this;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (keyFieldName == null) {
            throw new SearchSourceBuilderException("field must be set on range facet for facet [" + name + "]");
        }

        if (entries.isEmpty()) {
            throw new SearchSourceBuilderException("at least one range must be defined for range facet [" + name + "]");
        }

        builder.startObject(name);

        builder.startObject(RangeFacet.TYPE);
        if (valueFieldName != null && !keyFieldName.equals(valueFieldName)) {
            builder.field("key_field", keyFieldName);
            builder.field("value_field", valueFieldName);
        } else {
            builder.field("field", keyFieldName);
        }

        builder.startArray("ranges");
        for (Entry entry : entries) {
            builder.startObject();
            if (entry.fromAsString != null) {
                builder.field("from", entry.fromAsString);
            } else if (!Double.isInfinite(entry.from)) {
                builder.field("from", entry.from);
            }
            if (entry.toAsString != null) {
                builder.field("to", entry.toAsString);
            } else if (!Double.isInfinite(entry.to)) {
                builder.field("to", entry.to);
            }
            builder.endObject();
        }
        builder.endArray();

        builder.endObject();

        addFilterFacetAndGlobal(builder, params);

        builder.endObject();
        return builder;
    }

    static class Entry {
        double from = Double.NEGATIVE_INFINITY;
        double to = Double.POSITIVE_INFINITY;

        String fromAsString;
        String toAsString;

        Entry(String fromAsString, String toAsString) {
            this.fromAsString = fromAsString;
            this.toAsString = toAsString;
        }

        Entry(double from, double to) {
            this.from = from;
            this.to = to;
        }
    }
}
