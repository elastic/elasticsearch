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

package org.elasticsearch.search.facet.histogram;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilderException;
import org.elasticsearch.search.facet.FacetBuilder;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * A facet builder of histogram facets.
 */
public class HistogramFacetBuilder extends FacetBuilder {
    private String keyFieldName;
    private String valueFieldName;
    private long interval = -1;
    private HistogramFacet.ComparatorType comparatorType;

    /**
     * Constructs a new histogram facet with the provided facet logical name.
     *
     * @param name The logical name of the facet
     */
    public HistogramFacetBuilder(String name) {
        super(name);
    }

    /**
     * The field name to perform the histogram facet. Translates to perform the histogram facet
     * using the provided field as both the {@link #keyField(String)} and {@link #valueField(String)}.
     */
    public HistogramFacetBuilder field(String field) {
        this.keyFieldName = field;
        return this;
    }

    /**
     * The field name to use in order to control where the hit will "fall into" within the histogram
     * entries. Essentially, using the key field numeric value, the hit will be "rounded" into the relevant
     * bucket controlled by the interval.
     */
    public HistogramFacetBuilder keyField(String keyField) {
        this.keyFieldName = keyField;
        return this;
    }

    /**
     * The field name to use as the value of the hit to compute data based on values within the interval
     * (for example, total).
     */
    public HistogramFacetBuilder valueField(String valueField) {
        this.valueFieldName = valueField;
        return this;
    }

    /**
     * The interval used to control the bucket "size" where each key value of a hit will fall into.
     */
    public HistogramFacetBuilder interval(long interval) {
        this.interval = interval;
        return this;
    }

    /**
     * The interval used to control the bucket "size" where each key value of a hit will fall into.
     */
    public HistogramFacetBuilder interval(long interval, TimeUnit unit) {
        return interval(unit.toMillis(interval));
    }

    public HistogramFacetBuilder comparator(HistogramFacet.ComparatorType comparatorType) {
        this.comparatorType = comparatorType;
        return this;
    }

    /**
     * Should the facet run in global mode (not bounded by the search query) or not (bounded by
     * the search query). Defaults to <tt>false</tt>.
     */
    public HistogramFacetBuilder global(boolean global) {
        super.global(global);
        return this;
    }

    /**
     * An additional filter used to further filter down the set of documents the facet will run on.
     */
    public HistogramFacetBuilder facetFilter(FilterBuilder filter) {
        this.facetFilter = filter;
        return this;
    }

    /**
     * Sets the nested path the facet will execute on. A match (root object) will then cause all the
     * nested objects matching the path to be computed into the facet.
     */
    public HistogramFacetBuilder nested(String nested) {
        this.nested = nested;
        return this;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (keyFieldName == null) {
            throw new SearchSourceBuilderException("field must be set on histogram facet for facet [" + name + "]");
        }
        if (interval < 0) {
            throw new SearchSourceBuilderException("interval must be set on histogram facet for facet [" + name + "]");
        }
        builder.startObject(name);

        builder.startObject(HistogramFacet.TYPE);
        if (valueFieldName != null) {
            builder.field("key_field", keyFieldName);
            builder.field("value_field", valueFieldName);
        } else {
            builder.field("field", keyFieldName);
        }
        builder.field("interval", interval);

        if (comparatorType != null) {
            builder.field("comparator", comparatorType.description());
        }
        builder.endObject();

        addFilterFacetAndGlobal(builder, params);

        builder.endObject();
        return builder;
    }
}
