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

package org.elasticsearch.search.facets.histogram;

import org.elasticsearch.common.xcontent.builder.XContentBuilder;
import org.elasticsearch.index.query.xcontent.XContentFilterBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilderException;
import org.elasticsearch.search.facets.AbstractFacetBuilder;

import java.io.IOException;

/**
 * @author kimchy (shay.banon)
 */
public class HistogramFacetBuilder extends AbstractFacetBuilder {
    private String keyFieldName;
    private String valueFieldName;
    private long interval = -1;
    private HistogramFacet.ComparatorType comparatorType;

    public HistogramFacetBuilder(String name) {
        super(name);
    }

    public HistogramFacetBuilder field(String field) {
        this.keyFieldName = field;
        this.valueFieldName = field;
        return this;
    }

    public HistogramFacetBuilder keyField(String keyField) {
        this.keyFieldName = keyField;
        return this;
    }

    public HistogramFacetBuilder valueField(String valueField) {
        this.valueFieldName = valueField;
        return this;
    }

    public HistogramFacetBuilder interval(long interval) {
        this.interval = interval;
        return this;
    }

    public HistogramFacetBuilder comparator(HistogramFacet.ComparatorType comparatorType) {
        this.comparatorType = comparatorType;
        return this;
    }

    public HistogramFacetBuilder global(boolean global) {
        this.global = global;
        return this;
    }

    public HistogramFacetBuilder filter(XContentFilterBuilder filter) {
        this.filter = filter;
        return this;
    }

    @Override public void toXContent(XContentBuilder builder, Params params) throws IOException {
        if (keyFieldName == null) {
            throw new SearchSourceBuilderException("field must be set on histogram facet for facet [" + name + "]");
        }
        if (interval < 0) {
            throw new SearchSourceBuilderException("interval must be set on histogram facet for facet [" + name + "]");
        }
        builder.startObject(name);

        builder.startObject(HistogramFacetCollectorParser.NAME);
        if (valueFieldName != null && !keyFieldName.equals(valueFieldName)) {
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

        if (filter != null) {
            builder.field("filter");
            filter.toXContent(builder, params);
        }

        if (global != null) {
            builder.field("global", global);
        }

        builder.endObject();
    }
}
