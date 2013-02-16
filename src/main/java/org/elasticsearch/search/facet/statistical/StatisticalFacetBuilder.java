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

package org.elasticsearch.search.facet.statistical;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilderException;
import org.elasticsearch.search.facet.FacetBuilder;

import java.io.IOException;

/**
 *
 */
public class StatisticalFacetBuilder extends FacetBuilder {
    private String[] fieldsNames;
    private String fieldName;

    public StatisticalFacetBuilder(String name) {
        super(name);
    }

    public StatisticalFacetBuilder field(String field) {
        this.fieldName = field;
        return this;
    }

    /**
     * The fields the terms will be collected from.
     */
    public StatisticalFacetBuilder fields(String... fields) {
        this.fieldsNames = fields;
        return this;
    }

    /**
     * Marks the facet to run in a global scope, not bounded by any query.
     */
    public StatisticalFacetBuilder global(boolean global) {
        super.global(global);
        return this;
    }

    public StatisticalFacetBuilder facetFilter(FilterBuilder filter) {
        this.facetFilter = filter;
        return this;
    }

    /**
     * Sets the nested path the facet will execute on. A match (root object) will then cause all the
     * nested objects matching the path to be computed into the facet.
     */
    public StatisticalFacetBuilder nested(String nested) {
        this.nested = nested;
        return this;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (fieldName == null && fieldsNames == null) {
            throw new SearchSourceBuilderException("field must be set on statistical facet for facet [" + name + "]");
        }
        builder.startObject(name);

        builder.startObject(StatisticalFacet.TYPE);
        if (fieldsNames != null) {
            if (fieldsNames.length == 1) {
                builder.field("field", fieldsNames[0]);
            } else {
                builder.field("fields", fieldsNames);
            }
        } else {
            builder.field("field", fieldName);
        }
        builder.endObject();

        addFilterFacetAndGlobal(builder, params);

        builder.endObject();
        return builder;
    }
}
