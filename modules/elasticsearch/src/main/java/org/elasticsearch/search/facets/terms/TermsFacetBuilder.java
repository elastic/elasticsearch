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

package org.elasticsearch.search.facets.terms;

import org.elasticsearch.common.collect.Maps;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.xcontent.XContentFilterBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilderException;
import org.elasticsearch.search.facets.AbstractFacetBuilder;

import java.io.IOException;
import java.util.Map;

/**
 * @author kimchy (shay.banon)
 */
public class TermsFacetBuilder extends AbstractFacetBuilder {
    private String fieldName;
    private int size = 10;
    private String[] exclude;
    private String regex;
    private int regexFlags = 0;
    private TermsFacet.ComparatorType comparatorType;
    private String script;
    private Map<String, Object> params;

    public TermsFacetBuilder(String name) {
        super(name);
    }

    public TermsFacetBuilder global(boolean global) {
        this.global = global;
        return this;
    }

    public TermsFacetBuilder facetFilter(XContentFilterBuilder filter) {
        this.facetFilter = filter;
        return this;
    }

    public TermsFacetBuilder field(String field) {
        this.fieldName = field;
        return this;
    }

    public TermsFacetBuilder exclude(String... exclude) {
        this.exclude = exclude;
        return this;
    }

    public TermsFacetBuilder size(int size) {
        this.size = size;
        return this;
    }

    public TermsFacetBuilder regex(String regex) {
        return regex(regex, 0);
    }

    public TermsFacetBuilder regex(String regex, int flags) {
        this.regex = regex;
        this.regexFlags = flags;
        return this;
    }

    public TermsFacetBuilder order(TermsFacet.ComparatorType comparatorType) {
        this.comparatorType = comparatorType;
        return this;
    }

    public TermsFacetBuilder script(String script) {
        this.script = script;
        return this;
    }

    public TermsFacetBuilder param(String name, Object value) {
        if (params == null) {
            params = Maps.newHashMap();
        }
        params.put(name, value);
        return this;
    }

    @Override public void toXContent(XContentBuilder builder, Params params) throws IOException {
        if (fieldName == null) {
            throw new SearchSourceBuilderException("field must be set on terms facet for facet [" + name + "]");
        }
        builder.startObject(name);

        builder.startObject(TermsFacetCollectorParser.NAME);
        builder.field("field", fieldName);
        builder.field("size", size);
        if (exclude != null) {
            builder.startArray("exclude");
            for (String ex : exclude) {
                builder.value(ex);
            }
            builder.endArray();
        }
        if (regex != null) {
            builder.field("regex", regex);
            if (regexFlags != 0) {
                builder.field("regex_flags", Regex.flagsToString(regexFlags));
            }
        }
        if (comparatorType != null) {
            builder.field("order", comparatorType.name().toLowerCase());
        }

        if (script != null) {
            builder.field("script", script);
            if (this.params != null) {
                builder.field("params", this.params);
            }
        }

        builder.endObject();

        addFilterFacetAndGlobal(builder, params);

        builder.endObject();
    }
}
