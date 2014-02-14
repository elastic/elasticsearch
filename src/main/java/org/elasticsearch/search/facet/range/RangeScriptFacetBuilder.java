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

package org.elasticsearch.search.facet.range;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilderException;
import org.elasticsearch.search.facet.FacetBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class RangeScriptFacetBuilder extends FacetBuilder {

    private String lang;
    private String keyScript;
    private String valueScript;
    private Map<String, Object> params;
    private List<Entry> entries = Lists.newArrayList();

    public RangeScriptFacetBuilder(String name) {
        super(name);
    }

    /**
     * The language of the script.
     */
    public RangeScriptFacetBuilder lang(String lang) {
        this.lang = lang;
        return this;
    }

    public RangeScriptFacetBuilder keyScript(String keyScript) {
        this.keyScript = keyScript;
        return this;
    }

    public RangeScriptFacetBuilder valueScript(String valueScript) {
        this.valueScript = valueScript;
        return this;
    }

    public RangeScriptFacetBuilder param(String name, Object value) {
        if (params == null) {
            params = Maps.newHashMap();
        }
        params.put(name, value);
        return this;
    }

    /**
     * Adds a range entry with explicit from and to.
     *
     * @param from The from range limit
     * @param to   The to range limit
     */
    public RangeScriptFacetBuilder addRange(double from, double to) {
        entries.add(new Entry(from, to));
        return this;
    }

    /**
     * Adds a range entry with explicit from and unbounded to.
     *
     * @param from the from range limit, to is unbounded.
     */
    public RangeScriptFacetBuilder addUnboundedTo(double from) {
        entries.add(new Entry(from, Double.POSITIVE_INFINITY));
        return this;
    }

    /**
     * Adds a range entry with explicit to and unbounded from.
     *
     * @param to the to range limit, from is unbounded.
     */
    public RangeScriptFacetBuilder addUnboundedFrom(double to) {
        entries.add(new Entry(Double.NEGATIVE_INFINITY, to));
        return this;
    }


    /**
     * Should the facet run in global mode (not bounded by the search query) or not (bounded by
     * the search query). Defaults to <tt>false</tt>.
     */
    public RangeScriptFacetBuilder global(boolean global) {
        super.global(global);
        return this;
    }

    public RangeScriptFacetBuilder facetFilter(FilterBuilder filter) {
        this.facetFilter = filter;
        return this;
    }

    /**
     * Sets the nested path the facet will execute on. A match (root object) will then cause all the
     * nested objects matching the path to be computed into the facet.
     */
    public RangeScriptFacetBuilder nested(String nested) {
        this.nested = nested;
        return this;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (keyScript == null) {
            throw new SearchSourceBuilderException("key_script must be set on range script facet for facet [" + name + "]");
        }
        if (valueScript == null) {
            throw new SearchSourceBuilderException("value_script must be set on range script facet for facet [" + name + "]");
        }

        if (entries.isEmpty()) {
            throw new SearchSourceBuilderException("at least one range must be defined for range facet [" + name + "]");
        }

        builder.startObject(name);

        builder.startObject(RangeFacet.TYPE);
        builder.field("key_script", keyScript);
        builder.field("value_script", valueScript);
        if (lang != null) {
            builder.field("lang", lang);
        }

        builder.startArray("ranges");
        for (Entry entry : entries) {
            builder.startObject();
            if (!Double.isInfinite(entry.from)) {
                builder.field("from", entry.from);
            }
            if (!Double.isInfinite(entry.to)) {
                builder.field("to", entry.to);
            }
            builder.endObject();
        }
        builder.endArray();

        if (this.params != null) {
            builder.field("params", this.params);
        }
        builder.endObject();

        addFilterFacetAndGlobal(builder, params);

        builder.endObject();
        return builder;
    }

    private static class Entry {
        final double from;
        final double to;

        private Entry(double from, double to) {
            this.from = from;
            this.to = to;
        }
    }

}
