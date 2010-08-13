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

import org.elasticsearch.common.collect.Maps;
import org.elasticsearch.common.xcontent.builder.XContentBuilder;
import org.elasticsearch.index.query.xcontent.XContentFilterBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilderException;
import org.elasticsearch.search.facets.AbstractFacetBuilder;

import java.io.IOException;
import java.util.Map;

/**
 * @author kimchy (shay.banon)
 */
public class HistogramScriptFacetBuilder extends AbstractFacetBuilder {
    private String keyScript;
    private String valueScript;
    private Map<String, Object> params;
    private long interval = -1;
    private HistogramFacet.ComparatorType comparatorType;

    public HistogramScriptFacetBuilder(String name) {
        super(name);
    }

    public HistogramScriptFacetBuilder keyScript(String keyScript) {
        this.keyScript = keyScript;
        return this;
    }

    public HistogramScriptFacetBuilder valueScript(String valueScript) {
        this.valueScript = valueScript;
        return this;
    }

    public HistogramScriptFacetBuilder interval(long interval) {
        this.interval = interval;
        return this;
    }

    public HistogramScriptFacetBuilder param(String name, Object value) {
        if (params == null) {
            params = Maps.newHashMap();
        }
        params.put(name, value);
        return this;
    }

    public HistogramScriptFacetBuilder comparator(HistogramFacet.ComparatorType comparatorType) {
        this.comparatorType = comparatorType;
        return this;
    }

    public HistogramScriptFacetBuilder global(boolean global) {
        this.global = global;
        return this;
    }

    public HistogramScriptFacetBuilder facetFilter(XContentFilterBuilder filter) {
        this.facetFilter = filter;
        return this;
    }

    @Override public void toXContent(XContentBuilder builder, Params params) throws IOException {
        if (keyScript == null) {
            throw new SearchSourceBuilderException("key_script must be set on histogram script facet for facet [" + name + "]");
        }
        if (valueScript == null) {
            throw new SearchSourceBuilderException("value_script must be set on histogram script facet for facet [" + name + "]");
        }
        builder.startObject(name);

        builder.startObject(HistogramFacetCollectorParser.NAME);
        builder.field("key_script", keyScript);
        builder.field("value_script", valueScript);
        if (interval > 0) { // interval is optional in script facet, can be defined by the key script
            builder.field("interval", interval);
        }
        if (this.params != null) {
            builder.field("params", this.params);
        }
        if (comparatorType != null) {
            builder.field("comparator", comparatorType.description());
        }
        builder.endObject();

        addFilterFacetAndGlobal(builder, params);

        builder.endObject();
    }
}
