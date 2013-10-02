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

package org.elasticsearch.search.facet.termsstats;

import com.google.common.collect.Maps;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilderException;
import org.elasticsearch.search.facet.FacetBuilder;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;

/**
 *
 */
public class TermsStatsFacetBuilder extends FacetBuilder {

    private String keyField;
    private String valueField;
    private int size = -1;
    private int shardSize = -1;
    private TermsStatsFacet.ComparatorType comparatorType;

    private String script;
    private String lang;
    private Map<String, Object> params;

    /**
     * Constructs a new terms stats facet builder under the provided facet name.
     */
    public TermsStatsFacetBuilder(String name) {
        super(name);
    }

    public TermsStatsFacetBuilder keyField(String keyField) {
        this.keyField = keyField;
        return this;
    }

    public TermsStatsFacetBuilder valueField(String valueField) {
        this.valueField = valueField;
        return this;
    }

    /**
     * The order by which to return the facets by. Defaults to {@link TermsStatsFacet.ComparatorType#COUNT}.
     */
    public TermsStatsFacetBuilder order(TermsStatsFacet.ComparatorType comparatorType) {
        this.comparatorType = comparatorType;
        return this;
    }

    /**
     * Sets the size of the result.
     */
    public TermsStatsFacetBuilder size(int size) {
        this.size = size;
        return this;
    }

    /**
     * Sets the number of terms that will be returned from each shard. The higher the number the more accurate the results will be. The
     * shard size cannot be smaller than {@link #size(int) size}, therefore in this case it will fall back and be treated as being equal to
     * size.
     */
    public TermsStatsFacetBuilder shardSize(int shardSize) {
        this.shardSize = shardSize;
        return this;
    }

    /**
     * Marks all terms to be returned, even ones with 0 counts.
     */
    public TermsStatsFacetBuilder allTerms() {
        this.size = 0;
        return this;
    }

    /**
     * A value script to be executed (instead of value field) which results (numeric) will be used
     * to compute the totals.
     */
    public TermsStatsFacetBuilder valueScript(String script) {
        this.script = script;
        return this;
    }

    /**
     * The language of the script.
     */
    public TermsStatsFacetBuilder lang(String lang) {
        this.lang = lang;
        return this;
    }

    /**
     * A parameter that will be passed to the script.
     *
     * @param name  The name of the script parameter.
     * @param value The value of the script parameter.
     */
    public TermsStatsFacetBuilder param(String name, Object value) {
        if (params == null) {
            params = Maps.newHashMap();
        }
        params.put(name, value);
        return this;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (keyField == null) {
            throw new SearchSourceBuilderException("key field must be set on terms facet for facet [" + name + "]");
        }
        if (valueField == null && script == null) {
            throw new SearchSourceBuilderException("value field or value script must be set on terms facet for facet [" + name + "]");
        }
        builder.startObject(name);

        builder.startObject(TermsStatsFacet.TYPE);
        builder.field("key_field", keyField);
        if (valueField != null) {
            builder.field("value_field", valueField);
        }
        if (script != null) {
            builder.field("value_script", script);
            if (lang != null) {
                builder.field("lang", lang);
            }
            if (this.params != null) {
                builder.field("params", this.params);
            }
        }

        if (comparatorType != null) {
            builder.field("order", comparatorType.name().toLowerCase(Locale.ROOT));
        }

        if (size != -1) {
            builder.field("size", size);
        }
        if (shardSize > size) {
            builder.field("shard_size", shardSize);
        }

        builder.endObject();

        addFilterFacetAndGlobal(builder, params);

        builder.endObject();

        return builder;
    }
}