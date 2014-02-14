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

package org.elasticsearch.index.query;

import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * A filter for a field based on a term.
 *
 *
 */
public class TermFilterBuilder extends BaseFilterBuilder {

    private final String name;

    private final Object value;

    private Boolean cache;
    private String cacheKey;

    private String filterName;

    /**
     * A filter for a field based on a term.
     *
     * @param name  The field name
     * @param value The term value
     */
    public TermFilterBuilder(String name, String value) {
        this(name, (Object) value);
    }

    /**
     * A filter for a field based on a term.
     *
     * @param name  The field name
     * @param value The term value
     */
    public TermFilterBuilder(String name, int value) {
        this(name, (Object) value);
    }

    /**
     * A filter for a field based on a term.
     *
     * @param name  The field name
     * @param value The term value
     */
    public TermFilterBuilder(String name, long value) {
        this(name, (Object) value);
    }

    /**
     * A filter for a field based on a term.
     *
     * @param name  The field name
     * @param value The term value
     */
    public TermFilterBuilder(String name, float value) {
        this(name, (Object) value);
    }

    /**
     * A filter for a field based on a term.
     *
     * @param name  The field name
     * @param value The term value
     */
    public TermFilterBuilder(String name, double value) {
        this(name, (Object) value);
    }

    /**
     * A filter for a field based on a term.
     *
     * @param name  The field name
     * @param value The term value
     */
    public TermFilterBuilder(String name, Object value) {
        this.name = name;
        this.value = value;
    }

    /**
     * Sets the filter name for the filter that can be used when searching for matched_filters per hit.
     */
    public TermFilterBuilder filterName(String filterName) {
        this.filterName = filterName;
        return this;
    }

    /**
     * Should the filter be cached or not. Defaults to <tt>true</tt>.
     */
    public TermFilterBuilder cache(boolean cache) {
        this.cache = cache;
        return this;
    }

    public TermFilterBuilder cacheKey(String cacheKey) {
        this.cacheKey = cacheKey;
        return this;
    }

    @Override
    public void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(TermFilterParser.NAME);
        builder.field(name, value);
        if (filterName != null) {
            builder.field("_name", filterName);
        }
        if (cache != null) {
            builder.field("_cache", cache);
        }
        if (cacheKey != null) {
            builder.field("_cache_key", cacheKey);
        }
        builder.endObject();
    }
}