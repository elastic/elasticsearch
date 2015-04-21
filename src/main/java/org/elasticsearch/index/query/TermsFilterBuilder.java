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
 * A filer for a field based on several terms matching on any of them.
 */
public class TermsFilterBuilder extends BaseFilterBuilder {

    private final String name;

    private final Object values;

    private Boolean cache;
    private String cacheKey;

    private String filterName;

    private String execution;

    /**
     * A filer for a field based on several terms matching on any of them.
     *
     * @param name   The field name
     * @param values The terms
     */
    public TermsFilterBuilder(String name, String... values) {
        this(name, (Object[]) values);
    }

    /**
     * A filer for a field based on several terms matching on any of them.
     *
     * @param name   The field name
     * @param values The terms
     */
    public TermsFilterBuilder(String name, int... values) {
        this.name = name;
        this.values = values;
    }

    /**
     * A filer for a field based on several terms matching on any of them.
     *
     * @param name   The field name
     * @param values The terms
     */
    public TermsFilterBuilder(String name, long... values) {
        this.name = name;
        this.values = values;
    }

    /**
     * A filer for a field based on several terms matching on any of them.
     *
     * @param name   The field name
     * @param values The terms
     */
    public TermsFilterBuilder(String name, float... values) {
        this.name = name;
        this.values = values;
    }

    /**
     * A filer for a field based on several terms matching on any of them.
     *
     * @param name   The field name
     * @param values The terms
     */
    public TermsFilterBuilder(String name, double... values) {
        this.name = name;
        this.values = values;
    }

    /**
     * A filer for a field based on several terms matching on any of them.
     *
     * @param name   The field name
     * @param values The terms
     */
    public TermsFilterBuilder(String name, Object... values) {
        this.name = name;
        this.values = values;
    }

    /**
     * A filer for a field based on several terms matching on any of them.
     *
     * @param name   The field name
     * @param values The terms
     */
    public TermsFilterBuilder(String name, Iterable values) {
        this.name = name;
        this.values = values;
    }

    /**
     * Sets the execution mode for the terms filter. Cane be either "plain", "bool"
     * "and". Defaults to "plain".
     * @deprecated elasticsearch now makes better decisions on its own
     */
    @Deprecated
    public TermsFilterBuilder execution(String execution) {
        this.execution = execution;
        return this;
    }

    /**
     * Sets the filter name for the filter that can be used when searching for matched_filters per hit.
     */
    public TermsFilterBuilder filterName(String filterName) {
        this.filterName = filterName;
        return this;
    }

    /**
     * Should the filter be cached or not. Defaults to <tt>false</tt>.
     */
    public TermsFilterBuilder cache(boolean cache) {
        this.cache = cache;
        return this;
    }

    public TermsFilterBuilder cacheKey(String cacheKey) {
        this.cacheKey = cacheKey;
        return this;
    }

    @Override
    public void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(TermsFilterParser.NAME);
        builder.field(name, values);

        if (execution != null) {
            builder.field("execution", execution);
        }

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