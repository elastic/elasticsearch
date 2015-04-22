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

import com.google.common.collect.Lists;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;

/**
 * A filter that matches documents matching boolean combinations of other filters.
 * @deprecated Use {@link BoolFilterBuilder} instead
 */
@Deprecated
public class AndFilterBuilder extends BaseFilterBuilder {

    private ArrayList<FilterBuilder> filters = Lists.newArrayList();

    private Boolean cache;
    private String cacheKey;

    private String filterName;

    public AndFilterBuilder(FilterBuilder... filters) {
        for (FilterBuilder filter : filters) {
            this.filters.add(filter);
        }
    }

    /**
     * Adds a filter to the list of filters to "and".
     */
    public AndFilterBuilder add(FilterBuilder filterBuilder) {
        filters.add(filterBuilder);
        return this;
    }

    /**
     * Should the filter be cached or not. Defaults to <tt>false</tt>.
     */
    public AndFilterBuilder cache(boolean cache) {
        this.cache = cache;
        return this;
    }

    public AndFilterBuilder cacheKey(String cacheKey) {
        this.cacheKey = cacheKey;
        return this;
    }

    /**
     * Sets the filter name for the filter that can be used when searching for matched_filters per hit.
     */
    public AndFilterBuilder filterName(String filterName) {
        this.filterName = filterName;
        return this;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(AndFilterParser.NAME);
        builder.startArray("filters");
        for (FilterBuilder filter : filters) {
            filter.toXContent(builder, params);
        }
        builder.endArray();
        if (cache != null) {
            builder.field("_cache", cache);
        }
        if (cacheKey != null) {
            builder.field("_cache_key", cacheKey);
        }
        if (filterName != null) {
            builder.field("_name", filterName);
        }
        builder.endObject();
    }
}