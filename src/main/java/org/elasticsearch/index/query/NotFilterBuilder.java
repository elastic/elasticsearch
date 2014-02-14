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
 * A filter that matches documents matching boolean combinations of other filters.
 *
 *
 */
public class NotFilterBuilder extends BaseFilterBuilder {

    private FilterBuilder filter;

    private Boolean cache;

    private String filterName;

    public NotFilterBuilder(FilterBuilder filter) {
        this.filter = filter;
    }

    /**
     * Should the filter be cached or not. Defaults to <tt>false</tt>.
     */
    public NotFilterBuilder cache(boolean cache) {
        this.cache = cache;
        return this;
    }

    public NotFilterBuilder filterName(String filterName) {
        this.filterName = filterName;
        return this;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NotFilterParser.NAME);
        builder.field("filter");
        filter.toXContent(builder, params);
        if (cache != null) {
            builder.field("_cache", cache);
        }
        if (filterName != null) {
            builder.field("_name", filterName);
        }
        builder.endObject();
    }
}