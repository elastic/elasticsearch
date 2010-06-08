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

package org.elasticsearch.index.query.xcontent;

import org.elasticsearch.util.collect.Lists;
import org.elasticsearch.util.xcontent.builder.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;

/**
 * A filter that matches documents matching boolean combinations of other filters.
 *
 * @author kimchy (shay.banon)
 */
public class OrFilterBuilder extends BaseFilterBuilder {

    private ArrayList<XContentFilterBuilder> filters = Lists.newArrayList();

    private Boolean cache;

    public OrFilterBuilder(XContentFilterBuilder... filters) {
        for (XContentFilterBuilder filter : filters) {
            this.filters.add(filter);
        }
    }

    /**
     * Adds a filter to the list of filters to "or".
     */
    public OrFilterBuilder add(XContentFilterBuilder filterBuilder) {
        filters.add(filterBuilder);
        return this;
    }

    /**
     * Should the inner filters be cached or not. Defaults to <tt>true</tt>.
     */
    public OrFilterBuilder cache(boolean cache) {
        this.cache = cache;
        return this;
    }

    @Override protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(OrFilterParser.NAME);
        builder.startArray("filters");
        for (XContentFilterBuilder filter : filters) {
            filter.toXContent(builder, params);
        }
        builder.endArray();
        if (cache != null) {
            builder.field("cache", cache);
        }
        builder.endObject();
    }
}