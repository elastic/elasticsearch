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

import org.elasticsearch.util.xcontent.builder.XContentBuilder;

import java.io.IOException;

/**
 * A filter that matches documents matching boolean combinations of other filters.
 *
 * @author kimchy (shay.banon)
 */
public class NotFilterBuilder extends BaseFilterBuilder {

    private XContentFilterBuilder filter;

    private Boolean cache;

    public NotFilterBuilder(XContentFilterBuilder filter) {
        this.filter = filter;
    }

    /**
     * Should the inner filter be cached or not. Defaults to <tt>true</tt>.
     */
    public NotFilterBuilder cache(boolean cache) {
        this.cache = cache;
        return this;
    }

    @Override protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NotFilterParser.NAME);
        builder.field("filter");
        filter.toXContent(builder, params);
        if (cache != null) {
            builder.field("cache", cache);
        }
        builder.endObject();
    }
}