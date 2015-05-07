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
 * Constructs a filter that only match on documents that the field has a value in them.
 *
 *
 */
public class ExistsFilterBuilder extends BaseFilterBuilder {

    private String name;

    private String filterName;

    public ExistsFilterBuilder(String name) {
        this.name = name;
    }

    /**
     * Sets the filter name for the filter that can be used when searching for matched_filters per hit.
     */
    public ExistsFilterBuilder filterName(String filterName) {
        this.filterName = filterName;
        return this;
    }


    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(ExistsFilterParser.NAME);
        builder.field("field", name);
        if (filterName != null) {
            builder.field("_name", filterName);
        }
        builder.endObject();
    }
}
