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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * A filter that will return only documents matching specific ids (and a type).
 */
public class IdsFilterBuilder extends BaseFilterBuilder {

    private final List<String> types;

    private List<String> values = new ArrayList<>();

    private String filterName;

    /**
     * Create an ids filter based on the type.
     */
    public IdsFilterBuilder(String... types) {
        this.types = types == null ? null : Arrays.asList(types);
    }

    /**
     * Adds ids to the filter.
     */
    public IdsFilterBuilder addIds(String... ids) {
        values.addAll(Arrays.asList(ids));
        return this;
    }

    /**
     * Adds ids to the filter.
     */
    public IdsFilterBuilder ids(String... ids) {
        return addIds(ids);
    }

    /**
     * Sets the filter name for the filter that can be used when searching for matched_filters per hit.
     */
    public IdsFilterBuilder filterName(String filterName) {
        this.filterName = filterName;
        return this;
    }

    @Override
    public void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(IdsFilterParser.NAME);
        if (types != null) {
            if (types.size() == 1) {
                builder.field("type", types.get(0));
            } else {
                builder.startArray("types");
                for (Object type : types) {
                    builder.value(type);
                }
                builder.endArray();
            }
        }
        builder.startArray("values");
        for (Object value : values) {
            builder.value(value);
        }
        builder.endArray();

        if (filterName != null) {
            builder.field("_name", filterName);
        }

        builder.endObject();
    }
}