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
import java.util.Collection;
import java.util.List;

/**
 * A query that will return only documents matching specific ids (and a type).
 */
public class IdsQueryBuilder extends QueryBuilder implements BoostableQueryBuilder<IdsQueryBuilder> {

    private final List<String> types;

    private List<String> values = new ArrayList<>();

    private float boost = -1;

    private String queryName;

    public IdsQueryBuilder(String... types) {
        this.types = types == null ? null : Arrays.asList(types);
    }

    /**
     * Adds ids to the filter.
     */
    public IdsQueryBuilder addIds(String... ids) {
        values.addAll(Arrays.asList(ids));
        return this;
    }

    /**
     * Adds ids to the filter.
     */
    public IdsQueryBuilder addIds(Collection<String> ids) {
        values.addAll(ids);
        return this;
    }

    /**
     * Adds ids to the filter.
     */
    public IdsQueryBuilder ids(String... ids) {
        return addIds(ids);
    }

    /**
     * Adds ids to the filter.
     */
    public IdsQueryBuilder ids(Collection<String> ids) {
        return addIds(ids);
    }

    /**
     * Sets the boost for this query.  Documents matching this query will (in addition to the normal
     * weightings) have their score multiplied by the boost provided.
     */
    @Override
    public IdsQueryBuilder boost(float boost) {
        this.boost = boost;
        return this;
    }

    /**
     * Sets the query name for the filter that can be used when searching for matched_filters per hit.
     */
    public IdsQueryBuilder queryName(String queryName) {
        this.queryName = queryName;
        return this;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(IdsQueryParser.NAME);
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
        if (boost != -1) {
            builder.field("boost", boost);
        }
        if (queryName != null) {
            builder.field("_name", queryName);
        }
        builder.endObject();
    }
}