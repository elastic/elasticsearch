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

package org.elasticsearch.index.query.json;

import org.elasticsearch.util.json.JsonBuilder;

import java.io.IOException;
import java.util.ArrayList;

import static com.google.common.collect.Lists.*;

/**
 * @author kimchy (Shay Banon)
 */
public class DisMaxJsonQueryBuilder extends BaseJsonQueryBuilder {

    private ArrayList<JsonQueryBuilder> queries = newArrayList();

    private float boost = -1;

    private float tieBreakerMultiplier = -1;

    public DisMaxJsonQueryBuilder add(JsonQueryBuilder queryBuilder) {
        queries.add(queryBuilder);
        return this;
    }

    public DisMaxJsonQueryBuilder boost(float boost) {
        this.boost = boost;
        return this;
    }

    public DisMaxJsonQueryBuilder tieBreakerMultiplier(float tieBreakerMultiplier) {
        this.tieBreakerMultiplier = tieBreakerMultiplier;
        return this;
    }

    @Override protected void doJson(JsonBuilder builder) throws IOException {
        builder.startObject("disMax");
        if (tieBreakerMultiplier != -1) {
            builder.field("tieBreakerMultiplier", tieBreakerMultiplier);
        }
        if (boost != -1) {
            builder.field("boost", boost);
        }
        builder.startArray("queries");
        for (JsonQueryBuilder queryBuilder : queries) {
            queryBuilder.toJson(builder);
        }
        builder.endArray();
        builder.endObject();
    }
}