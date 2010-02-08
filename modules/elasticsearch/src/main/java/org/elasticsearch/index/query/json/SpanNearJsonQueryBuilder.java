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

import org.elasticsearch.index.query.QueryBuilderException;
import org.elasticsearch.util.json.JsonBuilder;

import java.io.IOException;
import java.util.ArrayList;

/**
 * @author kimchy (Shay Banon)
 */
public class SpanNearJsonQueryBuilder extends BaseJsonQueryBuilder implements JsonSpanQueryBuilder {

    private ArrayList<JsonSpanQueryBuilder> clauses = new ArrayList<JsonSpanQueryBuilder>();

    private int slop = -1;

    private Boolean inOrder;

    private Boolean collectPayloads;

    private float boost = -1;

    public SpanNearJsonQueryBuilder clause(JsonSpanQueryBuilder clause) {
        clauses.add(clause);
        return this;
    }

    public SpanNearJsonQueryBuilder slop(int slop) {
        this.slop = slop;
        return this;
    }

    public SpanNearJsonQueryBuilder inOrder(boolean inOrder) {
        this.inOrder = inOrder;
        return this;
    }

    public SpanNearJsonQueryBuilder collectPayloads(boolean collectPayloads) {
        this.collectPayloads = collectPayloads;
        return this;
    }

    public SpanNearJsonQueryBuilder boost(float boost) {
        this.boost = boost;
        return this;
    }

    @Override protected void doJson(JsonBuilder builder) throws IOException {
        if (clauses.isEmpty()) {
            throw new QueryBuilderException("Must have at least one clause when building a spanNear query");
        }
        if (slop == -1) {
            throw new QueryBuilderException("Must set the slop when building a spanNear query");
        }
        builder.startObject(SpanNearJsonQueryParser.NAME);
        builder.startArray("clauses");
        for (JsonSpanQueryBuilder clause : clauses) {
            clause.toJson(builder);
        }
        builder.endArray();
        builder.field("slop", slop);
        if (inOrder != null) {
            builder.field("inOrder", inOrder);
        }
        if (collectPayloads != null) {
            builder.field("collectPayloads", collectPayloads);
        }
        if (boost != -1) {
            builder.field("boost", boost);
        }
        builder.endObject();
    }
}
