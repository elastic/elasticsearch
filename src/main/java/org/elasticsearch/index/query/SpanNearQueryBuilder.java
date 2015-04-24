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

import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;

/**
 *
 */
public class SpanNearQueryBuilder extends BaseQueryBuilder implements SpanQueryBuilder, BoostableQueryBuilder<SpanNearQueryBuilder> {

    private ArrayList<SpanQueryBuilder> clauses = new ArrayList<>();

    private Integer slop = null;

    private Boolean inOrder;

    private Boolean collectPayloads;

    private float boost = -1;

    private String queryName;

    public SpanNearQueryBuilder clause(SpanQueryBuilder clause) {
        clauses.add(clause);
        return this;
    }

    public SpanNearQueryBuilder slop(int slop) {
        this.slop = slop;
        return this;
    }

    public SpanNearQueryBuilder inOrder(boolean inOrder) {
        this.inOrder = inOrder;
        return this;
    }

    public SpanNearQueryBuilder collectPayloads(boolean collectPayloads) {
        this.collectPayloads = collectPayloads;
        return this;
    }

    @Override
    public SpanNearQueryBuilder boost(float boost) {
        this.boost = boost;
        return this;
    }

    /**
     * Sets the query name for the filter that can be used when searching for matched_filters per hit.
     */
    public SpanNearQueryBuilder queryName(String queryName) {
        this.queryName = queryName;
        return this;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        if (clauses.isEmpty()) {
            throw new ElasticsearchIllegalArgumentException("Must have at least one clause when building a spanNear query");
        }
        if (slop == null) {
            throw new ElasticsearchIllegalArgumentException("Must set the slop when building a spanNear query");
        }
        builder.startObject(SpanNearQueryParser.NAME);
        builder.startArray("clauses");
        for (SpanQueryBuilder clause : clauses) {
            clause.toXContent(builder, params);
        }
        builder.endArray();
        builder.field("slop", slop.intValue());
        if (inOrder != null) {
            builder.field("in_order", inOrder);
        }
        if (collectPayloads != null) {
            builder.field("collect_payloads", collectPayloads);
        }
        if (boost != -1) {
            builder.field("boost", boost);
        }
        if (queryName != null) {
            builder.field("_name", queryName);
        }
        builder.endObject();
    }
}
