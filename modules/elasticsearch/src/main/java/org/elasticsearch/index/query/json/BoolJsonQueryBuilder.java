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

import org.apache.lucene.search.BooleanClause;
import org.elasticsearch.util.json.JsonBuilder;

import java.io.IOException;
import java.util.ArrayList;

/**
 * @author kimchy (Shay Banon)
 */
public class BoolJsonQueryBuilder extends BaseJsonQueryBuilder {

    private ArrayList<Clause> clauses = new ArrayList<Clause>();

    private float boost = -1;

    private Boolean disableCoord;

    private int minimumNumberShouldMatch = -1;

    public BoolJsonQueryBuilder must(JsonQueryBuilder queryBuilder) {
        clauses.add(new Clause(queryBuilder, BooleanClause.Occur.MUST));
        return this;
    }

    public BoolJsonQueryBuilder mustNot(JsonQueryBuilder queryBuilder) {
        clauses.add(new Clause(queryBuilder, BooleanClause.Occur.MUST_NOT));
        return this;
    }

    public BoolJsonQueryBuilder should(JsonQueryBuilder queryBuilder) {
        clauses.add(new Clause(queryBuilder, BooleanClause.Occur.SHOULD));
        return this;
    }

    public BoolJsonQueryBuilder boost(float boost) {
        this.boost = boost;
        return this;
    }

    public BoolJsonQueryBuilder disableCoord(boolean disableCoord) {
        this.disableCoord = disableCoord;
        return this;
    }

    public BoolJsonQueryBuilder minimumNumberShouldMatch(int minimumNumberShouldMatch) {
        this.minimumNumberShouldMatch = minimumNumberShouldMatch;
        return this;
    }

    @Override protected void doJson(JsonBuilder builder) throws IOException {
        builder.startObject("bool");
        for (Clause clause : clauses) {
            if (clause.occur == BooleanClause.Occur.MUST) {
                builder.field("must");
                clause.queryBuilder.toJson(builder);
            } else if (clause.occur == BooleanClause.Occur.MUST_NOT) {
                builder.field("mustNot");
                clause.queryBuilder.toJson(builder);
            } else if (clause.occur == BooleanClause.Occur.SHOULD) {
                builder.field("should");
                clause.queryBuilder.toJson(builder);
            }
        }
        if (boost != -1) {
            builder.field("boost", boost);
        }
        if (disableCoord != null) {
            builder.field("disableCoord", disableCoord);
        }
        if (minimumNumberShouldMatch == -1) {
            builder.field("minimumNumberShouldMatch", minimumNumberShouldMatch);
        }
        builder.endObject();
    }

    private static class Clause {
        final JsonQueryBuilder queryBuilder;
        final BooleanClause.Occur occur;

        private Clause(JsonQueryBuilder queryBuilder, BooleanClause.Occur occur) {
            this.queryBuilder = queryBuilder;
            this.occur = occur;
        }
    }
}
