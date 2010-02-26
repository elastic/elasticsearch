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
public class BoolJsonFilterBuilder extends BaseJsonQueryBuilder {

    private ArrayList<Clause> clauses = new ArrayList<Clause>();

    public BoolJsonFilterBuilder must(JsonFilterBuilder filterBuilder) {
        clauses.add(new Clause(filterBuilder, BooleanClause.Occur.MUST));
        return this;
    }

    public BoolJsonFilterBuilder mustNot(JsonFilterBuilder filterBuilder) {
        clauses.add(new Clause(filterBuilder, BooleanClause.Occur.MUST_NOT));
        return this;
    }

    public BoolJsonFilterBuilder should(JsonFilterBuilder filterBuilder) {
        clauses.add(new Clause(filterBuilder, BooleanClause.Occur.SHOULD));
        return this;
    }

    @Override protected void doJson(JsonBuilder builder, Params params) throws IOException {
        builder.startObject("bool");
        for (Clause clause : clauses) {
            if (clause.occur == BooleanClause.Occur.MUST) {
                builder.field("must");
                clause.filterBuilder.toJson(builder, params);
            } else if (clause.occur == BooleanClause.Occur.MUST_NOT) {
                builder.field("mustNot");
                clause.filterBuilder.toJson(builder, params);
            } else if (clause.occur == BooleanClause.Occur.SHOULD) {
                builder.field("should");
                clause.filterBuilder.toJson(builder, params);
            }
        }
        builder.endObject();
    }

    private static class Clause {
        final JsonFilterBuilder filterBuilder;
        final BooleanClause.Occur occur;

        private Clause(JsonFilterBuilder filterBuilder, BooleanClause.Occur occur) {
            this.filterBuilder = filterBuilder;
            this.occur = occur;
        }
    }
}