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

import org.apache.lucene.search.BooleanClause;
import org.elasticsearch.common.xcontent.builder.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * A Query that matches documents matching boolean combinations of other queries.
 *
 * @author kimchy (shay.banon)
 */
public class BoolQueryBuilder extends BaseQueryBuilder {

    private ArrayList<Clause> clauses = new ArrayList<Clause>();

    private float boost = -1;

    private Boolean disableCoord;

    private int minimumNumberShouldMatch = -1;

    /**
     * Adds a query that <b>must</b> appear in the matching documents.
     */
    public BoolQueryBuilder must(XContentQueryBuilder queryBuilder) {
        clauses.add(new Clause(queryBuilder, BooleanClause.Occur.MUST));
        return this;
    }

    /**
     * Adds a query that <b>must not</b> appear in the matching documents.
     */
    public BoolQueryBuilder mustNot(XContentQueryBuilder queryBuilder) {
        clauses.add(new Clause(queryBuilder, BooleanClause.Occur.MUST_NOT));
        return this;
    }

    /**
     * Adds a query that <i>should</i> appear in the matching documents. For a boolean query with no
     * <tt>MUST</tt> clauses one or more <code>SHOULD</code> clauses must match a document
     * for the BooleanQuery to match.
     *
     * @see #minimumNumberShouldMatch(int)
     */
    public BoolQueryBuilder should(XContentQueryBuilder queryBuilder) {
        clauses.add(new Clause(queryBuilder, BooleanClause.Occur.SHOULD));
        return this;
    }

    /**
     * Sets the boost for this query.  Documents matching this query will (in addition to the normal
     * weightings) have their score multiplied by the boost provided.
     */
    public BoolQueryBuilder boost(float boost) {
        this.boost = boost;
        return this;
    }

    /**
     * Disables <tt>Similarity#coord(int,int)</tt> in scoring. Defualts to <tt>false</tt>.
     */
    public BoolQueryBuilder disableCoord(boolean disableCoord) {
        this.disableCoord = disableCoord;
        return this;
    }

    /**
     * Specifies a minimum number of the optional (should) boolean clauses which must be satisfied.
     *
     * <p>By default no optional clauses are necessary for a match
     * (unless there are no required clauses).  If this method is used,
     * then the specified number of clauses is required.
     *
     * <p>Use of this method is totally independent of specifying that
     * any specific clauses are required (or prohibited).  This number will
     * only be compared against the number of matching optional clauses.
     *
     * @param minimumNumberShouldMatch the number of optional clauses that must match
     */
    public BoolQueryBuilder minimumNumberShouldMatch(int minimumNumberShouldMatch) {
        this.minimumNumberShouldMatch = minimumNumberShouldMatch;
        return this;
    }

    /**
     * A list of the current clauses.
     */
    public List<Clause> clauses() {
        return this.clauses;
    }

    @Override protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("bool");
        for (Clause clause : clauses) {
            if (clause.occur == BooleanClause.Occur.MUST) {
                builder.field("must");
                clause.queryBuilder.toXContent(builder, params);
            } else if (clause.occur == BooleanClause.Occur.MUST_NOT) {
                builder.field("must_not");
                clause.queryBuilder.toXContent(builder, params);
            } else if (clause.occur == BooleanClause.Occur.SHOULD) {
                builder.field("should");
                clause.queryBuilder.toXContent(builder, params);
            }
        }
        if (boost != -1) {
            builder.field("boost", boost);
        }
        if (disableCoord != null) {
            builder.field("disable_coord", disableCoord);
        }
        if (minimumNumberShouldMatch != -1) {
            builder.field("minimum_number_should_match", minimumNumberShouldMatch);
        }
        builder.endObject();
    }

    private static class Clause {
        final XContentQueryBuilder queryBuilder;
        final BooleanClause.Occur occur;

        private Clause(XContentQueryBuilder queryBuilder, BooleanClause.Occur occur) {
            this.queryBuilder = queryBuilder;
            this.occur = occur;
        }
    }
}
