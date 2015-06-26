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

import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * A query that matches on all documents.
 */
public class MatchAllQueryBuilder extends AbstractQueryBuilder<MatchAllQueryBuilder> implements BoostableQueryBuilder<MatchAllQueryBuilder> {

    public static final String NAME = "match_all";

    private float boost = 1.0f;

    static final MatchAllQueryBuilder PROTOTYPE = new MatchAllQueryBuilder();

    /**
     * Sets the boost for this query.  Documents matching this query will (in addition to the normal
     * weightings) have their score multiplied by the boost provided.
     */
    @Override
    public MatchAllQueryBuilder boost(float boost) {
        this.boost = boost;
        return this;
    }

    /**
     * Gets the boost for this query.
     */
    public float boost() {
        return this.boost;
    }

    @Override
    public void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        if (boost != 1.0f) {
            builder.field("boost", boost);
        }
        builder.endObject();
    }

    @Override
    public Query toQuery(QueryParseContext parseContext) {
        if (this.boost == 1.0f) {
            return Queries.newMatchAllQuery();
        }
        MatchAllDocsQuery query = new MatchAllDocsQuery();
        query.setBoost(boost);
        return query;
    }

    @Override
    public QueryValidationException validate() {
        // nothing to validate
        return null;
    }

    @Override
    public boolean doEquals(MatchAllQueryBuilder other) {
        return Float.compare(other.boost, boost) == 0;
    }

    @Override
    public int hashCode() {
        return boost != +0.0f ? Float.floatToIntBits(boost) : 0;
    }

    @Override
    public MatchAllQueryBuilder readFrom(StreamInput in) throws IOException {
        MatchAllQueryBuilder matchAllQueryBuilder = new MatchAllQueryBuilder();
        matchAllQueryBuilder.boost = in.readFloat();
        return matchAllQueryBuilder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeFloat(this.boost);
    }

    @Override
    public String getName() {
        return NAME;
    }
}
