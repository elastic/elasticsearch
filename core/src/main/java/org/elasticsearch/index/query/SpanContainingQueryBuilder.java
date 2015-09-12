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
 * Builder for {@link org.apache.lucene.search.spans.SpanContainingQuery}.
 */
public class SpanContainingQueryBuilder extends SpanQueryBuilder implements BoostableQueryBuilder<SpanContainingQueryBuilder> {

    private SpanQueryBuilder big;
    private SpanQueryBuilder little;
    private float boost = -1;
    private String queryName;

    /** 
     * Sets the little clause, it must be contained within {@code big} for a match.
     */
    public SpanContainingQueryBuilder little(SpanQueryBuilder clause) {
        this.little = clause;
        return this;
    }

    /** 
     * Sets the big clause, it must enclose {@code little} for a match.
     */
    public SpanContainingQueryBuilder big(SpanQueryBuilder clause) {
        this.big = clause;
        return this;
    }

    @Override
    public SpanContainingQueryBuilder boost(float boost) {
        this.boost = boost;
        return this;
    }

    /**
     * Sets the query name for the filter that can be used when searching for matched_filters per hit.
     */
    public SpanContainingQueryBuilder queryName(String queryName) {
        this.queryName = queryName;
        return this;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        if (big == null) {
            throw new IllegalArgumentException("Must specify big clause when building a span_containing query");
        }
        if (little == null) {
            throw new IllegalArgumentException("Must specify little clause when building a span_containing query");
        }
        builder.startObject(SpanContainingQueryParser.NAME);

        builder.field("big");
        big.toXContent(builder, params);

        builder.field("little");
        little.toXContent(builder, params);

        if (boost != -1) {
            builder.field("boost", boost);
        }

        if (queryName != null) {
            builder.field("_name", queryName);
        }

        builder.endObject();
    }
}
