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

import org.apache.lucene.search.spans.SpanQuery;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

public class SpanFirstQueryBuilder extends AbstractQueryBuilder<SpanFirstQueryBuilder> implements SpanQueryBuilder<SpanFirstQueryBuilder>, BoostableQueryBuilder<SpanFirstQueryBuilder> {

    public static final String NAME = "span_first";

    private final SpanQueryBuilder matchBuilder;

    private final int end;

    private float boost = -1;

    private String queryName;

    static final SpanFirstQueryBuilder SPAN_FIRST_QUERY_BUILDER = new SpanFirstQueryBuilder(null, -1);

    public SpanFirstQueryBuilder(SpanQueryBuilder matchBuilder, int end) {
        this.matchBuilder = matchBuilder;
        this.end = end;
    }

    @Override
    public SpanFirstQueryBuilder boost(float boost) {
        this.boost = boost;
        return this;
    }

    /**
     * Sets the query name for the filter that can be used when searching for matched_filters per hit.
     */
    public SpanFirstQueryBuilder queryName(String queryName) {
        this.queryName = queryName;
        return this;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.field("match");
        matchBuilder.toXContent(builder, params);
        builder.field("end", end);
        if (boost != -1) {
            builder.field("boost", boost);
        }
        if (queryName != null) {
            builder.field("_name", queryName);
        }
        builder.endObject();
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public SpanQuery toQuery(QueryParseContext parseContext) throws QueryParsingException, IOException {
        //norelease just a temporary implementation, will go away once this query is refactored and properly overrides toQuery
        return (SpanQuery)super.toQuery(parseContext);
    }
}
