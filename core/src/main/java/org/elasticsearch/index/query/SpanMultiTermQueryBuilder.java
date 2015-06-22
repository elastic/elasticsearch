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

public class SpanMultiTermQueryBuilder extends AbstractQueryBuilder<SpanMultiTermQueryBuilder> implements SpanQueryBuilder<SpanMultiTermQueryBuilder> {

    public static final String NAME = "span_multi";
    private MultiTermQueryBuilder multiTermQueryBuilder;
    static final SpanMultiTermQueryBuilder PROTOTYPE = new SpanMultiTermQueryBuilder(null);

    public SpanMultiTermQueryBuilder(MultiTermQueryBuilder multiTermQueryBuilder) {
        this.multiTermQueryBuilder = multiTermQueryBuilder;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params)
            throws IOException {
        builder.startObject(NAME);
        builder.field(SpanMultiTermQueryParser.MATCH_NAME);
        multiTermQueryBuilder.toXContent(builder, params);
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
