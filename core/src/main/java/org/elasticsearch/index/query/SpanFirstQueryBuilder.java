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

public class SpanFirstQueryBuilder extends AbstractQueryBuilder<SpanFirstQueryBuilder> implements SpanQueryBuilder<SpanFirstQueryBuilder>{

    public static final String NAME = "span_first";

    private final SpanQueryBuilder matchBuilder;

    private final int end;

    static final SpanFirstQueryBuilder SPAN_FIRST_QUERY_BUILDER = new SpanFirstQueryBuilder(null, -1);

    public SpanFirstQueryBuilder(SpanQueryBuilder matchBuilder, int end) {
        this.matchBuilder = matchBuilder;
        this.end = end;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.field("match");
        matchBuilder.toXContent(builder, params);
        builder.field("end", end);
        printBoostAndQueryName(builder);
        builder.endObject();
    }

    @Override
    public String getName() {
        return NAME;
    }
}
