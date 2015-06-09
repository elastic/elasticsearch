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

import org.apache.lucene.search.Query;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * @deprecated Use {@link SearchRequestBuilder#setTerminateAfter(int)} instead.
 */
@Deprecated
public class LimitQueryBuilder extends QueryBuilder<LimitQueryBuilder> {

    public static final String NAME = "limit";
    private final int limit;
    static final LimitQueryBuilder PROTOTYPE = new LimitQueryBuilder(-1);

    public LimitQueryBuilder(int limit) {
        this.limit = limit;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.field("value", limit);
        builder.endObject();
    }

    @Override
    public Query toQuery(QueryParseContext parseContext) {
        // this filter is deprecated and parses to a filter that matches everything
        return Queries.newMatchAllQuery();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LimitQueryBuilder that = (LimitQueryBuilder) o;
        return Integer.compare(that.limit, limit) == 0;
    }

    @Override
    public int hashCode() {
        return this.limit;
    }

    @Override
    public LimitQueryBuilder readFrom(StreamInput in) throws IOException {
        LimitQueryBuilder limitQueryBuilder = new LimitQueryBuilder(in.readInt());
        return limitQueryBuilder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeInt(this.limit);
    }

    @Override
    public String queryId() {
        return NAME;
    }
}
