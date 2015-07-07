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
import org.apache.lucene.search.spans.SpanFirstQuery;
import org.apache.lucene.search.spans.SpanQuery;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

public class SpanFirstQueryBuilder extends AbstractQueryBuilder<SpanFirstQueryBuilder> implements SpanQueryBuilder<SpanFirstQueryBuilder>{

    public static final String NAME = "span_first";

    private final SpanQueryBuilder matchBuilder;

    private final int end;

    static final SpanFirstQueryBuilder SPAN_FIRST_QUERY_BUILDER = new SpanFirstQueryBuilder();

    /**
     * Query that matches spans queries defined in <code>matchBuilder</code>
     * whose end position is less than or equal to <code>end</code>.
     * @param matchBuilder inner {@link SpanQueryBuilder}
     * @param end maximum end position of the match, needs to be positive
     * @throws IllegalArgumentException for negative <code>end</code> positions
     */
    public SpanFirstQueryBuilder(SpanQueryBuilder matchBuilder, int end) {
        this.matchBuilder = Objects.requireNonNull(matchBuilder);
        if (end >= 0) {
            this.end = end;
        } else {
            throw new IllegalArgumentException("end parameter needs to be positive");
        }
    }

    /**
     * only used for prototype
     */
    private SpanFirstQueryBuilder() {
        this.matchBuilder = null;
        this.end = -1;
    }

    /**
     * @return the inner {@link SpanQueryBuilder} defined in this query
     */
    public SpanQueryBuilder matchBuilder() {
        return this.matchBuilder;
    }

    /**
     * @return maximum end position of the matching inner span query
     */
    public int end() {
        return this.end;
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
    protected Query doToQuery(QueryParseContext parseContext) throws IOException {
        Query innerSpanQuery = matchBuilder.toQuery(parseContext);
        assert innerSpanQuery instanceof SpanQuery;
        return new SpanFirstQuery((SpanQuery) innerSpanQuery, end);
    }

    @Override
    public QueryValidationException validate() {
        return validateInnerQuery(matchBuilder, null);
    }

    @Override
    protected SpanFirstQueryBuilder doReadFrom(StreamInput in) throws IOException {
        SpanQueryBuilder matchBuilder = in.readNamedWriteable();
        int end = in.readInt();
        return new SpanFirstQueryBuilder(matchBuilder, end);
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(matchBuilder);
        out.writeInt(end);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(matchBuilder, end);
    }

    @Override
    protected boolean doEquals(SpanFirstQueryBuilder other) {
        return Objects.equals(matchBuilder, other.matchBuilder) &&
               Objects.equals(end, other.end);
    }

    @Override
    public String getName() {
        return NAME;
    }
}
