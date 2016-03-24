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
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.spans.SpanWithinQuery;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * Builder for {@link org.apache.lucene.search.spans.SpanWithinQuery}.
 */
public class SpanWithinQueryBuilder extends AbstractQueryBuilder<SpanWithinQueryBuilder> implements SpanQueryBuilder<SpanWithinQueryBuilder> {

    public static final String NAME = "span_within";
    private final SpanQueryBuilder big;
    private final SpanQueryBuilder little;
    static final SpanWithinQueryBuilder PROTOTYPE = new SpanWithinQueryBuilder(SpanTermQueryBuilder.PROTOTYPE, SpanTermQueryBuilder.PROTOTYPE);

    /**
     * Query that returns spans from <code>little</code> that are contained in a spans from <code>big</code>.
     * @param big clause that must enclose {@code little} for a match.
     * @param little the little clause, it must be contained within {@code big} for a match.
     */
    public SpanWithinQueryBuilder(SpanQueryBuilder big, SpanQueryBuilder little) {
        if (big == null) {
            throw new IllegalArgumentException("inner clause [big] cannot be null.");
        }
        if (little == null) {
            throw new IllegalArgumentException("inner clause [little] cannot be null.");
        }
        this.little = little;
        this.big = big;
    }

    /**
     * @return the little clause, contained within {@code big} for a match.
     */
    public SpanQueryBuilder littleQuery() {
        return this.little;
    }

    /**
     * @return the big clause that must enclose {@code little} for a match.
     */
    public SpanQueryBuilder bigQuery() {
        return this.big;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);

        builder.field(SpanWithinQueryParser.BIG_FIELD.getPreferredName());
        big.toXContent(builder, params);

        builder.field(SpanWithinQueryParser.LITTLE_FIELD.getPreferredName());
        little.toXContent(builder, params);

        printBoostAndQueryName(builder);

        builder.endObject();
    }

    @Override
    protected Query doToQuery(QueryShardContext context) throws IOException {
        Query innerBig = big.toQuery(context);
        assert innerBig instanceof SpanQuery;
        Query innerLittle = little.toQuery(context);
        assert innerLittle instanceof SpanQuery;
        return new SpanWithinQuery((SpanQuery) innerBig, (SpanQuery) innerLittle);
    }

    @Override
    protected SpanWithinQueryBuilder doReadFrom(StreamInput in) throws IOException {
        SpanQueryBuilder big = (SpanQueryBuilder)in.readQuery();
        SpanQueryBuilder little = (SpanQueryBuilder)in.readQuery();
        return new SpanWithinQueryBuilder(big, little);
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeQuery(big);
        out.writeQuery(little);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(big, little);
    }

    @Override
    protected boolean doEquals(SpanWithinQueryBuilder other) {
        return Objects.equals(big, other.big) &&
               Objects.equals(little, other.little);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }
}
