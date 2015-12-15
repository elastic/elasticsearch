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

import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.spans.SpanBoostQuery;
import org.apache.lucene.search.spans.SpanMultiTermQueryWrapper;
import org.apache.lucene.search.spans.SpanQuery;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * Query that allows wraping a {@link MultiTermQueryBuilder} (one of wildcard, fuzzy, prefix, term, range or regexp query)
 * as a {@link SpanQueryBuilder} so it can be nested.
 */
public class SpanMultiTermQueryBuilder extends AbstractQueryBuilder<SpanMultiTermQueryBuilder> implements SpanQueryBuilder<SpanMultiTermQueryBuilder> {

    public static final String NAME = "span_multi";
    private final MultiTermQueryBuilder multiTermQueryBuilder;
    static final SpanMultiTermQueryBuilder PROTOTYPE = new SpanMultiTermQueryBuilder(RangeQueryBuilder.PROTOTYPE);

    public SpanMultiTermQueryBuilder(MultiTermQueryBuilder multiTermQueryBuilder) {
        if (multiTermQueryBuilder == null) {
            throw new IllegalArgumentException("inner multi term query cannot be null");
        }
        this.multiTermQueryBuilder = multiTermQueryBuilder;
    }

    public MultiTermQueryBuilder innerQuery() {
        return this.multiTermQueryBuilder;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params)
            throws IOException {
        builder.startObject(NAME);
        builder.field(SpanMultiTermQueryParser.MATCH_FIELD.getPreferredName());
        multiTermQueryBuilder.toXContent(builder, params);
        printBoostAndQueryName(builder);
        builder.endObject();
    }

    @Override
    protected Query doToQuery(QueryShardContext context) throws IOException {
        Query subQuery = multiTermQueryBuilder.toQuery(context);
        float boost = AbstractQueryBuilder.DEFAULT_BOOST;
        if (subQuery instanceof BoostQuery) {
            BoostQuery boostQuery = (BoostQuery) subQuery;
            subQuery = boostQuery.getQuery();
            boost = boostQuery.getBoost();
        }
        //no MultiTermQuery extends SpanQuery, so SpanBoostQuery is not supported here
        assert subQuery instanceof SpanBoostQuery == false;
        if (subQuery instanceof MultiTermQuery == false) {
            throw new UnsupportedOperationException("unsupported inner query, should be " + MultiTermQuery.class.getName() +" but was "
                    + subQuery.getClass().getName());
        }
        SpanQuery wrapper = new SpanMultiTermQueryWrapper<>((MultiTermQuery) subQuery);
        if (boost != AbstractQueryBuilder.DEFAULT_BOOST) {
            wrapper = new SpanBoostQuery(wrapper, boost);
        }
        return wrapper;
    }

    @Override
    protected SpanMultiTermQueryBuilder doReadFrom(StreamInput in) throws IOException {
        MultiTermQueryBuilder multiTermBuilder = (MultiTermQueryBuilder)in.readQuery();
        return new SpanMultiTermQueryBuilder(multiTermBuilder);
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeQuery(multiTermQueryBuilder);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(multiTermQueryBuilder);
    }

    @Override
    protected boolean doEquals(SpanMultiTermQueryBuilder other) {
        return Objects.equals(multiTermQueryBuilder, other.multiTermQueryBuilder);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }
}
