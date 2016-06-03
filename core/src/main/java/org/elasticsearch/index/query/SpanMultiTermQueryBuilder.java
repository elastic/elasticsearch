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
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;
import java.util.Optional;

/**
 * Query that allows wraping a {@link MultiTermQueryBuilder} (one of wildcard, fuzzy, prefix, term, range or regexp query)
 * as a {@link SpanQueryBuilder} so it can be nested.
 */
public class SpanMultiTermQueryBuilder extends AbstractQueryBuilder<SpanMultiTermQueryBuilder>
        implements SpanQueryBuilder {

    public static final String NAME = "span_multi";
    public static final ParseField QUERY_NAME_FIELD = new ParseField(NAME);

    private static final ParseField MATCH_FIELD = new ParseField("match");

    private final MultiTermQueryBuilder multiTermQueryBuilder;

    public SpanMultiTermQueryBuilder(MultiTermQueryBuilder multiTermQueryBuilder) {
        if (multiTermQueryBuilder == null) {
            throw new IllegalArgumentException("inner multi term query cannot be null");
        }
        this.multiTermQueryBuilder = multiTermQueryBuilder;
    }

    /**
     * Read from a stream.
     */
    public SpanMultiTermQueryBuilder(StreamInput in) throws IOException {
        super(in);
        multiTermQueryBuilder = (MultiTermQueryBuilder) in.readNamedWriteable(QueryBuilder.class);
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(multiTermQueryBuilder);
    }

    public MultiTermQueryBuilder innerQuery() {
        return this.multiTermQueryBuilder;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params)
            throws IOException {
        builder.startObject(NAME);
        builder.field(MATCH_FIELD.getPreferredName());
        multiTermQueryBuilder.toXContent(builder, params);
        printBoostAndQueryName(builder);
        builder.endObject();
    }

    public static Optional<SpanMultiTermQueryBuilder> fromXContent(QueryParseContext parseContext) throws IOException {
        XContentParser parser = parseContext.parser();
        String currentFieldName = null;
        MultiTermQueryBuilder subQuery = null;
        String queryName = null;
        float boost = AbstractQueryBuilder.DEFAULT_BOOST;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (parseContext.getParseFieldMatcher().match(currentFieldName, MATCH_FIELD)) {
                    Optional<QueryBuilder> query = parseContext.parseInnerQueryBuilder();
                    if (query.isPresent() == false || query.get() instanceof MultiTermQueryBuilder == false) {
                        throw new ParsingException(parser.getTokenLocation(),
                                "[span_multi] [" + MATCH_FIELD.getPreferredName() + "] must be of type multi term query");
                    }
                    subQuery = (MultiTermQueryBuilder) query.get();
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "[span_multi] query does not support [" + currentFieldName + "]");
                }
            } else if (token.isValue()) {
                if (parseContext.getParseFieldMatcher().match(currentFieldName, AbstractQueryBuilder.NAME_FIELD)) {
                    queryName = parser.text();
                } else if (parseContext.getParseFieldMatcher().match(currentFieldName, AbstractQueryBuilder.BOOST_FIELD)) {
                    boost = parser.floatValue();
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "[span_multi] query does not support [" + currentFieldName + "]");
                }
            }
        }

        if (subQuery == null) {
            throw new ParsingException(parser.getTokenLocation(),
                    "[span_multi] must have [" + MATCH_FIELD.getPreferredName() + "] multi term query clause");
        }

        return Optional.of(new SpanMultiTermQueryBuilder(subQuery).queryName(queryName).boost(boost));
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
