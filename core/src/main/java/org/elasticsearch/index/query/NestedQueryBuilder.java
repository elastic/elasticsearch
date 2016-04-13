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
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.search.join.ScoreMode;
import org.apache.lucene.search.join.ToParentBlockJoinQuery;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.object.ObjectMapper;
import org.elasticsearch.index.query.support.InnerHitBuilder;

import java.io.IOException;
import java.util.Objects;

public class NestedQueryBuilder extends AbstractQueryBuilder<NestedQueryBuilder> {

    /**
     * The queries name used while parsing
     */
    public static final String NAME = "nested";
    public static final ParseField QUERY_NAME_FIELD = new ParseField(NAME);

    /**
     * The default score move for nested queries.
     */
    public static final ScoreMode DEFAULT_SCORE_MODE = ScoreMode.Avg;

    private static final ParseField SCORE_MODE_FIELD = new ParseField("score_mode");
    private static final ParseField PATH_FIELD = new ParseField("path");
    private static final ParseField QUERY_FIELD = new ParseField("query");
    private static final ParseField INNER_HITS_FIELD = new ParseField("inner_hits");

    private final QueryBuilder<?> query;

    private final String path;

    private ScoreMode scoreMode = DEFAULT_SCORE_MODE;

    private InnerHitBuilder innerHitBuilder;

    public NestedQueryBuilder(String path, QueryBuilder<?> query) {
        if (path == null) {
            throw new IllegalArgumentException("[" + NAME + "] requires 'path' field");
        }
        if (query == null) {
            throw new IllegalArgumentException("[" + NAME + "] requires 'query' field");
        }
        this.path = path;
        this.query = query;
    }

    public NestedQueryBuilder(String path, QueryBuilder query, ScoreMode scoreMode, InnerHitBuilder innerHitBuilder) {
        this(path, query);
        scoreMode(scoreMode);
        this.innerHitBuilder = innerHitBuilder;
        if (this.innerHitBuilder != null) {
            this.innerHitBuilder.setNestedPath(path);
            this.innerHitBuilder.setQuery(query);
        }
    }

    /**
     * Read from a stream.
     */
    public NestedQueryBuilder(StreamInput in) throws IOException {
        super(in);
        path = in.readString();
        scoreMode = ScoreMode.values()[in.readVInt()];
        query = in.readQuery();
        innerHitBuilder = in.readOptionalWriteable(InnerHitBuilder::new);
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(path);
        out.writeVInt(scoreMode.ordinal());
        out.writeQuery(query);
        out.writeOptionalWriteable(innerHitBuilder);
    }

    /**
     * The score mode how the scores from the matching child documents are mapped into the nested parent document.
     */
    public NestedQueryBuilder scoreMode(ScoreMode scoreMode) {
        if (scoreMode == null) {
            throw new IllegalArgumentException("[" + NAME + "] requires 'score_mode' field");
        }
        this.scoreMode = scoreMode;
        return this;
    }

    /**
     * Sets inner hit definition in the scope of this nested query and reusing the defined path and query.
     */
    public NestedQueryBuilder innerHit(InnerHitBuilder innerHit) {
        this.innerHitBuilder = Objects.requireNonNull(innerHit);
        this.innerHitBuilder.setNestedPath(path);
        this.innerHitBuilder.setQuery(query);
        return this;
    }

    /**
     * Returns the nested query to execute.
     */
    public QueryBuilder query() {
        return query;
    }

    /**
     * Returns inner hit definition in the scope of this query and reusing the defined type and query.
     */
    public InnerHitBuilder innerHit() {
        return innerHitBuilder;
    }

    /**
     * Returns how the scores from the matching child documents are mapped into the nested parent document.
     */
    public ScoreMode scoreMode() {
        return scoreMode;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.field(QUERY_FIELD.getPreferredName());
        query.toXContent(builder, params);
        builder.field(PATH_FIELD.getPreferredName(), path);
        if (scoreMode != null) {
            builder.field(SCORE_MODE_FIELD.getPreferredName(), HasChildQueryBuilder.scoreModeAsString(scoreMode));
        }
        printBoostAndQueryName(builder);
        if (innerHitBuilder != null) {
            builder.field(INNER_HITS_FIELD.getPreferredName(), innerHitBuilder, params);
        }
        builder.endObject();
    }

    public static NestedQueryBuilder fromXContent(QueryParseContext parseContext) throws IOException {
        XContentParser parser = parseContext.parser();
        float boost = AbstractQueryBuilder.DEFAULT_BOOST;
        ScoreMode scoreMode = NestedQueryBuilder.DEFAULT_SCORE_MODE;
        String queryName = null;
        QueryBuilder query = null;
        String path = null;
        String currentFieldName = null;
        InnerHitBuilder innerHitBuilder = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (parseContext.parseFieldMatcher().match(currentFieldName, QUERY_FIELD)) {
                    query = parseContext.parseInnerQueryBuilder();
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, INNER_HITS_FIELD)) {
                    innerHitBuilder = InnerHitBuilder.fromXContent(parser, parseContext);
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "[nested] query does not support [" + currentFieldName + "]");
                }
            } else if (token.isValue()) {
                if (parseContext.parseFieldMatcher().match(currentFieldName, PATH_FIELD)) {
                    path = parser.text();
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, AbstractQueryBuilder.BOOST_FIELD)) {
                    boost = parser.floatValue();
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, SCORE_MODE_FIELD)) {
                    scoreMode = HasChildQueryBuilder.parseScoreMode(parser.text());
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, AbstractQueryBuilder.NAME_FIELD)) {
                    queryName = parser.text();
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "[nested] query does not support [" + currentFieldName + "]");
                }
            }
        }
        return new NestedQueryBuilder(path, query, scoreMode, innerHitBuilder).queryName(queryName).boost(boost);
    }

    @Override
    public final String getWriteableName() {
        return NAME;
    }

    @Override
    protected boolean doEquals(NestedQueryBuilder that) {
        return Objects.equals(query, that.query)
                && Objects.equals(path, that.path)
                && Objects.equals(scoreMode, that.scoreMode)
                && Objects.equals(innerHitBuilder, that.innerHitBuilder);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(query, path, scoreMode, innerHitBuilder);
    }

    @Override
    protected Query doToQuery(QueryShardContext context) throws IOException {
        ObjectMapper nestedObjectMapper = context.getObjectMapper(path);
        if (nestedObjectMapper == null) {
            throw new IllegalStateException("[" + NAME + "] failed to find nested object under path [" + path + "]");
        }
        if (!nestedObjectMapper.nested().isNested()) {
            throw new IllegalStateException("[" + NAME + "] nested object under path [" + path + "] is not of nested type");
        }
        final BitSetProducer parentFilter;
        final Query childFilter;
        final Query innerQuery;
        ObjectMapper objectMapper = context.nestedScope().getObjectMapper();
        if (innerHitBuilder != null) {
            context.addInnerHit(innerHitBuilder);
        }
        if (objectMapper == null) {
            parentFilter = context.bitsetFilter(Queries.newNonNestedFilter());
        } else {
            parentFilter = context.bitsetFilter(objectMapper.nestedTypeFilter());
        }
        childFilter = nestedObjectMapper.nestedTypeFilter();
        try {
            context.nestedScope().nextLevel(nestedObjectMapper);
            innerQuery = this.query.toQuery(context);
            if (innerQuery == null) {
                return null;
            }
        } finally {
            context.nestedScope().previousLevel();
        }
        return new ToParentBlockJoinQuery(Queries.filtered(innerQuery, childFilter), parentFilter, scoreMode);
    }

    @Override
    protected QueryBuilder<?> doRewrite(QueryRewriteContext queryRewriteContext) throws IOException {
        QueryBuilder rewrite = query.rewrite(queryRewriteContext);
        if (rewrite != query) {
            return new NestedQueryBuilder(path, rewrite).scoreMode(scoreMode).innerHit(innerHitBuilder);
        }
        return this;
    }
}
