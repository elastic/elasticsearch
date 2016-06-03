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

import org.apache.lucene.search.MatchNoDocsQuery;
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

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public class NestedQueryBuilder extends AbstractQueryBuilder<NestedQueryBuilder> {

    /**
     * The queries name used while parsing
     */
    public static final String NAME = "nested";
    public static final ParseField QUERY_NAME_FIELD = new ParseField(NAME);
    /**
     * The default value for ignore_unmapped.
     */
    public static final boolean DEFAULT_IGNORE_UNMAPPED = false;

    private static final ParseField SCORE_MODE_FIELD = new ParseField("score_mode");
    private static final ParseField PATH_FIELD = new ParseField("path");
    private static final ParseField QUERY_FIELD = new ParseField("query");
    private static final ParseField INNER_HITS_FIELD = new ParseField("inner_hits");
    private static final ParseField IGNORE_UNMAPPED_FIELD = new ParseField("ignore_unmapped");

    private final String path;
    private final ScoreMode scoreMode;
    private final QueryBuilder query;
    private InnerHitBuilder innerHitBuilder;
    private boolean ignoreUnmapped = DEFAULT_IGNORE_UNMAPPED;

    public NestedQueryBuilder(String path, QueryBuilder query, ScoreMode scoreMode) {
        this(path, query, scoreMode, null);
    }

    private NestedQueryBuilder(String path, QueryBuilder query, ScoreMode scoreMode, InnerHitBuilder innerHitBuilder) {
        this.path = requireValue(path, "[" + NAME + "] requires 'path' field");
        this.query = requireValue(query, "[" + NAME + "] requires 'query' field");
        this.scoreMode = requireValue(scoreMode, "[" + NAME + "] requires 'score_mode' field");
        this.innerHitBuilder = innerHitBuilder;
    }

    /**
     * Read from a stream.
     */
    public NestedQueryBuilder(StreamInput in) throws IOException {
        super(in);
        path = in.readString();
        scoreMode = ScoreMode.values()[in.readVInt()];
        query = in.readNamedWriteable(QueryBuilder.class);
        innerHitBuilder = in.readOptionalWriteable(InnerHitBuilder::new);
        ignoreUnmapped = in.readBoolean();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(path);
        out.writeVInt(scoreMode.ordinal());
        out.writeNamedWriteable(query);
        out.writeOptionalWriteable(innerHitBuilder);
        out.writeBoolean(ignoreUnmapped);
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

    public NestedQueryBuilder innerHit(InnerHitBuilder innerHit) {
        this.innerHitBuilder = new InnerHitBuilder(innerHit, path, query);
        return this;
    }

    /**
     * Returns how the scores from the matching child documents are mapped into the nested parent document.
     */
    public ScoreMode scoreMode() {
        return scoreMode;
    }

    /**
     * Sets whether the query builder should ignore unmapped paths (and run a
     * {@link MatchNoDocsQuery} in place of this query) or throw an exception if
     * the path is unmapped.
     */
    public NestedQueryBuilder ignoreUnmapped(boolean ignoreUnmapped) {
        this.ignoreUnmapped = ignoreUnmapped;
        return this;
    }

    /**
     * Gets whether the query builder will ignore unmapped fields (and run a
     * {@link MatchNoDocsQuery} in place of this query) or throw an exception if
     * the path is unmapped.
     */
    public boolean ignoreUnmapped() {
        return ignoreUnmapped;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.field(QUERY_FIELD.getPreferredName());
        query.toXContent(builder, params);
        builder.field(PATH_FIELD.getPreferredName(), path);
        builder.field(IGNORE_UNMAPPED_FIELD.getPreferredName(), ignoreUnmapped);
        if (scoreMode != null) {
            builder.field(SCORE_MODE_FIELD.getPreferredName(), HasChildQueryBuilder.scoreModeAsString(scoreMode));
        }
        printBoostAndQueryName(builder);
        if (innerHitBuilder != null) {
            builder.field(INNER_HITS_FIELD.getPreferredName(), innerHitBuilder, params);
        }
        builder.endObject();
    }

    public static Optional<NestedQueryBuilder> fromXContent(QueryParseContext parseContext) throws IOException {
        XContentParser parser = parseContext.parser();
        float boost = AbstractQueryBuilder.DEFAULT_BOOST;
        ScoreMode scoreMode = ScoreMode.Avg;
        String queryName = null;
        Optional<QueryBuilder> query = Optional.empty();
        String path = null;
        String currentFieldName = null;
        InnerHitBuilder innerHitBuilder = null;
        boolean ignoreUnmapped = DEFAULT_IGNORE_UNMAPPED;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (parseContext.getParseFieldMatcher().match(currentFieldName, QUERY_FIELD)) {
                    query = parseContext.parseInnerQueryBuilder();
                } else if (parseContext.getParseFieldMatcher().match(currentFieldName, INNER_HITS_FIELD)) {
                    innerHitBuilder = InnerHitBuilder.fromXContent(parseContext);
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "[nested] query does not support [" + currentFieldName + "]");
                }
            } else if (token.isValue()) {
                if (parseContext.getParseFieldMatcher().match(currentFieldName, PATH_FIELD)) {
                    path = parser.text();
                } else if (parseContext.getParseFieldMatcher().match(currentFieldName, AbstractQueryBuilder.BOOST_FIELD)) {
                    boost = parser.floatValue();
                } else if (parseContext.getParseFieldMatcher().match(currentFieldName, IGNORE_UNMAPPED_FIELD)) {
                    ignoreUnmapped = parser.booleanValue();
                } else if (parseContext.getParseFieldMatcher().match(currentFieldName, SCORE_MODE_FIELD)) {
                    scoreMode = HasChildQueryBuilder.parseScoreMode(parser.text());
                } else if (parseContext.getParseFieldMatcher().match(currentFieldName, AbstractQueryBuilder.NAME_FIELD)) {
                    queryName = parser.text();
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "[nested] query does not support [" + currentFieldName + "]");
                }
            }
        }

        if (query.isPresent() == false) {
            // if inner query is empty, bubble this up to caller so they can decide how to deal with it
            return Optional.empty();
        }
        NestedQueryBuilder queryBuilder =  new NestedQueryBuilder(path, query.get(), scoreMode)
                .ignoreUnmapped(ignoreUnmapped)
                .queryName(queryName)
                .boost(boost);
        if (innerHitBuilder != null) {
            queryBuilder.innerHit(innerHitBuilder);
        }
        return Optional.of(queryBuilder);
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
                && Objects.equals(innerHitBuilder, that.innerHitBuilder)
                && Objects.equals(ignoreUnmapped, that.ignoreUnmapped);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(query, path, scoreMode, innerHitBuilder, ignoreUnmapped);
    }

    @Override
    protected Query doToQuery(QueryShardContext context) throws IOException {
        ObjectMapper nestedObjectMapper = context.getObjectMapper(path);
        if (nestedObjectMapper == null) {
            if (ignoreUnmapped) {
                return new MatchNoDocsQuery();
            } else {
                throw new IllegalStateException("[" + NAME + "] failed to find nested object under path [" + path + "]");
            }
        }
        if (!nestedObjectMapper.nested().isNested()) {
            throw new IllegalStateException("[" + NAME + "] nested object under path [" + path + "] is not of nested type");
        }
        final BitSetProducer parentFilter;
        final Query childFilter;
        final Query innerQuery;
        ObjectMapper objectMapper = context.nestedScope().getObjectMapper();
        if (objectMapper == null) {
            parentFilter = context.bitsetFilter(Queries.newNonNestedFilter());
        } else {
            parentFilter = context.bitsetFilter(objectMapper.nestedTypeFilter());
        }
        childFilter = nestedObjectMapper.nestedTypeFilter();
        try {
            context.nestedScope().nextLevel(nestedObjectMapper);
            innerQuery = this.query.toQuery(context);
        } finally {
            context.nestedScope().previousLevel();
        }
        return new ToParentBlockJoinQuery(Queries.filtered(innerQuery, childFilter), parentFilter, scoreMode);
    }

    @Override
    protected QueryBuilder doRewrite(QueryRewriteContext queryRewriteContext) throws IOException {
        QueryBuilder rewrite = query.rewrite(queryRewriteContext);
        if (rewrite != query) {
            return new NestedQueryBuilder(path, rewrite, scoreMode, innerHitBuilder);
        }
        return this;
    }

    @Override
    protected void extractInnerHitBuilders(Map<String, InnerHitBuilder> innerHits) {
        if (innerHitBuilder != null) {
            innerHitBuilder.inlineInnerHits(innerHits);
        }
    }
}
