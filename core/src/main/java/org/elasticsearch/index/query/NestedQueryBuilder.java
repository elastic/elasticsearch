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
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.object.ObjectMapper;
import org.elasticsearch.index.query.support.QueryInnerHits;
import org.elasticsearch.search.fetch.innerhits.InnerHitsContext;
import org.elasticsearch.search.fetch.innerhits.InnerHitsSubSearchContext;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;

public class NestedQueryBuilder extends AbstractQueryBuilder<NestedQueryBuilder> {

    /**
     * The default score move for nested queries.
     */
    public static final ScoreMode DEFAULT_SCORE_MODE = ScoreMode.Avg;

    /**
     * The queries name used while parsing
     */
    public static final String NAME = "nested";

    private final QueryBuilder query;

    private final String path;

    private ScoreMode scoreMode = DEFAULT_SCORE_MODE;

    private QueryInnerHits queryInnerHits;

    public NestedQueryBuilder(String path, QueryBuilder query) {
        if (path == null) {
            throw new IllegalArgumentException("[" + NAME + "] requires 'path' field");
        }
        if (query == null) {
            throw new IllegalArgumentException("[" + NAME + "] requires 'query' field");
        }
        this.path = path;
        this.query = query;
    }

    public NestedQueryBuilder(String path, QueryBuilder query, ScoreMode scoreMode, QueryInnerHits queryInnerHits) {
        this(path, query);
        scoreMode(scoreMode);
        this.queryInnerHits = queryInnerHits;
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
    public NestedQueryBuilder innerHit(QueryInnerHits innerHit) {
        this.queryInnerHits = innerHit;
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
    public QueryInnerHits innerHit() {
        return queryInnerHits;
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
        builder.field(NestedQueryParser.QUERY_FIELD.getPreferredName());
        query.toXContent(builder, params);
        builder.field(NestedQueryParser.PATH_FIELD.getPreferredName(), path);
        if (scoreMode != null) {
            builder.field(NestedQueryParser.SCORE_MODE_FIELD.getPreferredName(), scoreMode.name().toLowerCase(Locale.ROOT));
        }
        printBoostAndQueryName(builder);
        if (queryInnerHits != null) {
            queryInnerHits.toXContent(builder, params);
        }
        builder.endObject();
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
                && Objects.equals(queryInnerHits, that.queryInnerHits);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(query, path, scoreMode, queryInnerHits);
    }

    private NestedQueryBuilder(StreamInput in) throws IOException {
        path = in.readString();
        final int ordinal = in.readVInt();
        scoreMode = ScoreMode.values()[ordinal];
        query = in.readQuery();
        if (in.readBoolean()) {
            queryInnerHits = new QueryInnerHits(in);
        }
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(path);
        out.writeVInt(scoreMode.ordinal());
        out.writeQuery(query);
        if (queryInnerHits != null) {
            out.writeBoolean(true);
            queryInnerHits.writeTo(out);
        } else {
            out.writeBoolean(false);
        }
    }

    @Override
    protected NestedQueryBuilder doReadFrom(StreamInput in) throws IOException {
        return new NestedQueryBuilder(in);
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
        final ObjectMapper parentObjectMapper;
        final Query innerQuery;
        ObjectMapper objectMapper = context.nestedScope().getObjectMapper();
        try {
            if (objectMapper == null) {
                parentFilter = context.bitsetFilter(Queries.newNonNestedFilter());
            } else {
                parentFilter = context.bitsetFilter(objectMapper.nestedTypeFilter());
            }
            childFilter = nestedObjectMapper.nestedTypeFilter();
            parentObjectMapper = context.nestedScope().nextLevel(nestedObjectMapper);
            innerQuery = this.query.toQuery(context);
            if (innerQuery == null) {
                return null;
            }
        } finally {
            context.nestedScope().previousLevel();
        }

        if (queryInnerHits != null) {
            try (XContentParser parser = queryInnerHits.getXcontentParser()) {
                XContentParser.Token token = parser.nextToken();
                if (token != XContentParser.Token.START_OBJECT) {
                    throw new IllegalStateException("start object expected but was: [" + token + "]");
                }
                InnerHitsSubSearchContext innerHits = context.getInnerHitsContext(parser);
                if (innerHits != null) {
                    ParsedQuery parsedQuery = new ParsedQuery(innerQuery, context.copyNamedQueries());

                    InnerHitsContext.NestedInnerHits nestedInnerHits = new InnerHitsContext.NestedInnerHits(innerHits.getSubSearchContext(), parsedQuery, null, parentObjectMapper, nestedObjectMapper);
                    String name = innerHits.getName() != null ? innerHits.getName() : path;
                    context.addInnerHits(name, nestedInnerHits);
                }
            }
        }
        return new ToParentBlockJoinQuery(Queries.filtered(innerQuery, childFilter), parentFilter, scoreMode);
    }

}
