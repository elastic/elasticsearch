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

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopDocsCollector;
import org.apache.lucene.search.TopFieldCollector;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.search.TotalHitCountCollector;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.search.join.ParentChildrenBlockJoinQuery;
import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.Version;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.ObjectMapper;
import org.elasticsearch.index.search.ESToParentBlockJoinQuery;
import org.elasticsearch.index.search.NestedHelper;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.fetch.subphase.InnerHitsContext;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.search.fetch.subphase.InnerHitsContext.intersect;

public class NestedQueryBuilder extends AbstractQueryBuilder<NestedQueryBuilder> {
    public static final String NAME = "nested";
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
        if (out.getVersion().before(Version.V_5_5_0)) {
            final boolean hasInnerHit = innerHitBuilder != null;
            out.writeBoolean(hasInnerHit);
            if (hasInnerHit) {
                innerHitBuilder.writeToNestedBWC(out, query, path);
            }
        } else {
            out.writeOptionalWriteable(innerHitBuilder);
        }
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

    public NestedQueryBuilder innerHit(InnerHitBuilder innerHitBuilder) {
        this.innerHitBuilder = innerHitBuilder;
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
            builder.field(SCORE_MODE_FIELD.getPreferredName(), scoreModeAsString(scoreMode));
        }
        printBoostAndQueryName(builder);
        if (innerHitBuilder != null) {
            builder.field(INNER_HITS_FIELD.getPreferredName(), innerHitBuilder, params);
        }
        builder.endObject();
    }

    public static NestedQueryBuilder fromXContent(XContentParser parser) throws IOException {
        float boost = AbstractQueryBuilder.DEFAULT_BOOST;
        ScoreMode scoreMode = ScoreMode.Avg;
        String queryName = null;
        QueryBuilder query = null;
        String path = null;
        String currentFieldName = null;
        InnerHitBuilder innerHitBuilder = null;
        boolean ignoreUnmapped = DEFAULT_IGNORE_UNMAPPED;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (QUERY_FIELD.match(currentFieldName)) {
                    query = parseInnerQueryBuilder(parser);
                } else if (INNER_HITS_FIELD.match(currentFieldName)) {
                    innerHitBuilder = InnerHitBuilder.fromXContent(parser);
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "[nested] query does not support [" + currentFieldName + "]");
                }
            } else if (token.isValue()) {
                if (PATH_FIELD.match(currentFieldName)) {
                    path = parser.text();
                } else if (AbstractQueryBuilder.BOOST_FIELD.match(currentFieldName)) {
                    boost = parser.floatValue();
                } else if (IGNORE_UNMAPPED_FIELD.match(currentFieldName)) {
                    ignoreUnmapped = parser.booleanValue();
                } else if (SCORE_MODE_FIELD.match(currentFieldName)) {
                    scoreMode = parseScoreMode(parser.text());
                } else if (AbstractQueryBuilder.NAME_FIELD.match(currentFieldName)) {
                    queryName = parser.text();
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "[nested] query does not support [" + currentFieldName + "]");
                }
            }
        }
        NestedQueryBuilder queryBuilder =  new NestedQueryBuilder(path, query, scoreMode, innerHitBuilder)
            .ignoreUnmapped(ignoreUnmapped)
            .queryName(queryName)
            .boost(boost);
        return queryBuilder;
    }

    public static ScoreMode parseScoreMode(String scoreModeString) {
        if ("none".equals(scoreModeString)) {
            return ScoreMode.None;
        } else if ("min".equals(scoreModeString)) {
            return ScoreMode.Min;
        } else if ("max".equals(scoreModeString)) {
            return ScoreMode.Max;
        } else if ("avg".equals(scoreModeString)) {
            return ScoreMode.Avg;
        } else if ("sum".equals(scoreModeString)) {
            return ScoreMode.Total;
        }
        throw new IllegalArgumentException("No score mode for child query [" + scoreModeString + "] found");
    }

    public static String scoreModeAsString(ScoreMode scoreMode) {
        if (scoreMode == ScoreMode.Total) {
            // Lucene uses 'total' but 'sum' is more consistent with other elasticsearch APIs
            return "sum";
        } else {
            return scoreMode.name().toLowerCase(Locale.ROOT);
        }
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
        Query innerQuery;
        ObjectMapper objectMapper = context.nestedScope().getObjectMapper();
        if (objectMapper == null) {
            parentFilter = context.bitsetFilter(Queries.newNonNestedFilter(context.indexVersionCreated()));
        } else {
            parentFilter = context.bitsetFilter(objectMapper.nestedTypeFilter());
        }

        try {
            context.nestedScope().nextLevel(nestedObjectMapper);
            innerQuery = this.query.toQuery(context);
        } finally {
            context.nestedScope().previousLevel();
        }

        // ToParentBlockJoinQuery requires that the inner query only matches documents
        // in its child space
        if (new NestedHelper(context.getMapperService()).mightMatchNonNestedDocs(innerQuery, path)) {
            innerQuery = Queries.filtered(innerQuery, nestedObjectMapper.nestedTypeFilter());
        }

        return new ESToParentBlockJoinQuery(innerQuery, parentFilter, scoreMode,
                objectMapper == null ? null : objectMapper.fullPath());
    }

    @Override
    protected QueryBuilder doRewrite(QueryRewriteContext queryRewriteContext) throws IOException {
        QueryBuilder rewrittenQuery = query.rewrite(queryRewriteContext);
        if (rewrittenQuery != query) {
            NestedQueryBuilder nestedQuery = new NestedQueryBuilder(path, rewrittenQuery, scoreMode, innerHitBuilder);
            nestedQuery.ignoreUnmapped(ignoreUnmapped);
            return nestedQuery;
        }
        return this;
    }

    @Override
    public void extractInnerHitBuilders(Map<String, InnerHitContextBuilder> innerHits) {
        if (innerHitBuilder != null) {
            Map<String, InnerHitContextBuilder> children = new HashMap<>();
            InnerHitContextBuilder.extractInnerHits(query, children);
            InnerHitContextBuilder innerHitContextBuilder = new NestedInnerHitContextBuilder(path, query, innerHitBuilder, children);
            String name = innerHitBuilder.getName() != null ? innerHitBuilder.getName() : path;
            innerHits.put(name, innerHitContextBuilder);
        }
    }

    static class NestedInnerHitContextBuilder extends InnerHitContextBuilder {
        private final String path;

        NestedInnerHitContextBuilder(String path, QueryBuilder query, InnerHitBuilder innerHitBuilder,
                                     Map<String, InnerHitContextBuilder> children) {
            super(query, innerHitBuilder, children);
            this.path = path;
        }

        @Override
        protected void doBuild(SearchContext parentSearchContext,
                          InnerHitsContext innerHitsContext) throws IOException {
            QueryShardContext queryShardContext = parentSearchContext.getQueryShardContext();
            ObjectMapper nestedObjectMapper = queryShardContext.getObjectMapper(path);
            if (nestedObjectMapper == null) {
                if (innerHitBuilder.isIgnoreUnmapped() == false) {
                    throw new IllegalStateException("[" + query.getName() + "] no mapping found for type [" + path + "]");
                } else {
                    return;
                }
            }
            String name =  innerHitBuilder.getName() != null ? innerHitBuilder.getName() : nestedObjectMapper.fullPath();
            ObjectMapper parentObjectMapper = queryShardContext.nestedScope().nextLevel(nestedObjectMapper);
            NestedInnerHitSubContext nestedInnerHits = new NestedInnerHitSubContext(
                name, parentSearchContext, parentObjectMapper, nestedObjectMapper
            );
            setupInnerHitsContext(queryShardContext, nestedInnerHits);
            queryShardContext.nestedScope().previousLevel();
            innerHitsContext.addInnerHitDefinition(nestedInnerHits);
        }
    }

    static final class NestedInnerHitSubContext extends InnerHitsContext.InnerHitSubContext {

        private final ObjectMapper parentObjectMapper;
        private final ObjectMapper childObjectMapper;

        NestedInnerHitSubContext(String name, SearchContext context, ObjectMapper parentObjectMapper, ObjectMapper childObjectMapper) {
            super(name, context);
            this.parentObjectMapper = parentObjectMapper;
            this.childObjectMapper = childObjectMapper;
        }

        @Override
        public TopDocs[] topDocs(SearchHit[] hits) throws IOException {
            Weight innerHitQueryWeight = createInnerHitQueryWeight();
            TopDocs[] result = new TopDocs[hits.length];
            for (int i = 0; i < hits.length; i++) {
                SearchHit hit = hits[i];
                Query rawParentFilter;
                if (parentObjectMapper == null) {
                    rawParentFilter = Queries.newNonNestedFilter(context.indexShard().indexSettings().getIndexVersionCreated());
                } else {
                    rawParentFilter = parentObjectMapper.nestedTypeFilter();
                }

                int parentDocId = hit.docId();
                final int readerIndex = ReaderUtil.subIndex(parentDocId, searcher().getIndexReader().leaves());
                // With nested inner hits the nested docs are always in the same segement, so need to use the other segments
                LeafReaderContext ctx = searcher().getIndexReader().leaves().get(readerIndex);

                Query childFilter = childObjectMapper.nestedTypeFilter();
                BitSetProducer parentFilter = context.bitsetFilterCache().getBitSetProducer(rawParentFilter);
                Query q = new ParentChildrenBlockJoinQuery(parentFilter, childFilter, parentDocId);
                Weight weight = context.searcher().createNormalizedWeight(q, false);
                if (size() == 0) {
                    TotalHitCountCollector totalHitCountCollector = new TotalHitCountCollector();
                    intersect(weight, innerHitQueryWeight, totalHitCountCollector, ctx);
                    result[i] = new TopDocs(totalHitCountCollector.getTotalHits(), Lucene.EMPTY_SCORE_DOCS, 0);
                } else {
                    int topN = Math.min(from() + size(), context.searcher().getIndexReader().maxDoc());
                    TopDocsCollector<?> topDocsCollector;
                    if (sort() != null) {
                        topDocsCollector = TopFieldCollector.create(sort().sort, topN, true, trackScores(), trackScores());
                    } else {
                        topDocsCollector = TopScoreDocCollector.create(topN);
                    }
                    try {
                        intersect(weight, innerHitQueryWeight, topDocsCollector, ctx);
                    } finally {
                        clearReleasables(Lifetime.COLLECTION);
                    }
                    result[i] = topDocsCollector.topDocs(from(), size());
                }
            }
            return result;
        }
    }
}
