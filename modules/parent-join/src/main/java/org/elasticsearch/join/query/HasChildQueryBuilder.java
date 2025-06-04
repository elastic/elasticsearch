/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.join.query;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.OrdinalMap;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.join.JoinUtil;
import org.apache.lucene.search.join.ScoreMode;
import org.apache.lucene.search.similarities.Similarity;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.index.fielddata.IndexOrdinalsFieldData;
import org.elasticsearch.index.fielddata.plain.SortedSetOrdinalsIndexFieldData;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.InnerHitBuilder;
import org.elasticsearch.index.query.InnerHitContextBuilder;
import org.elasticsearch.index.query.NestedQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.QueryShardException;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.join.mapper.Joiner;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.search.SearchService.ALLOW_EXPENSIVE_QUERIES;

/**
 * A query builder for {@code has_child} query.
 */
public class HasChildQueryBuilder extends AbstractQueryBuilder<HasChildQueryBuilder> {
    public static final String NAME = "has_child";

    /**
     * The default maximum number of children that are required to match for the parent to be considered a match.
     */
    public static final int DEFAULT_MAX_CHILDREN = Integer.MAX_VALUE;
    /**
     * The default minimum number of children that are required to match for the parent to be considered a match.
     */
    public static final int DEFAULT_MIN_CHILDREN = 1;
    private static final ScoreMode DEFAULT_SCORE_MODE = ScoreMode.None;

    /**
     * The default value for ignore_unmapped.
     */
    public static final boolean DEFAULT_IGNORE_UNMAPPED = false;

    private static final ParseField QUERY_FIELD = new ParseField("query");
    private static final ParseField TYPE_FIELD = new ParseField("type");
    private static final ParseField MAX_CHILDREN_FIELD = new ParseField("max_children");
    private static final ParseField MIN_CHILDREN_FIELD = new ParseField("min_children");
    private static final ParseField SCORE_MODE_FIELD = new ParseField("score_mode");
    private static final ParseField INNER_HITS_FIELD = new ParseField("inner_hits");
    private static final ParseField IGNORE_UNMAPPED_FIELD = new ParseField("ignore_unmapped");

    private final QueryBuilder query;
    private final String type;
    private final ScoreMode scoreMode;
    private InnerHitBuilder innerHitBuilder;
    private int minChildren = DEFAULT_MIN_CHILDREN;
    private int maxChildren = DEFAULT_MAX_CHILDREN;
    private boolean ignoreUnmapped = DEFAULT_IGNORE_UNMAPPED;

    public HasChildQueryBuilder(String type, QueryBuilder query, ScoreMode scoreMode) {
        this(type, query, DEFAULT_MIN_CHILDREN, DEFAULT_MAX_CHILDREN, scoreMode, null);
    }

    private HasChildQueryBuilder(
        String type,
        QueryBuilder query,
        int minChildren,
        int maxChildren,
        ScoreMode scoreMode,
        InnerHitBuilder innerHitBuilder
    ) {
        this.type = requireValue(type, "[" + NAME + "] requires 'type' field");
        this.query = requireValue(query, "[" + NAME + "] requires 'query' field");
        this.scoreMode = requireValue(scoreMode, "[" + NAME + "] requires 'score_mode' field");
        this.innerHitBuilder = innerHitBuilder;
        this.minChildren = minChildren;
        this.maxChildren = maxChildren;
    }

    /**
     * Read from a stream.
     */
    public HasChildQueryBuilder(StreamInput in) throws IOException {
        super(in);
        type = in.readString();
        minChildren = in.readInt();
        maxChildren = in.readInt();
        scoreMode = ScoreMode.values()[in.readVInt()];
        query = in.readNamedWriteable(QueryBuilder.class);
        innerHitBuilder = in.readOptionalWriteable(InnerHitBuilder::new);
        ignoreUnmapped = in.readBoolean();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(type);
        out.writeInt(minChildren);
        out.writeInt(maxChildren);
        out.writeVInt(scoreMode.ordinal());
        out.writeNamedWriteable(query);
        out.writeOptionalWriteable(innerHitBuilder);
        out.writeBoolean(ignoreUnmapped);
    }

    /**
     * Defines the minimum number of children that are required to match for the parent to be considered a match and
     * the maximum number of children that are required to match for the parent to be considered a match.
     */
    public HasChildQueryBuilder minMaxChildren(int min, int max) {
        if (min <= 0) {
            throw new IllegalArgumentException("[" + NAME + "] requires positive 'min_children' field");
        }
        if (max <= 0) {
            throw new IllegalArgumentException("[" + NAME + "] requires positive 'max_children' field");
        }
        if (max < min) {
            throw new IllegalArgumentException("[" + NAME + "] 'max_children' is less than 'min_children'");
        }
        this.minChildren = min;
        this.maxChildren = max;
        return this;
    }

    /**
     * Returns inner hit definition in the scope of this query and reusing the defined type and query.
     */
    public InnerHitBuilder innerHit() {
        return innerHitBuilder;
    }

    public HasChildQueryBuilder innerHit(InnerHitBuilder innerHit) {
        this.innerHitBuilder = innerHit;
        innerHitBuilder.setIgnoreUnmapped(ignoreUnmapped);
        return this;
    }

    /**
     * Returns the children query to execute.
     */
    public QueryBuilder query() {
        return query;
    }

    /**
     * Returns the child type
     */
    public String childType() {
        return type;
    }

    /**
     * Returns how the scores from the matching child documents are mapped into the parent document.
     */
    public ScoreMode scoreMode() {
        return scoreMode;
    }

    /**
     * Returns the minimum number of children that are required to match for the parent to be considered a match.
     * The default is {@value #DEFAULT_MIN_CHILDREN}
     */
    public int minChildren() {
        return minChildren;
    }

    /**
     * Returns the maximum number of children that are required to match for the parent to be considered a match.
     * The default is {@value #DEFAULT_MAX_CHILDREN}
     */
    public int maxChildren() {
        return maxChildren;
    }

    /**
     * Sets whether the query builder should ignore unmapped types (and run a
     * {@link MatchNoDocsQuery} in place of this query) or throw an exception if
     * the type is unmapped.
     */
    public HasChildQueryBuilder ignoreUnmapped(boolean ignoreUnmapped) {
        this.ignoreUnmapped = ignoreUnmapped;
        if (innerHitBuilder != null) {
            innerHitBuilder.setIgnoreUnmapped(ignoreUnmapped);
        }
        return this;
    }

    /**
     * Gets whether the query builder will ignore unmapped types (and run a
     * {@link MatchNoDocsQuery} in place of this query) or throw an exception if
     * the type is unmapped.
     */
    public boolean ignoreUnmapped() {
        return ignoreUnmapped;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.field(QUERY_FIELD.getPreferredName());
        query.toXContent(builder, params);
        builder.field(TYPE_FIELD.getPreferredName(), type);
        if (false == scoreMode.equals(DEFAULT_SCORE_MODE)) {
            builder.field(SCORE_MODE_FIELD.getPreferredName(), NestedQueryBuilder.scoreModeAsString(scoreMode));
        }
        if (minChildren != DEFAULT_MIN_CHILDREN) {
            builder.field(MIN_CHILDREN_FIELD.getPreferredName(), minChildren);
        }
        if (maxChildren != DEFAULT_MAX_CHILDREN) {
            builder.field(MAX_CHILDREN_FIELD.getPreferredName(), maxChildren);
        }
        if (ignoreUnmapped != DEFAULT_IGNORE_UNMAPPED) {
            builder.field(IGNORE_UNMAPPED_FIELD.getPreferredName(), ignoreUnmapped);
        }
        boostAndQueryNameToXContent(builder);
        if (innerHitBuilder != null) {
            builder.field(INNER_HITS_FIELD.getPreferredName(), innerHitBuilder, params);
        }
        builder.endObject();
    }

    public static HasChildQueryBuilder fromXContent(XContentParser parser) throws IOException {
        float boost = AbstractQueryBuilder.DEFAULT_BOOST;
        String childType = null;
        ScoreMode scoreMode = DEFAULT_SCORE_MODE;
        int minChildren = HasChildQueryBuilder.DEFAULT_MIN_CHILDREN;
        int maxChildren = HasChildQueryBuilder.DEFAULT_MAX_CHILDREN;
        boolean ignoreUnmapped = DEFAULT_IGNORE_UNMAPPED;
        String queryName = null;
        InnerHitBuilder innerHitBuilder = null;
        String currentFieldName = null;
        XContentParser.Token token;
        QueryBuilder iqb = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (QUERY_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    iqb = parseInnerQueryBuilder(parser);
                } else if (INNER_HITS_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    innerHitBuilder = InnerHitBuilder.fromXContent(parser);
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "[has_child] query does not support [" + currentFieldName + "]");
                }
            } else if (token.isValue()) {
                if (TYPE_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    childType = parser.text();
                } else if (SCORE_MODE_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    scoreMode = NestedQueryBuilder.parseScoreMode(parser.text());
                } else if (AbstractQueryBuilder.BOOST_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    boost = parser.floatValue();
                } else if (MIN_CHILDREN_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    minChildren = parser.intValue(true);
                } else if (MAX_CHILDREN_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    maxChildren = parser.intValue(true);
                } else if (IGNORE_UNMAPPED_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    ignoreUnmapped = parser.booleanValue();
                } else if (AbstractQueryBuilder.NAME_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    queryName = parser.text();
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "[has_child] query does not support [" + currentFieldName + "]");
                }
            }
        }
        HasChildQueryBuilder hasChildQueryBuilder = new HasChildQueryBuilder(childType, iqb, scoreMode);
        hasChildQueryBuilder.minMaxChildren(minChildren, maxChildren);
        hasChildQueryBuilder.queryName(queryName);
        hasChildQueryBuilder.boost(boost);
        hasChildQueryBuilder.ignoreUnmapped(ignoreUnmapped);
        if (innerHitBuilder != null) {
            hasChildQueryBuilder.innerHit(innerHitBuilder);
        }
        return hasChildQueryBuilder;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    protected Query doToQuery(SearchExecutionContext context) throws IOException {
        if (context.allowExpensiveQueries() == false) {
            throw new ElasticsearchException(
                "[joining] queries cannot be executed when '" + ALLOW_EXPENSIVE_QUERIES.getKey() + "' is set to false."
            );
        }

        Joiner joiner = Joiner.getJoiner(context);
        if (joiner == null) {
            if (ignoreUnmapped) {
                return new MatchNoDocsQuery();
            } else {
                throw new QueryShardException(context, "[" + NAME + "] no join field has been configured");
            }
        }

        if (joiner.childTypeExists(type) == false) {
            if (ignoreUnmapped) {
                return new MatchNoDocsQuery();
            } else {
                throw new QueryShardException(
                    context,
                    "[" + NAME + "] join field [" + joiner.getJoinField() + "] doesn't hold [" + type + "] as a child"
                );
            }
        }

        String parentJoinField = joiner.parentJoinField(type);
        if (context.isFieldMapped(parentJoinField) == false) {
            if (ignoreUnmapped) {
                return new MatchNoDocsQuery();
            }
            throw new QueryShardException(context, "[" + NAME + "] no parent join field [" + parentJoinField + "] configured");
        }

        Query parentFilter = joiner.parentFilter(type);
        Query childFilter = joiner.filter(type);
        Query filteredQuery = Queries.filtered(query.toQuery(context), childFilter);
        MappedFieldType ft = context.getFieldType(parentJoinField);
        final SortedSetOrdinalsIndexFieldData fieldData = context.getForField(ft, MappedFieldType.FielddataOperation.SEARCH);
        return new LateParsingQuery(
            parentFilter,
            filteredQuery,
            minChildren,
            maxChildren,
            parentJoinField,
            scoreMode,
            fieldData,
            context.getSearchSimilarity()
        );
    }

    /**
     * A query that rewrites into another query using
     * {@link JoinUtil#createJoinQuery(String, Query, Query, IndexSearcher, ScoreMode, OrdinalMap, int, int)}
     * that executes the actual join.
     * <p>
     * This query is exclusively used by the {@link HasChildQueryBuilder} and {@link HasParentQueryBuilder} to get access
     * to the {@link DirectoryReader} used by the current search in order to retrieve the {@link OrdinalMap}.
     * The {@link OrdinalMap} is required by {@link JoinUtil} to execute the join.
     */
    // TODO: Find a way to remove this query and let doToQuery(...) just return the query from JoinUtil.createJoinQuery(...)
    public static final class LateParsingQuery extends Query {

        private final Query toQuery;
        private final Query innerQuery;
        private final int minChildren;
        private final int maxChildren;
        private final String joinField;
        private final ScoreMode scoreMode;
        private final SortedSetOrdinalsIndexFieldData fieldDataJoin;
        private final Similarity similarity;

        LateParsingQuery(
            Query toQuery,
            Query innerQuery,
            int minChildren,
            int maxChildren,
            String joinField,
            ScoreMode scoreMode,
            SortedSetOrdinalsIndexFieldData fieldData,
            Similarity similarity
        ) {
            this.toQuery = toQuery;
            this.innerQuery = innerQuery;
            this.minChildren = minChildren;
            this.maxChildren = maxChildren;
            this.joinField = joinField;
            this.scoreMode = scoreMode;
            this.fieldDataJoin = fieldData;
            this.similarity = similarity;
        }

        @Override
        public Query rewrite(IndexSearcher searcher) throws IOException {
            Query rewritten = super.rewrite(searcher);
            if (rewritten != this) {
                return rewritten;
            }
            IndexReader reader = searcher.getIndexReader();
            if (reader instanceof DirectoryReader) {
                IndexSearcher indexSearcher = new IndexSearcher(reader);
                indexSearcher.setQueryCache(null);
                indexSearcher.setSimilarity(similarity);
                IndexOrdinalsFieldData indexParentChildFieldData = fieldDataJoin.loadGlobal((DirectoryReader) reader);
                OrdinalMap ordinalMap = indexParentChildFieldData.getOrdinalMap();
                return JoinUtil.createJoinQuery(
                    joinField,
                    innerQuery,
                    toQuery,
                    indexSearcher,
                    scoreMode,
                    ordinalMap,
                    minChildren,
                    maxChildren
                );
            } else {
                if (reader.leaves().isEmpty() && reader.numDocs() == 0) {
                    // asserting reader passes down a MultiReader during rewrite which makes this
                    // blow up since for this query to work we have to have a DirectoryReader otherwise
                    // we can't load global ordinals - for this to work we simply check if the reader has no leaves
                    // and rewrite to match nothing
                    return new MatchNoDocsQuery("Can't load against an empty reader");
                }
                throw new IllegalStateException(
                    "can't load global ordinals for reader of type: " + searcher.getClass() + " must be a DirectoryReader"
                );
            }
        }

        @Override
        public boolean equals(Object o) {
            if (sameClassAs(o) == false) return false;

            LateParsingQuery that = (LateParsingQuery) o;

            if (minChildren != that.minChildren) return false;
            if (maxChildren != that.maxChildren) return false;
            if (toQuery.equals(that.toQuery) == false) return false;
            if (innerQuery.equals(that.innerQuery) == false) return false;
            if (joinField.equals(that.joinField) == false) return false;
            return scoreMode == that.scoreMode;
        }

        @Override
        public int hashCode() {
            return Objects.hash(getClass(), toQuery, innerQuery, minChildren, maxChildren, joinField, scoreMode);
        }

        @Override
        public String toString(String s) {
            return "LateParsingQuery {joinField=" + joinField + "}";
        }

        @Override
        public void visit(QueryVisitor visitor) {
            visitor.visitLeaf(this);
        }

        public int getMinChildren() {
            return minChildren;
        }

        public int getMaxChildren() {
            return maxChildren;
        }

        public ScoreMode getScoreMode() {
            return scoreMode;
        }

        public Query getInnerQuery() {
            return innerQuery;
        }

        public Similarity getSimilarity() {
            return similarity;
        }
    }

    @Override
    protected boolean doEquals(HasChildQueryBuilder that) {
        return Objects.equals(query, that.query)
            && Objects.equals(type, that.type)
            && Objects.equals(scoreMode, that.scoreMode)
            && Objects.equals(minChildren, that.minChildren)
            && Objects.equals(maxChildren, that.maxChildren)
            && Objects.equals(innerHitBuilder, that.innerHitBuilder)
            && Objects.equals(ignoreUnmapped, that.ignoreUnmapped);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(query, type, scoreMode, minChildren, maxChildren, innerHitBuilder, ignoreUnmapped);
    }

    @Override
    protected QueryBuilder doRewrite(QueryRewriteContext queryRewriteContext) throws IOException {
        QueryBuilder rewrittenQuery = query.rewrite(queryRewriteContext);
        if (rewrittenQuery != query) {
            HasChildQueryBuilder hasChildQueryBuilder = new HasChildQueryBuilder(
                type,
                rewrittenQuery,
                minChildren,
                maxChildren,
                scoreMode,
                innerHitBuilder
            );
            hasChildQueryBuilder.ignoreUnmapped(ignoreUnmapped);
            return hasChildQueryBuilder;
        }
        return this;
    }

    @Override
    protected void extractInnerHitBuilders(Map<String, InnerHitContextBuilder> innerHits) {
        if (innerHitBuilder != null) {
            String name = innerHitBuilder.getName() != null ? innerHitBuilder.getName() : type;
            if (innerHits.containsKey(name)) {
                throw new IllegalArgumentException("[inner_hits] already contains an entry for key [" + name + "]");
            }

            Map<String, InnerHitContextBuilder> children = new HashMap<>();
            InnerHitContextBuilder.extractInnerHits(query, children);
            InnerHitContextBuilder innerHitContextBuilder = new ParentChildInnerHitContextBuilder(
                type,
                true,
                query,
                innerHitBuilder,
                children
            );
            innerHits.put(name, innerHitContextBuilder);
        }
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.ZERO;
    }
}
