/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.join.query;

import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.index.fielddata.plain.SortedSetOrdinalsIndexFieldData;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.InnerHitBuilder;
import org.elasticsearch.index.query.InnerHitContextBuilder;
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
 * Builder for the 'has_parent' query.
 */
public class HasParentQueryBuilder extends AbstractQueryBuilder<HasParentQueryBuilder> {
    public static final String NAME = "has_parent";

    /**
     * The default value for ignore_unmapped.
     */
    public static final boolean DEFAULT_IGNORE_UNMAPPED = false;

    private static final boolean DEFAULT_SCORE = false;

    private static final ParseField QUERY_FIELD = new ParseField("query");
    private static final ParseField PARENT_TYPE_FIELD = new ParseField("parent_type");
    private static final ParseField SCORE_FIELD = new ParseField("score");
    private static final ParseField INNER_HITS_FIELD = new ParseField("inner_hits");
    private static final ParseField IGNORE_UNMAPPED_FIELD = new ParseField("ignore_unmapped");

    private final QueryBuilder query;
    private final String parentType;
    private final boolean score;
    private InnerHitBuilder innerHitBuilder;
    private boolean ignoreUnmapped = false;

    public HasParentQueryBuilder(String type, QueryBuilder query, boolean score) {
        this(type, query, score, null);
    }

    private HasParentQueryBuilder(String type, QueryBuilder query, boolean score, InnerHitBuilder innerHitBuilder) {
        this.parentType = requireValue(type, "[" + NAME + "] requires '" + PARENT_TYPE_FIELD.getPreferredName() + "' field");
        this.query = requireValue(query, "[" + NAME + "] requires '" + QUERY_FIELD.getPreferredName() + "' field");
        this.score = score;
        this.innerHitBuilder = innerHitBuilder;
    }

    /**
     * Read from a stream.
     */
    public HasParentQueryBuilder(StreamInput in) throws IOException {
        super(in);
        parentType = in.readString();
        score = in.readBoolean();
        query = in.readNamedWriteable(QueryBuilder.class);
        innerHitBuilder = in.readOptionalWriteable(InnerHitBuilder::new);
        ignoreUnmapped = in.readBoolean();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(parentType);
        out.writeBoolean(score);
        out.writeNamedWriteable(query);
        out.writeOptionalWriteable(innerHitBuilder);
        out.writeBoolean(ignoreUnmapped);
    }

    /**
     * Returns the query to execute.
     */
    public QueryBuilder query() {
        return query;
    }

    /**
     * Returns <code>true</code> if the parent score is mapped into the child documents
     */
    public boolean score() {
        return score;
    }

    /**
     * Returns the parents type name
     */
    public String type() {
        return parentType;
    }

    /**
     *  Returns inner hit definition in the scope of this query and reusing the defined type and query.
     */
    public InnerHitBuilder innerHit() {
        return innerHitBuilder;
    }

    public HasParentQueryBuilder innerHit(InnerHitBuilder innerHit) {
        this.innerHitBuilder = innerHit;
        innerHitBuilder.setIgnoreUnmapped(ignoreUnmapped);
        return this;
    }

    /**
     * Sets whether the query builder should ignore unmapped types (and run a
     * {@link MatchNoDocsQuery} in place of this query) or throw an exception if
     * the type is unmapped.
     */
    public HasParentQueryBuilder ignoreUnmapped(boolean ignoreUnmapped) {
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
        if (joiner.parentTypeExists(parentType) == false) {
            if (ignoreUnmapped) {
                return new MatchNoDocsQuery();
            } else {
                throw new QueryShardException(
                    context,
                    "[" + NAME + "] join field [" + joiner.getJoinField() + "] doesn't hold [" + parentType + "] as a parent"
                );
            }
        }

        Query parentFilter = joiner.filter(parentType);
        Query innerQuery = Queries.filtered(query.toQuery(context), parentFilter);
        Query childFilter = joiner.childrenFilter(parentType);
        MappedFieldType fieldType = context.getFieldType(joiner.childJoinField(parentType));
        final SortedSetOrdinalsIndexFieldData fieldData = context.getForField(fieldType, MappedFieldType.FielddataOperation.SEARCH);
        return new HasChildQueryBuilder.LateParsingQuery(
            childFilter,
            innerQuery,
            HasChildQueryBuilder.DEFAULT_MIN_CHILDREN,
            HasChildQueryBuilder.DEFAULT_MAX_CHILDREN,
            fieldType.name(),
            score ? ScoreMode.Max : ScoreMode.None,
            fieldData,
            context.getSearchSimilarity()
        );
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.field(QUERY_FIELD.getPreferredName());
        query.toXContent(builder, params);
        builder.field(PARENT_TYPE_FIELD.getPreferredName(), parentType);
        if (score != DEFAULT_SCORE) {
            builder.field(SCORE_FIELD.getPreferredName(), score);
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

    public static HasParentQueryBuilder fromXContent(XContentParser parser) throws IOException {
        float boost = AbstractQueryBuilder.DEFAULT_BOOST;
        String parentType = null;
        boolean score = DEFAULT_SCORE;
        String queryName = null;
        InnerHitBuilder innerHits = null;
        boolean ignoreUnmapped = DEFAULT_IGNORE_UNMAPPED;

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
                    innerHits = InnerHitBuilder.fromXContent(parser);
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "[has_parent] query does not support [" + currentFieldName + "]");
                }
            } else if (token.isValue()) {
                if (PARENT_TYPE_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    parentType = parser.text();
                } else if (SCORE_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    score = parser.booleanValue();
                } else if (IGNORE_UNMAPPED_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    ignoreUnmapped = parser.booleanValue();
                } else if (AbstractQueryBuilder.BOOST_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    boost = parser.floatValue();
                } else if (AbstractQueryBuilder.NAME_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    queryName = parser.text();
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "[has_parent] query does not support [" + currentFieldName + "]");
                }
            }
        }
        HasParentQueryBuilder queryBuilder = new HasParentQueryBuilder(parentType, iqb, score).ignoreUnmapped(ignoreUnmapped)
            .queryName(queryName)
            .boost(boost);
        if (innerHits != null) {
            queryBuilder.innerHit(innerHits);
        }
        return queryBuilder;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    protected boolean doEquals(HasParentQueryBuilder that) {
        return Objects.equals(query, that.query)
            && Objects.equals(parentType, that.parentType)
            && Objects.equals(score, that.score)
            && Objects.equals(innerHitBuilder, that.innerHitBuilder)
            && Objects.equals(ignoreUnmapped, that.ignoreUnmapped);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(query, parentType, score, innerHitBuilder, ignoreUnmapped);
    }

    @Override
    protected QueryBuilder doRewrite(QueryRewriteContext queryRewriteContext) throws IOException {
        QueryBuilder rewrittenQuery = query.rewrite(queryRewriteContext);
        if (rewrittenQuery != query) {
            HasParentQueryBuilder hasParentQueryBuilder = new HasParentQueryBuilder(parentType, rewrittenQuery, score, innerHitBuilder);
            hasParentQueryBuilder.ignoreUnmapped(ignoreUnmapped);
            return hasParentQueryBuilder;
        }
        return this;
    }

    @Override
    protected void extractInnerHitBuilders(Map<String, InnerHitContextBuilder> innerHits) {
        if (innerHitBuilder != null) {
            String name = innerHitBuilder.getName() != null ? innerHitBuilder.getName() : parentType;
            if (innerHits.containsKey(name)) {
                throw new IllegalArgumentException("[inner_hits] already contains an entry for key [" + name + "]");
            }

            Map<String, InnerHitContextBuilder> children = new HashMap<>();
            InnerHitContextBuilder.extractInnerHits(query, children);
            InnerHitContextBuilder innerHitContextBuilder = new ParentChildInnerHitContextBuilder(
                parentType,
                false,
                query,
                innerHitBuilder,
                children
            );
            innerHits.put(name, innerHitContextBuilder);
        }
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersion.ZERO;
    }
}
