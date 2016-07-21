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

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.fielddata.plain.ParentChildIndexFieldData;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.internal.ParentFieldMapper;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Builder for the 'has_parent' query.
 */
public class HasParentQueryBuilder extends AbstractQueryBuilder<HasParentQueryBuilder> {
    public static final String NAME = "has_parent";

    /**
     * The default value for ignore_unmapped.
     */
    public static final boolean DEFAULT_IGNORE_UNMAPPED = false;

    private static final ParseField QUERY_FIELD = new ParseField("query", "filter");
    private static final ParseField SCORE_MODE_FIELD = new ParseField("score_mode").withAllDeprecated("score");
    private static final ParseField TYPE_FIELD = new ParseField("parent_type", "type");
    private static final ParseField SCORE_FIELD = new ParseField("score");
    private static final ParseField INNER_HITS_FIELD = new ParseField("inner_hits");
    private static final ParseField IGNORE_UNMAPPED_FIELD = new ParseField("ignore_unmapped");

    private final QueryBuilder query;
    private final String type;
    private final boolean score;
    private InnerHitBuilder innerHit;
    private boolean ignoreUnmapped = false;

    public HasParentQueryBuilder(String type, QueryBuilder query, boolean score) {
        this(type, query, score, null);
    }

    private HasParentQueryBuilder(String type, QueryBuilder query, boolean score, InnerHitBuilder innerHit) {
        this.type = requireValue(type, "[" + NAME + "] requires 'type' field");
        this.query = requireValue(query, "[" + NAME + "] requires 'query' field");
        this.score = score;
        this.innerHit = innerHit;
    }

    /**
     * Read from a stream.
     */
    public HasParentQueryBuilder(StreamInput in) throws IOException {
        super(in);
        type = in.readString();
        score = in.readBoolean();
        query = in.readNamedWriteable(QueryBuilder.class);
        innerHit = in.readOptionalWriteable(InnerHitBuilder::new);
        ignoreUnmapped = in.readBoolean();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(type);
        out.writeBoolean(score);
        out.writeNamedWriteable(query);
        out.writeOptionalWriteable(innerHit);
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
        return type;
    }

    /**
     *  Returns inner hit definition in the scope of this query and reusing the defined type and query.
     */
    public InnerHitBuilder innerHit() {
        return innerHit;
    }

    public HasParentQueryBuilder innerHit(InnerHitBuilder innerHit) {
        this.innerHit = new InnerHitBuilder(innerHit, query, type);
        return this;
    }

    /**
     * Sets whether the query builder should ignore unmapped types (and run a
     * {@link MatchNoDocsQuery} in place of this query) or throw an exception if
     * the type is unmapped.
     */
    public HasParentQueryBuilder ignoreUnmapped(boolean ignoreUnmapped) {
        this.ignoreUnmapped = ignoreUnmapped;
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
    protected Query doToQuery(QueryShardContext context) throws IOException {
        Query innerQuery;
        String[] previousTypes = context.getTypes();
        context.setTypes(type);
        try {
            innerQuery = query.toQuery(context);
        } finally {
            context.setTypes(previousTypes);
        }

        DocumentMapper parentDocMapper = context.getMapperService().documentMapper(type);
        if (parentDocMapper == null) {
            if (ignoreUnmapped) {
                return new MatchNoDocsQuery();
            } else {
                throw new QueryShardException(context, "[" + NAME + "] query configured 'parent_type' [" + type + "] is not a valid type");
            }
        }

        Set<String> childTypes = new HashSet<>();
        ParentChildIndexFieldData parentChildIndexFieldData = null;
        for (DocumentMapper documentMapper : context.getMapperService().docMappers(false)) {
            ParentFieldMapper parentFieldMapper = documentMapper.parentFieldMapper();
            if (parentFieldMapper.active() && type.equals(parentFieldMapper.type())) {
                childTypes.add(documentMapper.type());
                parentChildIndexFieldData = context.getForField(parentFieldMapper.fieldType());
            }
        }

        if (childTypes.isEmpty()) {
            throw new QueryShardException(context, "[" + NAME + "] no child types found for type [" + type + "]");
        }

        Query childrenQuery;
        if (childTypes.size() == 1) {
            DocumentMapper documentMapper = context.getMapperService().documentMapper(childTypes.iterator().next());
            childrenQuery = documentMapper.typeFilter();
        } else {
            BooleanQuery.Builder childrenFilter = new BooleanQuery.Builder();
            for (String childrenTypeStr : childTypes) {
                DocumentMapper documentMapper = context.getMapperService().documentMapper(childrenTypeStr);
                childrenFilter.add(documentMapper.typeFilter(), BooleanClause.Occur.SHOULD);
            }
            childrenQuery = childrenFilter.build();
        }

        // wrap the query with type query
        innerQuery = Queries.filtered(innerQuery, parentDocMapper.typeFilter());
        return new HasChildQueryBuilder.LateParsingQuery(childrenQuery,
                                                         innerQuery,
                                                         HasChildQueryBuilder.DEFAULT_MIN_CHILDREN,
                                                         HasChildQueryBuilder.DEFAULT_MAX_CHILDREN,
                                                         type,
                                                         score ? ScoreMode.Max : ScoreMode.None,
                                                         parentChildIndexFieldData,
                                                         context.getSearchSimilarity());
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.field(QUERY_FIELD.getPreferredName());
        query.toXContent(builder, params);
        builder.field(TYPE_FIELD.getPreferredName(), type);
        builder.field(SCORE_FIELD.getPreferredName(), score);
        builder.field(IGNORE_UNMAPPED_FIELD.getPreferredName(), ignoreUnmapped);
        printBoostAndQueryName(builder);
        if (innerHit != null) {
            builder.field(INNER_HITS_FIELD.getPreferredName(), innerHit, params);
        }
        builder.endObject();
    }

    public static Optional<HasParentQueryBuilder> fromXContent(QueryParseContext parseContext) throws IOException {
        XContentParser parser = parseContext.parser();
        float boost = AbstractQueryBuilder.DEFAULT_BOOST;
        String parentType = null;
        boolean score = false;
        String queryName = null;
        InnerHitBuilder innerHits = null;
        boolean ignoreUnmapped = DEFAULT_IGNORE_UNMAPPED;

        String currentFieldName = null;
        XContentParser.Token token;
        Optional<QueryBuilder> iqb = Optional.empty();
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (parseContext.getParseFieldMatcher().match(currentFieldName, QUERY_FIELD)) {
                    iqb = parseContext.parseInnerQueryBuilder();
                } else if (parseContext.getParseFieldMatcher().match(currentFieldName, INNER_HITS_FIELD)) {
                    innerHits = InnerHitBuilder.fromXContent(parseContext);
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "[has_parent] query does not support [" + currentFieldName + "]");
                }
            } else if (token.isValue()) {
                if (parseContext.getParseFieldMatcher().match(currentFieldName, TYPE_FIELD)) {
                    parentType = parser.text();
                } else if (parseContext.getParseFieldMatcher().match(currentFieldName, SCORE_MODE_FIELD)) {
                    String scoreModeValue = parser.text();
                    if ("score".equals(scoreModeValue)) {
                        score = true;
                    } else if ("none".equals(scoreModeValue)) {
                        score = false;
                    } else {
                        throw new ParsingException(parser.getTokenLocation(), "[has_parent] query does not support [" +
                                scoreModeValue + "] as an option for score_mode");
                    }
                } else if (parseContext.getParseFieldMatcher().match(currentFieldName, SCORE_FIELD)) {
                    score = parser.booleanValue();
                } else if (parseContext.getParseFieldMatcher().match(currentFieldName, IGNORE_UNMAPPED_FIELD)) {
                    ignoreUnmapped = parser.booleanValue();
                } else if (parseContext.getParseFieldMatcher().match(currentFieldName, AbstractQueryBuilder.BOOST_FIELD)) {
                    boost = parser.floatValue();
                } else if (parseContext.getParseFieldMatcher().match(currentFieldName, AbstractQueryBuilder.NAME_FIELD)) {
                    queryName = parser.text();
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "[has_parent] query does not support [" + currentFieldName + "]");
                }
            }
        }
        if (iqb.isPresent() == false) {
            // if inner query is empty, bubble this up to caller so they can decide how to deal with it
            return Optional.empty();
        }
        HasParentQueryBuilder queryBuilder =  new HasParentQueryBuilder(parentType, iqb.get(), score)
                .ignoreUnmapped(ignoreUnmapped)
                .queryName(queryName)
                .boost(boost);
        if (innerHits != null) {
            queryBuilder.innerHit(innerHits);
        }
        return Optional.of(queryBuilder);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    protected boolean doEquals(HasParentQueryBuilder that) {
        return Objects.equals(query, that.query)
                && Objects.equals(type, that.type)
                && Objects.equals(score, that.score)
                && Objects.equals(innerHit, that.innerHit)
                && Objects.equals(ignoreUnmapped, that.ignoreUnmapped);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(query, type, score, innerHit, ignoreUnmapped);
    }

    @Override
    protected QueryBuilder doRewrite(QueryRewriteContext queryShardContext) throws IOException {
        QueryBuilder rewrittenQuery = query.rewrite(queryShardContext);
        if (rewrittenQuery != query) {
            InnerHitBuilder rewrittenInnerHit = InnerHitBuilder.rewrite(innerHit, rewrittenQuery);
            return new HasParentQueryBuilder(type, rewrittenQuery, score, rewrittenInnerHit);
        }
        return this;
    }

    @Override
    protected void extractInnerHitBuilders(Map<String, InnerHitBuilder> innerHits) {
        if (innerHit!= null) {
            innerHit.inlineInnerHits(innerHits);
        }
    }
}
