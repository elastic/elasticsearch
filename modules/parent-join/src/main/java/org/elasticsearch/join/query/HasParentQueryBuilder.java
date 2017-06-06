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
package org.elasticsearch.join.query;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.DocValuesTermsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopDocsCollector;
import org.apache.lucene.search.TopFieldCollector;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.search.TotalHitCountCollector;
import org.apache.lucene.search.Weight;
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
import org.elasticsearch.index.fielddata.plain.SortedSetDVOrdinalsIndexFieldData;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.ParentFieldMapper;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.InnerHitBuilder;
import org.elasticsearch.index.query.InnerHitContextBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.query.QueryShardException;
import org.elasticsearch.join.mapper.ParentIdFieldMapper;
import org.elasticsearch.join.mapper.ParentJoinFieldMapper;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.fetch.subphase.InnerHitsContext;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.elasticsearch.search.fetch.subphase.InnerHitsContext.intersect;

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
    private InnerHitBuilder innerHitBuilder;
    private boolean ignoreUnmapped = false;

    public HasParentQueryBuilder(String type, QueryBuilder query, boolean score) {
        this(type, query, score, null);
    }

    private HasParentQueryBuilder(String type, QueryBuilder query, boolean score, InnerHitBuilder innerHitBuilder) {
        this.type = requireValue(type, "[" + NAME + "] requires 'type' field");
        this.query = requireValue(query, "[" + NAME + "] requires 'query' field");
        this.score = score;
        this.innerHitBuilder = innerHitBuilder;
    }

    /**
     * Read from a stream.
     */
    public HasParentQueryBuilder(StreamInput in) throws IOException {
        super(in);
        type = in.readString();
        score = in.readBoolean();
        query = in.readNamedWriteable(QueryBuilder.class);
        innerHitBuilder = in.readOptionalWriteable(InnerHitBuilder::new);
        ignoreUnmapped = in.readBoolean();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(type);
        out.writeBoolean(score);
        out.writeNamedWriteable(query);
        if (out.getVersion().before(Version.V_5_5_0)) {
            final boolean hasInnerHit = innerHitBuilder != null;
            out.writeBoolean(hasInnerHit);
            if (hasInnerHit) {
                innerHitBuilder.writeToParentChildBWC(out, query, type);
            }
        } else {
            out.writeOptionalWriteable(innerHitBuilder);
        }
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
        return innerHitBuilder;
    }

    public HasParentQueryBuilder innerHit(InnerHitBuilder innerHit) {
        this.innerHitBuilder = innerHit;
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
        if (context.getIndexSettings().isSingleType()) {
            return joinFieldDoToQuery(context);
        } else  {
            return parentFieldDoToQuery(context);
        }
    }

    private Query joinFieldDoToQuery(QueryShardContext context) throws IOException {
        ParentJoinFieldMapper joinFieldMapper = ParentJoinFieldMapper.getMapper(context.getMapperService());
        ParentIdFieldMapper parentIdFieldMapper = joinFieldMapper.getParentIdFieldMapper(type, true);
        if (parentIdFieldMapper != null) {
            Query parentFilter = parentIdFieldMapper.getParentFilter();
            Query innerQuery = Queries.filtered(query.toQuery(context), parentFilter);
            Query childFilter = parentIdFieldMapper.getChildrenFilter();
            MappedFieldType fieldType = parentIdFieldMapper.fieldType();
            final SortedSetDVOrdinalsIndexFieldData fieldData = context.getForField(fieldType);
            return new HasChildQueryBuilder.LateParsingQuery(childFilter, innerQuery,
                HasChildQueryBuilder.DEFAULT_MIN_CHILDREN, HasChildQueryBuilder.DEFAULT_MAX_CHILDREN,
                fieldType.name(), score ? ScoreMode.Max : ScoreMode.None, fieldData, context.getSearchSimilarity());
        } else {
            if (ignoreUnmapped) {
                return new MatchNoDocsQuery();
            } else {
                throw new QueryShardException(context, "[" + NAME + "] join field has no parent type configured");
            }
        }
    }

    private Query parentFieldDoToQuery(QueryShardContext context) throws IOException {
        Query innerQuery;
        String[] previousTypes = context.getTypes();
        context.setTypes(type);
        try {
            innerQuery = query.toQuery(context);
        } finally {
            context.setTypes(previousTypes);
        }

        DocumentMapper parentDocMapper = context.documentMapper(type);
        if (parentDocMapper == null) {
            if (ignoreUnmapped) {
                return new MatchNoDocsQuery();
            } else {
                throw new QueryShardException(context,
                    "[" + NAME + "] query configured 'parent_type' [" + type + "] is not a valid type");
            }
        }

        Set<String> childTypes = new HashSet<>();
        for (DocumentMapper documentMapper : context.getMapperService().docMappers(false)) {
            ParentFieldMapper parentFieldMapper = documentMapper.parentFieldMapper();
            if (parentFieldMapper.active() && type.equals(parentFieldMapper.type())) {
                childTypes.add(documentMapper.type());
            }
        }
        if (childTypes.isEmpty()) {
            throw new QueryShardException(context, "[" + NAME + "] no child types found for type [" + type + "]");
        }

        Query childrenQuery;
        if (childTypes.size() == 1) {
            DocumentMapper documentMapper = context.getMapperService().documentMapper(childTypes.iterator().next());
            childrenQuery = documentMapper.typeFilter(context);
        } else {
            BooleanQuery.Builder childrenFilter = new BooleanQuery.Builder();
            for (String childrenTypeStr : childTypes) {
                DocumentMapper documentMapper = context.getMapperService().documentMapper(childrenTypeStr);
                childrenFilter.add(documentMapper.typeFilter(context), BooleanClause.Occur.SHOULD);
            }
            childrenQuery = childrenFilter.build();
        }

        // wrap the query with type query
        innerQuery = Queries.filtered(innerQuery, parentDocMapper.typeFilter(context));

        final MappedFieldType parentType = parentDocMapper.parentFieldMapper().getParentJoinFieldType();
        final SortedSetDVOrdinalsIndexFieldData fieldData = context.getForField(parentType);
        return new HasChildQueryBuilder.LateParsingQuery(childrenQuery,
            innerQuery,
            HasChildQueryBuilder.DEFAULT_MIN_CHILDREN,
            HasChildQueryBuilder.DEFAULT_MAX_CHILDREN,
            ParentFieldMapper.joinField(type),
            score ? ScoreMode.Max : ScoreMode.None,
            fieldData,
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
        if (innerHitBuilder != null) {
            builder.field(INNER_HITS_FIELD.getPreferredName(), innerHitBuilder, params);
        }
        builder.endObject();
    }

    public static HasParentQueryBuilder fromXContent(QueryParseContext parseContext) throws IOException {
        XContentParser parser = parseContext.parser();
        float boost = AbstractQueryBuilder.DEFAULT_BOOST;
        String parentType = null;
        boolean score = false;
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
                if (QUERY_FIELD.match(currentFieldName)) {
                    iqb = parseContext.parseInnerQueryBuilder();
                } else if (INNER_HITS_FIELD.match(currentFieldName)) {
                    innerHits = InnerHitBuilder.fromXContent(parseContext);
                } else {
                    throw new ParsingException(parser.getTokenLocation(),
                        "[has_parent] query does not support [" + currentFieldName + "]");
                }
            } else if (token.isValue()) {
                if (TYPE_FIELD.match(currentFieldName)) {
                    parentType = parser.text();
                } else if (SCORE_MODE_FIELD.match(currentFieldName)) {
                    String scoreModeValue = parser.text();
                    if ("score".equals(scoreModeValue)) {
                        score = true;
                    } else if ("none".equals(scoreModeValue)) {
                        score = false;
                    } else {
                        throw new ParsingException(parser.getTokenLocation(), "[has_parent] query does not support [" +
                                scoreModeValue + "] as an option for score_mode");
                    }
                } else if (SCORE_FIELD.match(currentFieldName)) {
                    score = parser.booleanValue();
                } else if (IGNORE_UNMAPPED_FIELD.match(currentFieldName)) {
                    ignoreUnmapped = parser.booleanValue();
                } else if (AbstractQueryBuilder.BOOST_FIELD.match(currentFieldName)) {
                    boost = parser.floatValue();
                } else if (AbstractQueryBuilder.NAME_FIELD.match(currentFieldName)) {
                    queryName = parser.text();
                } else {
                    throw new ParsingException(parser.getTokenLocation(),
                        "[has_parent] query does not support [" + currentFieldName + "]");
                }
            }
        }
        HasParentQueryBuilder queryBuilder =  new HasParentQueryBuilder(parentType, iqb, score)
                .ignoreUnmapped(ignoreUnmapped)
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
                && Objects.equals(type, that.type)
                && Objects.equals(score, that.score)
                && Objects.equals(innerHitBuilder, that.innerHitBuilder)
                && Objects.equals(ignoreUnmapped, that.ignoreUnmapped);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(query, type, score, innerHitBuilder, ignoreUnmapped);
    }

    @Override
    protected QueryBuilder doRewrite(QueryRewriteContext queryShardContext) throws IOException {
        QueryBuilder rewrittenQuery = query.rewrite(queryShardContext);
        if (rewrittenQuery != query) {
            HasParentQueryBuilder hasParentQueryBuilder = new HasParentQueryBuilder(type, rewrittenQuery, score, innerHitBuilder);
            hasParentQueryBuilder.ignoreUnmapped(ignoreUnmapped);
            return hasParentQueryBuilder;
        }
        return this;
    }

    @Override
    protected void extractInnerHitBuilders(Map<String, InnerHitContextBuilder> innerHits) {
        if (innerHitBuilder != null) {
            Map<String, InnerHitContextBuilder> children = new HashMap<>();
            InnerHitContextBuilder.extractInnerHits(query, children);
            String name = innerHitBuilder.getName() != null ? innerHitBuilder.getName() : type;
            InnerHitContextBuilder innerHitContextBuilder =
                new ParentChildInnerHitContextBuilder(type, query, innerHitBuilder, children);
            innerHits.put(name, innerHitContextBuilder);
        }
    }

    static class ParentChildInnerHitContextBuilder extends InnerHitContextBuilder {
        private final String typeName;

        ParentChildInnerHitContextBuilder(String typeName, QueryBuilder query, InnerHitBuilder innerHitBuilder,
                                          Map<String, InnerHitContextBuilder> children) {
            super(query, innerHitBuilder, children);
            this.typeName = typeName;
        }

        @Override
        public void build(SearchContext parentSearchContext, InnerHitsContext innerHitsContext) throws IOException {
            QueryShardContext queryShardContext = parentSearchContext.getQueryShardContext();
            DocumentMapper documentMapper = queryShardContext.documentMapper(typeName);
            if (documentMapper == null) {
                if (innerHitBuilder.isIgnoreUnmapped() == false) {
                    throw new IllegalStateException("[" + query.getName() + "] no mapping found for type [" + typeName + "]");
                } else {
                    return;
                }
            }
            String name = innerHitBuilder.getName() != null ? innerHitBuilder.getName() : documentMapper.type();
            ParentChildInnerHitSubContext parentChildInnerHits = new ParentChildInnerHitSubContext(
                name, parentSearchContext, queryShardContext.getMapperService(), documentMapper
            );
            setupInnerHitsContext(queryShardContext, parentChildInnerHits);
            innerHitsContext.addInnerHitDefinition(parentChildInnerHits);
        }
    }

    static final class ParentChildInnerHitSubContext extends InnerHitsContext.InnerHitSubContext {
        private final MapperService mapperService;
        private final DocumentMapper documentMapper;

        ParentChildInnerHitSubContext(String name, SearchContext context, MapperService mapperService, DocumentMapper documentMapper) {
            super(name, context);
            this.mapperService = mapperService;
            this.documentMapper = documentMapper;
        }

        @Override
        public TopDocs[] topDocs(SearchHit[] hits) throws IOException {
            Weight innerHitQueryWeight = createInnerHitQueryWeight();
            TopDocs[] result = new TopDocs[hits.length];
            for (int i = 0; i < hits.length; i++) {
                SearchHit hit = hits[i];
                final Query hitQuery;
                if (isParentHit(hit)) {
                    String field = ParentFieldMapper.joinField(hit.getType());
                    hitQuery = new DocValuesTermsQuery(field, hit.getId());
                } else if (isChildHit(hit)) {
                    DocumentMapper hitDocumentMapper = mapperService.documentMapper(hit.getType());
                    final String parentType = hitDocumentMapper.parentFieldMapper().type();
                    SearchHitField parentField = hit.field(ParentFieldMapper.NAME);
                    if (parentField == null) {
                        throw new IllegalStateException("All children must have a _parent");
                    }
                    Term uidTerm = context.mapperService().createUidTerm(parentType, parentField.getValue());
                    if (uidTerm == null) {
                        hitQuery = new MatchNoDocsQuery("Missing type: " + parentType);
                    } else {
                        hitQuery = new TermQuery(uidTerm);
                    }
                } else {
                    result[i] = Lucene.EMPTY_TOP_DOCS;
                    continue;
                }

                BooleanQuery q = new BooleanQuery.Builder()
                    // Only include docs that have the current hit as parent
                    .add(hitQuery, BooleanClause.Occur.FILTER)
                    // Only include docs that have this inner hits type
                    .add(documentMapper.typeFilter(context.getQueryShardContext()), BooleanClause.Occur.FILTER)
                    .build();
                Weight weight = context.searcher().createNormalizedWeight(q, false);
                if (size() == 0) {
                    TotalHitCountCollector totalHitCountCollector = new TotalHitCountCollector();
                    for (LeafReaderContext ctx : context.searcher().getIndexReader().leaves()) {
                        intersect(weight, innerHitQueryWeight, totalHitCountCollector, ctx);
                    }
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
                        for (LeafReaderContext ctx : context.searcher().getIndexReader().leaves()) {
                            intersect(weight, innerHitQueryWeight, topDocsCollector, ctx);
                        }
                    } finally {
                        clearReleasables(Lifetime.COLLECTION);
                    }
                    result[i] = topDocsCollector.topDocs(from(), size());
                }
            }
            return result;
        }

        private boolean isParentHit(SearchHit hit) {
            return hit.getType().equals(documentMapper.parentFieldMapper().type());
        }

        private boolean isChildHit(SearchHit hit) {
            DocumentMapper hitDocumentMapper = mapperService.documentMapper(hit.getType());
            return documentMapper.type().equals(hitDocumentMapper.parentFieldMapper().type());
        }
    }
}
