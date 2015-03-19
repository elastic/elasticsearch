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
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.FilteredQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.search.NotFilter;
import org.elasticsearch.common.lucene.search.XBooleanFilter;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.fielddata.plain.ParentChildIndexFieldData;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.internal.ParentFieldMapper;
import org.elasticsearch.index.query.support.InnerHitsQueryParserHelper;
import org.elasticsearch.index.query.support.QueryInnerHitBuilder;
import org.elasticsearch.index.query.support.XContentStructure;
import org.elasticsearch.index.search.child.CustomQueryWrappingFilter;
import org.elasticsearch.index.search.child.ParentConstantScoreQuery;
import org.elasticsearch.index.search.child.ParentQuery;
import org.elasticsearch.search.fetch.innerhits.InnerHitsContext;
import org.elasticsearch.search.internal.SubSearchContext;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import static org.elasticsearch.index.query.QueryParserUtils.ensureNotDeleteByQuery;

/**
 * Builder for the 'has_parent' query.
 */
public class HasParentQueryBuilder extends BaseQueryBuilder implements QueryParser, BoostableQueryBuilder<HasParentQueryBuilder> {

    private QueryBuilder queryBuilder;
    private String parentType;
    private String scoreType;
    private float boost = 1.0f;
    private String queryName;
    private QueryInnerHitBuilder innerHit = null;

    public static final String NAME = "has_parent";

    private InnerHitsQueryParserHelper innerHitsQueryParserHelper;

    @Inject
    public HasParentQueryBuilder(InnerHitsQueryParserHelper innerHitsQueryParserHelper) {
        this.innerHitsQueryParserHelper = innerHitsQueryParserHelper;
    }

    /**
     * @param parentType  The parent type
     * @param parentQuery The query that will be matched with parent documents
     */
    public HasParentQueryBuilder(String parentType, QueryBuilder parentQuery) {
        this.parentType = parentType;
        this.queryBuilder = parentQuery;
    }

    @Override
    public HasParentQueryBuilder boost(float boost) {
        this.boost = boost;
        return this;
    }

    /**
     * Defines how the parent score is mapped into the child documents.
     */
    public HasParentQueryBuilder scoreType(String scoreType) {
        this.scoreType = scoreType;
        return this;
    }

    /**
     * Sets the query name for the filter that can be used when searching for matched_filters per hit.
     */
    public HasParentQueryBuilder queryName(String queryName) {
        this.queryName = queryName;
        return this;
    }

    /**
     * Sets inner hit definition in the scope of this query and reusing the defined type and query.
     */
    public HasParentQueryBuilder innerHit(QueryInnerHitBuilder innerHit) {
        this.innerHit = innerHit;
        return this;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(HasParentQueryBuilder.NAME);
        builder.field("query");
        queryBuilder.toXContent(builder, params);
        builder.field("parent_type", parentType);
        if (scoreType != null) {
            builder.field("score_type", scoreType);
        }
        if (boost != 1.0f) {
            builder.field("boost", boost);
        }
        if (queryName != null) {
            builder.field("_name", queryName);
        }
        if (innerHit != null) {
            builder.startObject("inner_hits");
            builder.value(innerHit);
            builder.endObject();
        }
        builder.endObject();
    }

    @Override
    public String[] names() {
        return new String[]{NAME, Strings.toCamelCase(NAME)};
    }

    @Override
    public Query parse(QueryParseContext parseContext) throws IOException, QueryParsingException {
        ensureNotDeleteByQuery(NAME, parseContext);
        XContentParser parser = parseContext.parser();

        boolean queryFound = false;
        float boost = 1.0f;
        String parentType = null;
        boolean score = false;
        String queryName = null;
        Tuple<String, SubSearchContext> innerHits = null;

        String currentFieldName = null;
        XContentParser.Token token;
        XContentStructure.InnerQuery iq = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                // Usually, the query would be parsed here, but the child
                // type may not have been extracted yet, so use the
                // XContentStructure.<type> facade to parse if available,
                // or delay parsing if not.
                if ("query".equals(currentFieldName)) {
                    iq = new XContentStructure.InnerQuery(parseContext, parentType == null ? null : new String[] {parentType});
                    queryFound = true;
                } else if ("inner_hits".equals(currentFieldName)) {
                    innerHits = innerHitsQueryParserHelper.parse(parseContext);
                } else {
                    throw new QueryParsingException(parseContext.index(), "[has_parent] query does not support [" + currentFieldName + "]");
                }
            } else if (token.isValue()) {
                if ("type".equals(currentFieldName) || "parent_type".equals(currentFieldName) || "parentType".equals(currentFieldName)) {
                    parentType = parser.text();
                } else if ("score_type".equals(currentFieldName) || "scoreType".equals(currentFieldName)) {
                    String scoreTypeValue = parser.text();
                    if ("score".equals(scoreTypeValue)) {
                        score = true;
                    } else if ("none".equals(scoreTypeValue)) {
                        score = false;
                    }
                } else if ("score_mode".equals(currentFieldName) || "scoreMode".equals(currentFieldName)) {
                    String scoreModeValue = parser.text();
                    if ("score".equals(scoreModeValue)) {
                        score = true;
                    } else if ("none".equals(scoreModeValue)) {
                        score = false;
                    }
                } else if ("boost".equals(currentFieldName)) {
                    boost = parser.floatValue();
                } else if ("_name".equals(currentFieldName)) {
                    queryName = parser.text();
                } else {
                    throw new QueryParsingException(parseContext.index(), "[has_parent] query does not support [" + currentFieldName + "]");
                }
            }
        }
        if (!queryFound) {
            throw new QueryParsingException(parseContext.index(), "[has_parent] query requires 'query' field");
        }
        if (parentType == null) {
            throw new QueryParsingException(parseContext.index(), "[has_parent] query requires 'parent_type' field");
        }

        Query innerQuery = iq.asQuery(parentType);

        if (innerQuery == null) {
            return null;
        }

        innerQuery.setBoost(boost);
        Query query = createParentQuery(innerQuery, parentType, score, parseContext, innerHits);
        if (query == null) {
            return null;
        }

        query.setBoost(boost);
        if (queryName != null) {
            parseContext.addNamedFilter(queryName, new CustomQueryWrappingFilter(query));
        }
        return query;
    }

    static Query createParentQuery(Query innerQuery, String parentType, boolean score, QueryParseContext parseContext, Tuple<String, SubSearchContext> innerHits) {
        DocumentMapper parentDocMapper = parseContext.mapperService().documentMapper(parentType);
        if (parentDocMapper == null) {
            throw new QueryParsingException(parseContext.index(), "[has_parent] query configured 'parent_type' [" + parentType + "] is not a valid type");
        }

        if (innerHits != null) {
            InnerHitsContext.ParentChildInnerHits parentChildInnerHits = new InnerHitsContext.ParentChildInnerHits(innerHits.v2(), innerQuery, null, parentDocMapper);
            String name = innerHits.v1() != null ? innerHits.v1() : parentType;
            parseContext.addInnerHits(name, parentChildInnerHits);
        }

        Set<String> parentTypes = new HashSet<>(5);
        parentTypes.add(parentDocMapper.type());
        ParentChildIndexFieldData parentChildIndexFieldData = null;
        for (DocumentMapper documentMapper : parseContext.mapperService().docMappers(false)) {
            ParentFieldMapper parentFieldMapper = documentMapper.parentFieldMapper();
            if (parentFieldMapper.active()) {
                DocumentMapper parentTypeDocumentMapper = parseContext.mapperService().documentMapper(parentFieldMapper.type());
                parentChildIndexFieldData = parseContext.getForField(parentFieldMapper);
                if (parentTypeDocumentMapper == null) {
                    // Only add this, if this parentFieldMapper (also a parent)  isn't a child of another parent.
                    parentTypes.add(parentFieldMapper.type());
                }
            }
        }
        if (parentChildIndexFieldData == null) {
            throw new QueryParsingException(parseContext.index(), "[has_parent] no _parent field configured");
        }

        Filter parentFilter = null;
        if (parentTypes.size() == 1) {
            DocumentMapper documentMapper = parseContext.mapperService().documentMapper(parentTypes.iterator().next());
            if (documentMapper != null) {
                parentFilter = documentMapper.typeFilter();
            }
        } else {
            XBooleanFilter parentsFilter = new XBooleanFilter();
            for (String parentTypeStr : parentTypes) {
                DocumentMapper documentMapper = parseContext.mapperService().documentMapper(parentTypeStr);
                if (documentMapper != null) {
                    parentsFilter.add(documentMapper.typeFilter(), BooleanClause.Occur.SHOULD);
                }
            }
            parentFilter = parentsFilter;
        }

        if (parentFilter == null) {
            return null;
        }

        // wrap the query with type query
        innerQuery = new FilteredQuery(innerQuery, parseContext.cacheFilter(parentDocMapper.typeFilter(), null, parseContext.autoFilterCachePolicy()));
        Filter childrenFilter = parseContext.cacheFilter(new NotFilter(parentFilter), null, parseContext.autoFilterCachePolicy());
        if (score) {
            return new ParentQuery(parentChildIndexFieldData, innerQuery, parentDocMapper.type(), childrenFilter);
        } else {
            return new ParentConstantScoreQuery(parentChildIndexFieldData, innerQuery, parentDocMapper.type(), childrenFilter);
        }
    }
}

