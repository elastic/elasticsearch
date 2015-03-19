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

import org.apache.lucene.search.Filter;
import org.apache.lucene.search.FilteredQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.join.BitDocIdSetFilter;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.fielddata.plain.ParentChildIndexFieldData;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.internal.ParentFieldMapper;
import org.elasticsearch.index.query.support.InnerHitsQueryParserHelper;
import org.elasticsearch.index.query.support.QueryInnerHitBuilder;
import org.elasticsearch.index.query.support.XContentStructure;
import org.elasticsearch.index.search.child.ChildrenConstantScoreQuery;
import org.elasticsearch.index.search.child.ChildrenQuery;
import org.elasticsearch.index.search.child.CustomQueryWrappingFilter;
import org.elasticsearch.index.search.child.ScoreType;
import org.elasticsearch.index.search.nested.NonNestedDocsFilter;
import org.elasticsearch.search.fetch.innerhits.InnerHitsContext;
import org.elasticsearch.search.internal.SubSearchContext;

import java.io.IOException;

import static org.elasticsearch.index.query.QueryParserUtils.ensureNotDeleteByQuery;

/**
 *
 */
public class HasChildQueryBuilder extends BaseQueryBuilder implements QueryParser, BoostableQueryBuilder<HasChildQueryBuilder> {

    private QueryBuilder queryBuilder;

    private String childType;

    private float boost = 1.0f;

    private String scoreType;

    private Integer minChildren;

    private Integer maxChildren;

    private Integer shortCircuitCutoff;

    private String queryName;

    private QueryInnerHitBuilder innerHit = null;

    public static final String NAME = "has_child";

    private InnerHitsQueryParserHelper innerHitsQueryParserHelper;

    @Inject
    public HasChildQueryBuilder(InnerHitsQueryParserHelper innerHitsQueryParserHelper) {
        this.innerHitsQueryParserHelper = innerHitsQueryParserHelper;
    }

    public HasChildQueryBuilder(String type, QueryBuilder queryBuilder) {
        this.childType = type;
        this.queryBuilder = queryBuilder;
    }

    /**
     * Sets the boost for this query.  Documents matching this query will (in addition to the normal
     * weightings) have their score multiplied by the boost provided.
     */
    @Override
    public HasChildQueryBuilder boost(float boost) {
        this.boost = boost;
        return this;
    }

    /**
     * Defines how the scores from the matching child documents are mapped into the parent document.
     */
    public HasChildQueryBuilder scoreType(String scoreType) {
        this.scoreType = scoreType;
        return this;
    }

    /**
     * Defines the minimum number of children that are required to match for the parent to be considered a match.
     */
    public HasChildQueryBuilder minChildren(int minChildren) {
        this.minChildren = minChildren;
        return this;
    }

    /**
     * Defines the maximum number of children that are required to match for the parent to be considered a match.
     */
    public HasChildQueryBuilder maxChildren(int maxChildren) {
        this.maxChildren = maxChildren;
        return this;
    }

    /**
     * Configures at what cut off point only to evaluate parent documents that contain the matching parent id terms
     * instead of evaluating all parent docs.
     */
    public HasChildQueryBuilder setShortCircuitCutoff(int shortCircuitCutoff) {
        this.shortCircuitCutoff = shortCircuitCutoff;
        return this;
    }

    /**
     * Sets the query name for the filter that can be used when searching for matched_filters per hit.
     */
    public HasChildQueryBuilder queryName(String queryName) {
        this.queryName = queryName;
        return this;
    }

    /**
     * Sets inner hit definition in the scope of this query and reusing the defined type and query.
     */
    public HasChildQueryBuilder innerHit(QueryInnerHitBuilder innerHit) {
        this.innerHit = innerHit;
        return this;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(HasChildQueryBuilder.NAME);
        builder.field("query");
        queryBuilder.toXContent(builder, params);
        builder.field("child_type", childType);
        if (boost != 1.0f) {
            builder.field("boost", boost);
        }
        if (scoreType != null) {
            builder.field("score_type", scoreType);
        }
        if (minChildren != null) {
            builder.field("min_children", minChildren);
        }
        if (maxChildren != null) {
            builder.field("max_children", maxChildren);
        }
        if (shortCircuitCutoff != null) {
            builder.field("short_circuit_cutoff", shortCircuitCutoff);
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
        return new String[] { NAME, Strings.toCamelCase(NAME) };
    }

    @Override
    public Query parse(QueryParseContext parseContext) throws IOException, QueryParsingException {
        ensureNotDeleteByQuery(NAME, parseContext);
        XContentParser parser = parseContext.parser();

        boolean queryFound = false;
        float boost = 1.0f;
        String childType = null;
        ScoreType scoreType = ScoreType.NONE;
        int minChildren = 0;
        int maxChildren = 0;
        int shortCircuitParentDocSet = 8192;
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
                    iq = new XContentStructure.InnerQuery(parseContext, childType == null ? null : new String[] { childType });
                    queryFound = true;
                } else if ("inner_hits".equals(currentFieldName)) {
                    innerHits = innerHitsQueryParserHelper.parse(parseContext);
                } else {
                    throw new QueryParsingException(parseContext.index(), "[has_child] query does not support [" + currentFieldName + "]");
                }
            } else if (token.isValue()) {
                if ("type".equals(currentFieldName) || "child_type".equals(currentFieldName) || "childType".equals(currentFieldName)) {
                    childType = parser.text();
                } else if ("score_type".equals(currentFieldName) || "scoreType".equals(currentFieldName)) {
                    scoreType = ScoreType.fromString(parser.text());
                } else if ("score_mode".equals(currentFieldName) || "scoreMode".equals(currentFieldName)) {
                    scoreType = ScoreType.fromString(parser.text());
                } else if ("boost".equals(currentFieldName)) {
                    boost = parser.floatValue();
                } else if ("min_children".equals(currentFieldName) || "minChildren".equals(currentFieldName)) {
                    minChildren = parser.intValue(true);
                } else if ("max_children".equals(currentFieldName) || "maxChildren".equals(currentFieldName)) {
                    maxChildren = parser.intValue(true);
                } else if ("short_circuit_cutoff".equals(currentFieldName)) {
                    shortCircuitParentDocSet = parser.intValue();
                } else if ("_name".equals(currentFieldName)) {
                    queryName = parser.text();
                } else {
                    throw new QueryParsingException(parseContext.index(), "[has_child] query does not support [" + currentFieldName + "]");
                }
            }
        }
        if (!queryFound) {
            throw new QueryParsingException(parseContext.index(), "[has_child] requires 'query' field");
        }
        if (childType == null) {
            throw new QueryParsingException(parseContext.index(), "[has_child] requires 'type' field");
        }

        Query innerQuery = iq.asQuery(childType);

        if (innerQuery == null) {
            return null;
        }
        innerQuery.setBoost(boost);

        DocumentMapper childDocMapper = parseContext.mapperService().documentMapper(childType);
        if (childDocMapper == null) {
            throw new QueryParsingException(parseContext.index(), "[has_child] No mapping for for type [" + childType + "]");
        }
        if (!childDocMapper.parentFieldMapper().active()) {
            throw new QueryParsingException(parseContext.index(), "[has_child]  Type [" + childType + "] does not have parent mapping");
        }

        if (innerHits != null) {
            InnerHitsContext.ParentChildInnerHits parentChildInnerHits = new InnerHitsContext.ParentChildInnerHits(innerHits.v2(), innerQuery, null, childDocMapper);
            String name = innerHits.v1() != null ? innerHits.v1() : childType;
            parseContext.addInnerHits(name, parentChildInnerHits);
        }

        ParentFieldMapper parentFieldMapper = childDocMapper.parentFieldMapper();
        if (!parentFieldMapper.active()) {
            throw new QueryParsingException(parseContext.index(), "[has_child] _parent field not configured");
        }

        String parentType = parentFieldMapper.type();
        DocumentMapper parentDocMapper = parseContext.mapperService().documentMapper(parentType);
        if (parentDocMapper == null) {
            throw new QueryParsingException(parseContext.index(), "[has_child]  Type [" + childType
                    + "] points to a non existent parent type [" + parentType + "]");
        }

        if (maxChildren > 0 && maxChildren < minChildren) {
            throw new QueryParsingException(parseContext.index(), "[has_child] 'max_children' is less than 'min_children'");
        }

        BitDocIdSetFilter nonNestedDocsFilter = null;
        if (parentDocMapper.hasNestedObjects()) {
            nonNestedDocsFilter = parseContext.bitsetFilter(NonNestedDocsFilter.INSTANCE);
        }

        // wrap the query with type query
        innerQuery = new FilteredQuery(innerQuery, parseContext.cacheFilter(childDocMapper.typeFilter(), null, parseContext.autoFilterCachePolicy()));

        Query query;
        Filter parentFilter = parseContext.cacheFilter(parentDocMapper.typeFilter(), null, parseContext.autoFilterCachePolicy());
        ParentChildIndexFieldData parentChildIndexFieldData = parseContext.getForField(parentFieldMapper);
        if (minChildren > 1 || maxChildren > 0 || scoreType != ScoreType.NONE) {
            query = new ChildrenQuery(parentChildIndexFieldData, parentType, childType, parentFilter, innerQuery, scoreType, minChildren,
                    maxChildren, shortCircuitParentDocSet, nonNestedDocsFilter);
        } else {
            query = new ChildrenConstantScoreQuery(parentChildIndexFieldData, innerQuery, parentType, childType, parentFilter,
                    shortCircuitParentDocSet, nonNestedDocsFilter);
        }
        if (queryName != null) {
            parseContext.addNamedFilter(queryName, new CustomQueryWrappingFilter(query));
        }
        query.setBoost(boost);
        return query;
    }
}
