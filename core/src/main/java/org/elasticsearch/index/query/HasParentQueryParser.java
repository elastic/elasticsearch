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

import org.apache.lucene.search.*;
import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.fielddata.plain.ParentChildIndexFieldData;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.internal.ParentFieldMapper;
import org.elasticsearch.index.query.support.InnerHitsQueryParserHelper;
import org.elasticsearch.index.query.support.XContentStructure;
import org.elasticsearch.search.fetch.innerhits.InnerHitsContext;
import org.elasticsearch.search.fetch.innerhits.InnerHitsSubSearchContext;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import static org.elasticsearch.index.query.HasChildQueryParser.joinUtilHelper;

public class HasParentQueryParser implements QueryParser {

    public static final String NAME = "has_parent";
    private static final ParseField QUERY_FIELD = new ParseField("query", "filter");

    private final InnerHitsQueryParserHelper innerHitsQueryParserHelper;

    @Inject
    public HasParentQueryParser(InnerHitsQueryParserHelper innerHitsQueryParserHelper) {
        this.innerHitsQueryParserHelper = innerHitsQueryParserHelper;
    }

    @Override
    public String[] names() {
        return new String[]{NAME, Strings.toCamelCase(NAME)};
    }

    @Override
    public Query parse(QueryParseContext parseContext) throws IOException, QueryParsingException {
        XContentParser parser = parseContext.parser();

        boolean queryFound = false;
        float boost = 1.0f;
        String parentType = null;
        boolean score = false;
        String queryName = null;
        InnerHitsSubSearchContext innerHits = null;

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
                if (parseContext.parseFieldMatcher().match(currentFieldName, QUERY_FIELD)) {
                    iq = new XContentStructure.InnerQuery(parseContext, parentType == null ? null : new String[] {parentType});
                    queryFound = true;
                } else if ("inner_hits".equals(currentFieldName)) {
                    innerHits = innerHitsQueryParserHelper.parse(parseContext);
                } else {
                    throw new QueryParsingException(parseContext, "[has_parent] query does not support [" + currentFieldName + "]");
                }
            } else if (token.isValue()) {
                if ("type".equals(currentFieldName) || "parent_type".equals(currentFieldName) || "parentType".equals(currentFieldName)) {
                    parentType = parser.text();
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
                    throw new QueryParsingException(parseContext, "[has_parent] query does not support [" + currentFieldName + "]");
                }
            }
        }
        if (!queryFound) {
            throw new QueryParsingException(parseContext, "[has_parent] query requires 'query' field");
        }
        if (parentType == null) {
            throw new QueryParsingException(parseContext, "[has_parent] query requires 'parent_type' field");
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
            parseContext.addNamedQuery(queryName, query);
        }
        return query;
    }

    static Query createParentQuery(Query innerQuery, String parentType, boolean score, QueryParseContext parseContext, InnerHitsSubSearchContext innerHits) throws IOException {
        DocumentMapper parentDocMapper = parseContext.mapperService().documentMapper(parentType);
        if (parentDocMapper == null) {
            throw new QueryParsingException(parseContext, "[has_parent] query configured 'parent_type' [" + parentType
                    + "] is not a valid type");
        }

        if (innerHits != null) {
            ParsedQuery parsedQuery = new ParsedQuery(innerQuery, parseContext.copyNamedQueries());
            InnerHitsContext.ParentChildInnerHits parentChildInnerHits = new InnerHitsContext.ParentChildInnerHits(innerHits.getSubSearchContext(), parsedQuery, null, parseContext.mapperService(), parentDocMapper);
            String name = innerHits.getName() != null ? innerHits.getName() : parentType;
            parseContext.addInnerHits(name, parentChildInnerHits);
        }

        Set<String> parentTypes = new HashSet<>(5);
        parentTypes.add(parentDocMapper.type());
        ParentChildIndexFieldData parentChildIndexFieldData = null;
        for (DocumentMapper documentMapper : parseContext.mapperService().docMappers(false)) {
            ParentFieldMapper parentFieldMapper = documentMapper.parentFieldMapper();
            if (parentFieldMapper.active()) {
                DocumentMapper parentTypeDocumentMapper = parseContext.mapperService().documentMapper(parentFieldMapper.type());
                parentChildIndexFieldData = parseContext.getForField(parentFieldMapper.fieldType());
                if (parentTypeDocumentMapper == null) {
                    // Only add this, if this parentFieldMapper (also a parent)  isn't a child of another parent.
                    parentTypes.add(parentFieldMapper.type());
                }
            }
        }
        if (parentChildIndexFieldData == null) {
            throw new QueryParsingException(parseContext, "[has_parent] no _parent field configured");
        }

        Query parentTypeQuery = null;
        if (parentTypes.size() == 1) {
            DocumentMapper documentMapper = parseContext.mapperService().documentMapper(parentTypes.iterator().next());
            if (documentMapper != null) {
                parentTypeQuery = documentMapper.typeFilter();
            }
        } else {
            BooleanQuery.Builder parentsFilter = new BooleanQuery.Builder();
            for (String parentTypeStr : parentTypes) {
                DocumentMapper documentMapper = parseContext.mapperService().documentMapper(parentTypeStr);
                if (documentMapper != null) {
                    parentsFilter.add(documentMapper.typeFilter(), BooleanClause.Occur.SHOULD);
                }
            }
            parentTypeQuery = parentsFilter.build();
        }

        if (parentTypeQuery == null) {
            return null;
        }

        // wrap the query with type query
        innerQuery = Queries.filtered(innerQuery, parentDocMapper.typeFilter());
        Query childrenFilter = Queries.not(parentTypeQuery);
        ScoreMode scoreMode = score ? ScoreMode.Max : ScoreMode.None;
        return joinUtilHelper(parentType, parentChildIndexFieldData, childrenFilter, scoreMode, innerQuery, 0, Integer.MAX_VALUE);
    }

}
