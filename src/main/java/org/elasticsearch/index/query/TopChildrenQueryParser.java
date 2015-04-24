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

import org.apache.lucene.search.FilteredQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.join.BitDocIdSetFilter;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.fielddata.plain.ParentChildIndexFieldData;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.internal.ParentFieldMapper;
import org.elasticsearch.index.query.support.XContentStructure;
import org.elasticsearch.index.search.child.CustomQueryWrappingFilter;
import org.elasticsearch.index.search.child.ScoreType;
import org.elasticsearch.index.search.child.TopChildrenQuery;

import java.io.IOException;

import static org.elasticsearch.index.query.QueryParserUtils.ensureNotDeleteByQuery;

/**
 *
 */
public class TopChildrenQueryParser implements QueryParser {

    public static final String NAME = "top_children";

    @Inject
    public TopChildrenQueryParser() {
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
        String childType = null;
        ScoreType scoreType = ScoreType.MAX;
        int factor = 5;
        int incrementalFactor = 2;
        String queryName = null;

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
                    iq = new XContentStructure.InnerQuery(parseContext, childType == null ? null : new String[] {childType});
                    queryFound = true;
                } else {
                    throw new QueryParsingException(parseContext.index(), "[top_children] query does not support [" + currentFieldName + "]");
                }
            } else if (token.isValue()) {
                if ("type".equals(currentFieldName)) {
                    childType = parser.text();
                } else if ("score".equals(currentFieldName)) {
                    scoreType = ScoreType.fromString(parser.text());
                } else if ("score_mode".equals(currentFieldName) || "scoreMode".equals(currentFieldName)) {
                    scoreType = ScoreType.fromString(parser.text());
                } else if ("boost".equals(currentFieldName)) {
                    boost = parser.floatValue();
                } else if ("factor".equals(currentFieldName)) {
                    factor = parser.intValue();
                } else if ("incremental_factor".equals(currentFieldName) || "incrementalFactor".equals(currentFieldName)) {
                    incrementalFactor = parser.intValue();
                } else if ("_name".equals(currentFieldName)) {
                    queryName = parser.text();
                } else {
                    throw new QueryParsingException(parseContext.index(), "[top_children] query does not support [" + currentFieldName + "]");
                }
            }
        }
        if (!queryFound) {
            throw new QueryParsingException(parseContext.index(), "[top_children] requires 'query' field");
        }
        if (childType == null) {
            throw new QueryParsingException(parseContext.index(), "[top_children] requires 'type' field");
        }

        Query innerQuery = iq.asQuery(childType);

        if (innerQuery == null) {
            return null;
        }

        DocumentMapper childDocMapper = parseContext.mapperService().documentMapper(childType);
        if (childDocMapper == null) {
            throw new QueryParsingException(parseContext.index(), "No mapping for for type [" + childType + "]");
        }
        ParentFieldMapper parentFieldMapper = childDocMapper.parentFieldMapper();
        if (!parentFieldMapper.active()) {
            throw new QueryParsingException(parseContext.index(), "Type [" + childType + "] does not have parent mapping");
        }
        String parentType = childDocMapper.parentFieldMapper().type();

        BitDocIdSetFilter nonNestedDocsFilter = null;
        if (childDocMapper.hasNestedObjects()) {
            nonNestedDocsFilter = parseContext.bitsetFilter(Queries.newNonNestedFilter());
        }

        innerQuery.setBoost(boost);
        // wrap the query with type query
        innerQuery = new FilteredQuery(innerQuery, parseContext.cacheFilter(childDocMapper.typeFilter(), null, parseContext.autoFilterCachePolicy()));
        ParentChildIndexFieldData parentChildIndexFieldData = parseContext.getForField(parentFieldMapper);
        TopChildrenQuery query = new TopChildrenQuery(parentChildIndexFieldData, innerQuery, childType, parentType, scoreType, factor, incrementalFactor, nonNestedDocsFilter);
        if (queryName != null) {
            parseContext.addNamedFilter(queryName, new CustomQueryWrappingFilter(query));
        }
        return query;
    }
}
