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

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiDocValues;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.join.JoinUtil;
import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.fielddata.IndexParentChildFieldData;
import org.elasticsearch.index.fielddata.plain.ParentChildIndexFieldData;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.internal.ParentFieldMapper;
import org.elasticsearch.index.query.support.InnerHitsQueryParserHelper;
import org.elasticsearch.index.query.support.XContentStructure;
import org.elasticsearch.search.fetch.innerhits.InnerHitsContext;
import org.elasticsearch.search.fetch.innerhits.InnerHitsSubSearchContext;

import java.io.IOException;

/**
 *
 */
public class HasChildQueryParser implements QueryParser {

    public static final String NAME = "has_child";
    private static final ParseField QUERY_FIELD = new ParseField("query", "filter");

    private final InnerHitsQueryParserHelper innerHitsQueryParserHelper;

    @Inject
    public HasChildQueryParser(InnerHitsQueryParserHelper innerHitsQueryParserHelper) {
        this.innerHitsQueryParserHelper = innerHitsQueryParserHelper;
    }

    @Override
    public String[] names() {
        return new String[] { NAME, Strings.toCamelCase(NAME) };
    }

    @Override
    public Query parse(QueryParseContext parseContext) throws IOException, QueryParsingException {
        XContentParser parser = parseContext.parser();

        boolean queryFound = false;
        float boost = 1.0f;
        String childType = null;
        ScoreMode scoreMode = ScoreMode.None;
        int minChildren = 0;
        int maxChildren = Integer.MAX_VALUE;
        String queryName = null;
        InnerHitsSubSearchContext innerHits = null;

        String currentFieldName = null;
        XContentParser.Token token;
        XContentStructure.InnerQuery iq = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (parseContext.isDeprecatedSetting(currentFieldName)) {
                // skip
            } else if (token == XContentParser.Token.START_OBJECT) {
                // Usually, the query would be parsed here, but the child
                // type may not have been extracted yet, so use the
                // XContentStructure.<type> facade to parse if available,
                // or delay parsing if not.
                if (parseContext.parseFieldMatcher().match(currentFieldName, QUERY_FIELD)) {
                    iq = new XContentStructure.InnerQuery(parseContext, childType == null ? null : new String[] { childType });
                    queryFound = true;
                } else if ("inner_hits".equals(currentFieldName)) {
                    innerHits = innerHitsQueryParserHelper.parse(parseContext);
                } else {
                    throw new QueryParsingException(parseContext, "[has_child] query does not support [" + currentFieldName + "]");
                }
            } else if (token.isValue()) {
                if ("type".equals(currentFieldName) || "child_type".equals(currentFieldName) || "childType".equals(currentFieldName)) {
                    childType = parser.text();
                } else if ("score_mode".equals(currentFieldName) || "scoreMode".equals(currentFieldName)) {
                    scoreMode = parseScoreMode(parser.text());
                } else if ("boost".equals(currentFieldName)) {
                    boost = parser.floatValue();
                } else if ("min_children".equals(currentFieldName) || "minChildren".equals(currentFieldName)) {
                    minChildren = parser.intValue(true);
                } else if ("max_children".equals(currentFieldName) || "maxChildren".equals(currentFieldName)) {
                    maxChildren = parser.intValue(true);
                } else if ("_name".equals(currentFieldName)) {
                    queryName = parser.text();
                } else {
                    throw new QueryParsingException(parseContext, "[has_child] query does not support [" + currentFieldName + "]");
                }
            }
        }
        if (!queryFound) {
            throw new QueryParsingException(parseContext, "[has_child] requires 'query' field");
        }
        if (childType == null) {
            throw new QueryParsingException(parseContext, "[has_child] requires 'type' field");
        }

        Query innerQuery = iq.asQuery(childType);

        if (innerQuery == null) {
            return null;
        }
        innerQuery.setBoost(boost);

        DocumentMapper childDocMapper = parseContext.mapperService().documentMapper(childType);
        if (childDocMapper == null) {
            throw new QueryParsingException(parseContext, "[has_child] No mapping for for type [" + childType + "]");
        }
        ParentFieldMapper parentFieldMapper = childDocMapper.parentFieldMapper();
        if (parentFieldMapper.active() == false) {
            throw new QueryParsingException(parseContext, "[has_child] _parent field has no parent type configured");
        }

        if (innerHits != null) {
            ParsedQuery parsedQuery = new ParsedQuery(innerQuery, parseContext.copyNamedQueries());
            InnerHitsContext.ParentChildInnerHits parentChildInnerHits = new InnerHitsContext.ParentChildInnerHits(innerHits.getSubSearchContext(), parsedQuery, null, parseContext.mapperService(), childDocMapper);
            String name = innerHits.getName() != null ? innerHits.getName() : childType;
            parseContext.addInnerHits(name, parentChildInnerHits);
        }

        String parentType = parentFieldMapper.type();
        DocumentMapper parentDocMapper = parseContext.mapperService().documentMapper(parentType);
        if (parentDocMapper == null) {
            throw new QueryParsingException(parseContext, "[has_child]  Type [" + childType + "] points to a non existent parent type ["
                    + parentType + "]");
        }

        if (maxChildren > 0 && maxChildren < minChildren) {
            throw new QueryParsingException(parseContext, "[has_child] 'max_children' is less than 'min_children'");
        }

        // wrap the query with type query
        innerQuery = Queries.filtered(innerQuery, childDocMapper.typeFilter());

        final Query query;
        final ParentChildIndexFieldData parentChildIndexFieldData = parseContext.getForField(parentFieldMapper.fieldType());
        query = joinUtilHelper(parentType, parentChildIndexFieldData, parentDocMapper.typeFilter(), scoreMode, innerQuery, minChildren, maxChildren);
        if (queryName != null) {
            parseContext.addNamedQuery(queryName, query);
        }
        query.setBoost(boost);
        return query;
    }

    public static Query joinUtilHelper(String parentType, ParentChildIndexFieldData parentChildIndexFieldData, Query toQuery, ScoreMode scoreMode, Query innerQuery, int minChildren, int maxChildren) throws IOException {
        return new LateParsingQuery(toQuery, innerQuery, minChildren, maxChildren, parentType, scoreMode, parentChildIndexFieldData);
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
        } else if ("total".equals(scoreModeString)) {
            return ScoreMode.Total;
        }
        throw new IllegalArgumentException("No score mode for child query [" + scoreModeString + "] found");
    }

    final static class LateParsingQuery extends Query {

        private final Query toQuery;
        private final Query innerQuery;
        private final int minChildren;
        private final int maxChildren;
        private final String parentType;
        private final ScoreMode scoreMode;
        private final ParentChildIndexFieldData parentChildIndexFieldData;
        private final Object identity = new Object();

        LateParsingQuery(Query toQuery, Query innerQuery, int minChildren, int maxChildren, String parentType, ScoreMode scoreMode, ParentChildIndexFieldData parentChildIndexFieldData) {
            this.toQuery = toQuery;
            this.innerQuery = innerQuery;
            this.minChildren = minChildren;
            this.maxChildren = maxChildren;
            this.parentType = parentType;
            this.scoreMode = scoreMode;
            this.parentChildIndexFieldData = parentChildIndexFieldData;
        }

        @Override
        public Query rewrite(IndexReader reader) throws IOException {
            if (getBoost() != 1.0F) {
                return super.rewrite(reader);
            }
            String joinField = ParentFieldMapper.joinField(parentType);
            IndexSearcher indexSearcher = new IndexSearcher(reader);
            indexSearcher.setQueryCache(null);
            IndexParentChildFieldData indexParentChildFieldData = parentChildIndexFieldData.loadGlobal(indexSearcher.getIndexReader());
            MultiDocValues.OrdinalMap ordinalMap = ParentChildIndexFieldData.getOrdinalMap(indexParentChildFieldData, parentType);
            return JoinUtil.createJoinQuery(joinField, innerQuery, toQuery, indexSearcher, scoreMode, ordinalMap, minChildren, maxChildren);
        }

        // Even though we only cache rewritten queries it is good to let all queries implement hashCode() and equals():

        // We can't check for actually equality here, since we need to IndexReader for this, but
        // that isn't available on all cases during query parse time, so instead rely on identity:
        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            if (!super.equals(o)) return false;

            LateParsingQuery that = (LateParsingQuery) o;
            return identity.equals(that.identity);
        }

        @Override
        public int hashCode() {
            int result = super.hashCode();
            result = 31 * result + identity.hashCode();
            return result;
        }

        @Override
        public String toString(String s) {
            return "LateParsingQuery {parentType=" + parentType + "}";
        }
    }
}
