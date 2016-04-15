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

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiDocValues;
import org.apache.lucene.search.*;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.search.join.JoinUtil;
import org.apache.lucene.search.join.ScoreMode;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.util.SuppressForbidden;
import org.elasticsearch.Version;
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
import org.elasticsearch.index.search.child.ChildrenConstantScoreQuery;
import org.elasticsearch.index.search.child.ChildrenQuery;
import org.elasticsearch.index.search.child.ScoreType;
import org.elasticsearch.search.fetch.innerhits.InnerHitsContext;
import org.elasticsearch.search.fetch.innerhits.InnerHitsSubSearchContext;

import java.io.IOException;

/**
 *
 */
@SuppressForbidden(reason="Old p/c queries still use filters")
public class HasChildQueryParser implements QueryParser {

    public static final String NAME = "has_child";
    private static final ParseField QUERY_FIELD = new ParseField("query", "filter");
    private static final ParseField SCORE_MODE = new ParseField("score_mode", "score_type");

    private final InnerHitsQueryParserHelper innerHitsQueryParserHelper;

    @Inject
    public HasChildQueryParser(InnerHitsQueryParserHelper innerHitsQueryParserHelper) {
        this.innerHitsQueryParserHelper = innerHitsQueryParserHelper;
    }

    @Override
    public String[] names() {
        return new String[] { NAME };
    }

    @Override
    public Query parse(QueryParseContext parseContext) throws IOException, QueryParsingException {
        XContentParser parser = parseContext.parser();

        boolean queryFound = false;
        float boost = 1.0f;
        String childType = null;
        ScoreType scoreType = ScoreType.NONE;
        int minChildren = 0;
        int maxChildren = 0;
        int shortCircuitParentDocSet = 8192;
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
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, SCORE_MODE)) {
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

        BitSetProducer nonNestedDocsFilter = null;
        if (parentDocMapper.hasNestedObjects()) {
            nonNestedDocsFilter = parseContext.bitsetFilter(Queries.newNonNestedFilter());
        }

        // wrap the query with type query
        innerQuery = Queries.filtered(innerQuery, childDocMapper.typeFilter());

        final Query query;
        final ParentChildIndexFieldData parentChildIndexFieldData = parseContext.getForField(parentFieldMapper.fieldType());
        if (parseContext.indexVersionCreated().onOrAfter(Version.V_2_0_0_beta1)) {
            query = joinUtilHelper(parentType, parentChildIndexFieldData, parseContext.similarityService().similarity(), parentDocMapper.typeFilter(), scoreType, innerQuery, minChildren, maxChildren);
        } else {
            // TODO: use the query API
            Filter parentFilter = new QueryWrapperFilter(parentDocMapper.typeFilter());
            if (minChildren > 1 || maxChildren > 0 || scoreType != ScoreType.NONE) {
                query = new ChildrenQuery(parentChildIndexFieldData, parentType, childType, parentFilter, innerQuery, scoreType, minChildren,
                        maxChildren, shortCircuitParentDocSet, nonNestedDocsFilter);
            } else {
                query = new ChildrenConstantScoreQuery(parentChildIndexFieldData, innerQuery, parentType, childType, parentFilter,
                        shortCircuitParentDocSet, nonNestedDocsFilter);
            }
        }
        if (queryName != null) {
            parseContext.addNamedQuery(queryName, query);
        }
        query.setBoost(boost);
        return query;
    }

    public static Query joinUtilHelper(String parentType, ParentChildIndexFieldData parentChildIndexFieldData, Similarity similarity, Query toQuery, ScoreType scoreType, Query innerQuery, int minChildren, int maxChildren) throws IOException {
        ScoreMode scoreMode;
        // TODO: move entirely over from ScoreType to org.apache.lucene.join.ScoreMode, when we drop the 1.x parent child code.
        switch (scoreType) {
            case NONE:
                scoreMode = ScoreMode.None;
                break;
            case MIN:
                scoreMode = ScoreMode.Min;
                break;
            case MAX:
                scoreMode = ScoreMode.Max;
                break;
            case SUM:
                scoreMode = ScoreMode.Total;
                break;
            case AVG:
                scoreMode = ScoreMode.Avg;
                break;
            default:
                throw new UnsupportedOperationException("score type [" + scoreType + "] not supported");
        }
        // 0 in pre 2.x p/c impl means unbounded
        if (maxChildren == 0) {
            maxChildren = Integer.MAX_VALUE;
        }
        return new LateParsingQuery(toQuery, innerQuery, minChildren, maxChildren, parentType, scoreMode, parentChildIndexFieldData, similarity);
    }

    final static class LateParsingQuery extends Query {

        private final Query toQuery;
        private final Query innerQuery;
        private final int minChildren;
        private final int maxChildren;
        private final String parentType;
        private final ScoreMode scoreMode;
        private final ParentChildIndexFieldData parentChildIndexFieldData;
        private final Similarity similarity;
        private final Object identity = new Object();

        LateParsingQuery(Query toQuery, Query innerQuery, int minChildren, int maxChildren, String parentType, ScoreMode scoreMode, ParentChildIndexFieldData parentChildIndexFieldData, Similarity similarity) {
            this.toQuery = toQuery;
            this.innerQuery = innerQuery;
            this.minChildren = minChildren;
            this.maxChildren = maxChildren;
            this.parentType = parentType;
            this.scoreMode = scoreMode;
            this.parentChildIndexFieldData = parentChildIndexFieldData;
            this.similarity = similarity;
        }

        @Override
        public Query rewrite(IndexReader reader) throws IOException {
            if (getBoost() != 1.0F) {
                return super.rewrite(reader);
            }
            if (reader instanceof DirectoryReader) {
                String joinField = ParentFieldMapper.joinField(parentType);
                IndexSearcher indexSearcher = new IndexSearcher(reader);
                indexSearcher.setQueryCache(null);
                indexSearcher.setSimilarity(similarity);
                IndexParentChildFieldData indexParentChildFieldData = parentChildIndexFieldData.loadGlobal((DirectoryReader) reader);
                MultiDocValues.OrdinalMap ordinalMap = ParentChildIndexFieldData.getOrdinalMap(indexParentChildFieldData, parentType);
                return JoinUtil.createJoinQuery(joinField, innerQuery, toQuery, indexSearcher, scoreMode, ordinalMap, minChildren, maxChildren);
            } else {
                if (reader.leaves().isEmpty() && reader.numDocs() == 0) {
                    // asserting reader passes down a MultiReader during rewrite which makes this
                    // blow up since for this query to work we have to have a DirectoryReader otherwise
                    // we can't load global ordinals - for this to work we simply check if the reader has no leaves
                    // and rewrite to match nothing
                    return new MatchNoDocsQuery();
                }
                throw new IllegalStateException("can't load global ordinals for reader of type: " + reader.getClass() + " must be a DirectoryReader");
            }
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
