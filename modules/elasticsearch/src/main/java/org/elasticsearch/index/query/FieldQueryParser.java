/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import org.apache.lucene.queryParser.MapperQueryParser;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParserSettings;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.support.QueryParsers;

import java.io.IOException;

import static org.elasticsearch.common.lucene.search.Queries.*;

/**
 * @author kimchy (shay.banon)
 */
public class FieldQueryParser implements QueryParser {

    public static final String NAME = "field";

    @Inject public FieldQueryParser() {
    }

    @Override public String[] names() {
        return new String[]{NAME};
    }

    @Override public Query parse(QueryParseContext parseContext) throws IOException, QueryParsingException {
        XContentParser parser = parseContext.parser();

        XContentParser.Token token = parser.nextToken();
        assert token == XContentParser.Token.FIELD_NAME;
        String fieldName = parser.currentName();

        QueryParserSettings qpSettings = new QueryParserSettings();
        qpSettings.defaultField(fieldName);
        token = parser.nextToken();
        if (token == XContentParser.Token.START_OBJECT) {
            String currentFieldName = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token.isValue()) {
                    if ("query".equals(currentFieldName)) {
                        qpSettings.queryString(parser.text());
                    } else if ("boost".equals(currentFieldName)) {
                        qpSettings.boost(parser.floatValue());
                    } else if ("enable_position_increments".equals(currentFieldName) || "enablePositionIncrements".equals(currentFieldName)) {
                        qpSettings.enablePositionIncrements(parser.booleanValue());
                    } else if ("allow_leading_wildcard".equals(currentFieldName) || "allowLeadingWildcard".equals(currentFieldName)) {
                        qpSettings.allowLeadingWildcard(parser.booleanValue());
                    } else if ("auto_generate_phrase_queries".equals(currentFieldName) || "autoGeneratePhraseQueries".equals(currentFieldName)) {
                        qpSettings.autoGeneratePhraseQueries(parser.booleanValue());
                    } else if ("lowercase_expanded_terms".equals(currentFieldName) || "lowercaseExpandedTerms".equals(currentFieldName)) {
                        qpSettings.lowercaseExpandedTerms(parser.booleanValue());
                    } else if ("phrase_slop".equals(currentFieldName) || "phraseSlop".equals(currentFieldName)) {
                        qpSettings.phraseSlop(parser.intValue());
                    } else if ("analyzer".equals(currentFieldName)) {
                        qpSettings.analyzer(parseContext.analysisService().analyzer(parser.text()));
                    } else if ("default_operator".equals(currentFieldName) || "defaultOperator".equals(currentFieldName)) {
                        String op = parser.text();
                        if ("or".equalsIgnoreCase(op)) {
                            qpSettings.defaultOperator(org.apache.lucene.queryParser.QueryParser.Operator.OR);
                        } else if ("and".equalsIgnoreCase(op)) {
                            qpSettings.defaultOperator(org.apache.lucene.queryParser.QueryParser.Operator.AND);
                        } else {
                            throw new QueryParsingException(parseContext.index(), "Query default operator [" + op + "] is not allowed");
                        }
                    } else if ("fuzzy_min_sim".equals(currentFieldName) || "fuzzyMinSim".equals(currentFieldName)) {
                        qpSettings.fuzzyMinSim(parser.floatValue());
                    } else if ("fuzzy_prefix_length".equals(currentFieldName) || "fuzzyPrefixLength".equals(currentFieldName)) {
                        qpSettings.fuzzyPrefixLength(parser.intValue());
                    } else if ("escape".equals(currentFieldName)) {
                        qpSettings.escape(parser.booleanValue());
                    } else if ("analyze_wildcard".equals(currentFieldName) || "analyzeWildcard".equals(currentFieldName)) {
                        qpSettings.analyzeWildcard(parser.booleanValue());
                    } else if ("rewrite".equals(currentFieldName)) {
                        qpSettings.rewriteMethod(QueryParsers.parseRewriteMethod(parser.textOrNull()));
                    }
                }
            }
            parser.nextToken();
        } else {
            qpSettings.queryString(parser.text());
            // move to the next token
            parser.nextToken();
        }

        if (qpSettings.analyzer() == null) {
            qpSettings.analyzer(parseContext.mapperService().searchAnalyzer());
        }

        if (qpSettings.queryString() == null) {
            throw new QueryParsingException(parseContext.index(), "No value specified for term query");
        }

        if (qpSettings.escape()) {
            qpSettings.queryString(org.apache.lucene.queryParser.QueryParser.escape(qpSettings.queryString()));
        }

        Query query = parseContext.indexCache().queryParserCache().get(qpSettings);
        if (query != null) {
            return query;
        }

        MapperQueryParser queryParser = parseContext.singleQueryParser(qpSettings);

        try {
            query = queryParser.parse(qpSettings.queryString());
            query.setBoost(qpSettings.boost());
            query = optimizeQuery(fixNegativeQueryIfNeeded(query));
            parseContext.indexCache().queryParserCache().put(qpSettings, query);
            return query;
        } catch (ParseException e) {
            throw new QueryParsingException(parseContext.index(), "Failed to parse query [" + qpSettings.queryString() + "]", e);
        }
    }
}