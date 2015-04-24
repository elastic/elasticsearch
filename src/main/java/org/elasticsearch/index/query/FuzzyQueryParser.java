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

import org.apache.lucene.index.Term;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.support.QueryParsers;

import java.io.IOException;

/**
 *
 */
public class FuzzyQueryParser implements QueryParser {

    public static final String NAME = "fuzzy";
    private static final Fuzziness DEFAULT_FUZZINESS = Fuzziness.AUTO;
    private static final ParseField FUZZINESS = Fuzziness.FIELD.withDeprecation("min_similarity");


    @Inject
    public FuzzyQueryParser() {
    }

    @Override
    public String[] names() {
        return new String[]{NAME};
    }

    @Override
    public Query parse(QueryParseContext parseContext) throws IOException, QueryParsingException {
        XContentParser parser = parseContext.parser();

        XContentParser.Token token = parser.nextToken();
        if (token != XContentParser.Token.FIELD_NAME) {
            throw new QueryParsingException(parseContext.index(), "[fuzzy] query malformed, no field");
        }
        String fieldName = parser.currentName();

        String value = null;
        float boost = 1.0f;
        Fuzziness fuzziness = DEFAULT_FUZZINESS;
        int prefixLength = FuzzyQuery.defaultPrefixLength;
        int maxExpansions = FuzzyQuery.defaultMaxExpansions;
        boolean transpositions = false;
        String queryName = null;
        MultiTermQuery.RewriteMethod rewriteMethod = null;
        token = parser.nextToken();
        if (token == XContentParser.Token.START_OBJECT) {
            String currentFieldName = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else {
                    if ("term".equals(currentFieldName)) {
                        value = parser.text();
                    } else if ("value".equals(currentFieldName)) {
                        value = parser.text();
                    } else if ("boost".equals(currentFieldName)) {
                        boost = parser.floatValue();
                    } else if (FUZZINESS.match(currentFieldName, parseContext.parseFlags())) {
                        fuzziness = Fuzziness.parse(parser);
                    } else if ("prefix_length".equals(currentFieldName) || "prefixLength".equals(currentFieldName)) {
                        prefixLength = parser.intValue();
                    } else if ("max_expansions".equals(currentFieldName) || "maxExpansions".equals(currentFieldName)) {
                        maxExpansions = parser.intValue();
                    } else if ("transpositions".equals(currentFieldName)) {
                      transpositions = parser.booleanValue();
                    } else if ("rewrite".equals(currentFieldName)) {
                        rewriteMethod = QueryParsers.parseRewriteMethod(parser.textOrNull(), null);
                    } else if ("_name".equals(currentFieldName)) {
                        queryName = parser.text();
                    } else {
                        throw new QueryParsingException(parseContext.index(), "[fuzzy] query does not support [" + currentFieldName + "]");
                    }
                }
            }
            parser.nextToken();
        } else {
            value = parser.text();
            // move to the next token
            parser.nextToken();
        }

        if (value == null) {
            throw new QueryParsingException(parseContext.index(), "No value specified for fuzzy query");
        }

        Query query = null;
        MapperService.SmartNameFieldMappers smartNameFieldMappers = parseContext.smartFieldMappers(fieldName);
        if (smartNameFieldMappers != null) {
            if (smartNameFieldMappers.hasMapper()) {
                query = smartNameFieldMappers.mapper().fuzzyQuery(value, fuzziness, prefixLength, maxExpansions, transpositions);
            }
        }
        if (query == null) {
            query = new FuzzyQuery(new Term(fieldName, value), fuzziness.asDistance(value), prefixLength, maxExpansions, transpositions);
        }
        if (query instanceof MultiTermQuery) {
            QueryParsers.setRewriteMethod((MultiTermQuery) query, rewriteMethod);
        }
        query.setBoost(boost);

        if (queryName != null) {
            parseContext.addNamedQuery(queryName, query);
        }
        return query;
    }
}
