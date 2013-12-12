/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
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

import org.apache.lucene.queryparser.XSimpleQueryParser;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.analysis.NamedAnalyzer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * SimpleQueryStringParser is a query parser that acts similar to a query_string
 * query, but won't throw exceptions for any weird string syntax. It supports
 * the following:
 *
 * <ul>
 *  <li>'{@code +}' specifies {@code AND} operation: <tt>token1+token2</tt>
 *  <li>'{@code |}' specifies {@code OR} operation: <tt>token1|token2</tt>
 *  <li>'{@code -}' negates a single token: <tt>-token0</tt>
 *  <li>'{@code "}' creates phrases of terms: <tt>"term1 term2 ..."</tt>
 *  <li>'{@code *}' at the end of terms specifies prefix query: <tt>term*</tt>
 *  <li>'{@code (}' and '{@code )}' specifies precedence: <tt>token1 + (token2 | token3)</tt>
 * </ul>
 *
 * See: {@link XSimpleQueryParser} for more information.
 *
 * This query supports these options:
 *
 * Required:
 * {@code query} - query text to be converted into other queries
 *
 * Optional:
 * {@code analyzer} - anaylzer to be used for analyzing tokens to determine
 * which kind of query they should be converted into, defaults to "standard"
 * {@code default_operator} - default operator for boolean queries, defaults
 * to OR
 * {@code fields} - fields to search, defaults to _all if not set, allows
 * boosting a field with ^n
 */
public class SimpleQueryStringParser implements QueryParser {

    public static final String NAME = "simple_query_string";

    @Inject
    public SimpleQueryStringParser(Settings settings) {

    }

    @Override
    public String[] names() {
        return new String[]{NAME, Strings.toCamelCase(NAME)};
    }

    @Override
    public Query parse(QueryParseContext parseContext) throws IOException, QueryParsingException {
        XContentParser parser = parseContext.parser();

        String currentFieldName = null;
        String queryBody = null;
        String field = null;
        Map<String, Float> fieldsAndWeights = null;
        BooleanClause.Occur defaultOperator = null;
        NamedAnalyzer analyzer = null;
        XContentParser.Token token = null;

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_ARRAY) {
                if ("fields".equals(currentFieldName)) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        String fField = null;
                        float fBoost = 1;
                        char[] text = parser.textCharacters();
                        int end = parser.textOffset() + parser.textLength();
                        for (int i = parser.textOffset(); i < end; i++) {
                            if (text[i] == '^') {
                                int relativeLocation = i - parser.textOffset();
                                fField = new String(text, parser.textOffset(), relativeLocation);
                                fBoost = Float.parseFloat(new String(text, i + 1, parser.textLength() - relativeLocation - 1));
                                break;
                            }
                        }
                        if (fField == null) {
                            fField = parser.text();
                        }

                        if (fieldsAndWeights == null) {
                            fieldsAndWeights = new HashMap<String, Float>();
                        }

                        if (Regex.isSimpleMatchPattern(fField)) {
                            for (String fieldName : parseContext.mapperService().simpleMatchToIndexNames(fField)) {
                                fieldsAndWeights.put(fieldName, fBoost);
                            }
                        } else {
                            fieldsAndWeights.put(fField, fBoost);
                        }
                    }
                } else {
                    throw new QueryParsingException(parseContext.index(),
                            "[" + NAME + "] query does not support [" + currentFieldName + "]");
                }
            } else if (token.isValue()) {
                if ("query".equals(currentFieldName)) {
                    queryBody = parser.text();
                } else if ("analyzer".equals(currentFieldName)) {
                    analyzer = parseContext.analysisService().analyzer(parser.text());
                    if (analyzer == null) {
                        throw new QueryParsingException(parseContext.index(),
                                "[" + NAME + "] analyzer [" + parser.text() + "] not found");
                    }
                } else if ("field".equals(currentFieldName)) {
                    field = parser.text();
                } else if ("default_operator".equals(currentFieldName)) {
                    String op = parser.text();
                    if ("or".equalsIgnoreCase(op)) {
                        defaultOperator = BooleanClause.Occur.SHOULD;
                    } else if ("and".equalsIgnoreCase(op)) {
                        defaultOperator = BooleanClause.Occur.MUST;
                    } else {
                        throw new QueryParsingException(parseContext.index(),
                                "[" + NAME + "] default operator [" + op + "] is not allowed");
                    }
                } else {
                    throw new QueryParsingException(parseContext.index(), "[" + NAME + "] unsupported field [" + parser.currentName() + "]");
                }
            }
        }

        // Query text is required
        if (queryBody == null) {
            throw new QueryParsingException(parseContext.index(), "[" + NAME + "] query text missing");
        }

        // Support specifying only a field instead of a map
        if (field == null) {
            field = currentFieldName;
        }

        // Use the default field (_all) if no fields specified
        if (queryBody != null && fieldsAndWeights == null) {
            field = parseContext.defaultField();
        }

        // Use standard analyzer by default
        if (analyzer == null) {
            analyzer = parseContext.analysisService().analyzer("standard");
        }

        XSimpleQueryParser sqp;
        if (fieldsAndWeights != null) {
            sqp = new XSimpleQueryParser(analyzer, fieldsAndWeights);
        } else {
            sqp = new XSimpleQueryParser(analyzer, field);
        }

        if (defaultOperator != null) {
            sqp.setDefaultOperator(defaultOperator);
        }

        return sqp.parse(queryBody);
    }
}
