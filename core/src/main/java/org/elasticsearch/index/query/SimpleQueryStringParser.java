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

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/**
 * SimpleQueryStringParser is a query parser that acts similar to a query_string
 * query, but won't throw exceptions for any weird string syntax. It supports
 * the following:
 * <ul>
 * <li>'{@code +}' specifies {@code AND} operation: <tt>token1+token2</tt>
 * <li>'{@code |}' specifies {@code OR} operation: <tt>token1|token2</tt>
 * <li>'{@code -}' negates a single token: <tt>-token0</tt>
 * <li>'{@code "}' creates phrases of terms: <tt>"term1 term2 ..."</tt>
 * <li>'{@code *}' at the end of terms specifies prefix query: <tt>term*</tt>
 * <li>'{@code (}' and '{@code)}' specifies precedence: <tt>token1 + (token2 | token3)</tt>
 * <li>'{@code ~}N' at the end of terms specifies fuzzy query: <tt>term~1</tt>
 * <li>'{@code ~}N' at the end of phrases specifies near/slop query: <tt>"term1 term2"~5</tt>
 * </ul>
 * <p>
 * See: {@link SimpleQueryParser} for more information.
 * <p>
 * This query supports these options:
 * <p>
 * Required:
 * {@code query} - query text to be converted into other queries
 * <p>
 * Optional:
 * {@code analyzer} - anaylzer to be used for analyzing tokens to determine
 * which kind of query they should be converted into, defaults to "standard"
 * {@code default_operator} - default operator for boolean queries, defaults
 * to OR
 * {@code fields} - fields to search, defaults to _all if not set, allows
 * boosting a field with ^n
 */
public class SimpleQueryStringParser implements QueryParser<SimpleQueryStringBuilder> {

    public static final ParseField MINIMUM_SHOULD_MATCH_FIELD = new ParseField("minimum_should_match");
    public static final ParseField ANALYZE_WILDCARD_FIELD = new ParseField("analyze_wildcard");
    public static final ParseField LENIENT_FIELD = new ParseField("lenient");
    public static final ParseField LOWERCASE_EXPANDED_TERMS_FIELD = new ParseField("lowercase_expanded_terms");
    public static final ParseField LOCALE_FIELD = new ParseField("locale");
    public static final ParseField FLAGS_FIELD = new ParseField("flags");
    public static final ParseField DEFAULT_OPERATOR_FIELD = new ParseField("default_operator");
    public static final ParseField ANALYZER_FIELD = new ParseField("analyzer");
    public static final ParseField QUERY_FIELD = new ParseField("query");
    public static final ParseField FIELDS_FIELD = new ParseField("fields");

    @Override
    public String[] names() {
        return new String[]{SimpleQueryStringBuilder.NAME, Strings.toCamelCase(SimpleQueryStringBuilder.NAME)};
    }

    @Override
    public SimpleQueryStringBuilder fromXContent(QueryParseContext parseContext) throws IOException {
        XContentParser parser = parseContext.parser();

        String currentFieldName = null;
        String queryBody = null;
        float boost = AbstractQueryBuilder.DEFAULT_BOOST;
        String queryName = null;
        String minimumShouldMatch = null;
        Map<String, Float> fieldsAndWeights = new HashMap<>();
        Operator defaultOperator = null;
        String analyzerName = null;
        int flags = SimpleQueryStringFlag.ALL.value();
        boolean lenient = SimpleQueryStringBuilder.DEFAULT_LENIENT;
        boolean lowercaseExpandedTerms = SimpleQueryStringBuilder.DEFAULT_LOWERCASE_EXPANDED_TERMS;
        boolean analyzeWildcard = SimpleQueryStringBuilder.DEFAULT_ANALYZE_WILDCARD;
        Locale locale = null;

        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_ARRAY) {
                if (parseContext.parseFieldMatcher().match(currentFieldName, FIELDS_FIELD)) {
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
                        fieldsAndWeights.put(fField, fBoost);
                    }
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "[" + SimpleQueryStringBuilder.NAME + "] query does not support [" + currentFieldName + "]");
                }
            } else if (token.isValue()) {
                if (parseContext.parseFieldMatcher().match(currentFieldName, QUERY_FIELD)) {
                    queryBody = parser.text();
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, AbstractQueryBuilder.BOOST_FIELD)) {
                    boost = parser.floatValue();
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, ANALYZER_FIELD)) {
                    analyzerName = parser.text();
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, DEFAULT_OPERATOR_FIELD)) {
                    defaultOperator = Operator.fromString(parser.text());
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, FLAGS_FIELD)) {
                    if (parser.currentToken() != XContentParser.Token.VALUE_NUMBER) {
                        // Possible options are:
                        // ALL, NONE, AND, OR, PREFIX, PHRASE, PRECEDENCE, ESCAPE, WHITESPACE, FUZZY, NEAR, SLOP
                        flags = SimpleQueryStringFlag.resolveFlags(parser.text());
                    } else {
                        flags = parser.intValue();
                        if (flags < 0) {
                            flags = SimpleQueryStringFlag.ALL.value();
                        }
                    }
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, LOCALE_FIELD)) {
                    String localeStr = parser.text();
                    locale = Locale.forLanguageTag(localeStr);
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, LOWERCASE_EXPANDED_TERMS_FIELD)) {
                    lowercaseExpandedTerms = parser.booleanValue();
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, LENIENT_FIELD)) {
                    lenient = parser.booleanValue();
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, ANALYZE_WILDCARD_FIELD)) {
                    analyzeWildcard = parser.booleanValue();
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, AbstractQueryBuilder.NAME_FIELD)) {
                    queryName = parser.text();
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, MINIMUM_SHOULD_MATCH_FIELD)) {
                    minimumShouldMatch = parser.textOrNull();
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "[" + SimpleQueryStringBuilder.NAME + "] unsupported field [" + parser.currentName() + "]");
                }
            } else {
                throw new ParsingException(parser.getTokenLocation(), "[" + SimpleQueryStringBuilder.NAME + "] unknown token [" + token + "] after [" + currentFieldName + "]");
            }
        }

        // Query text is required
        if (queryBody == null) {
            throw new ParsingException(parser.getTokenLocation(), "[" + SimpleQueryStringBuilder.NAME + "] query text missing");
        }

        SimpleQueryStringBuilder qb = new SimpleQueryStringBuilder(queryBody);
        qb.boost(boost).fields(fieldsAndWeights).analyzer(analyzerName).queryName(queryName).minimumShouldMatch(minimumShouldMatch);
        qb.flags(flags).defaultOperator(defaultOperator).locale(locale).lowercaseExpandedTerms(lowercaseExpandedTerms);
        qb.lenient(lenient).analyzeWildcard(analyzeWildcard).boost(boost);
        return qb;
    }

    @Override
    public SimpleQueryStringBuilder getBuilderPrototype() {
        return SimpleQueryStringBuilder.PROTOTYPE;
    }
}
