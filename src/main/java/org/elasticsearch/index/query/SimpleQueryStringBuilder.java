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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.LocaleUtils;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.MapperService;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/**
 * SimpleQuery is a query parser that acts similar to a query_string
 * query, but won't throw exceptions for any weird string syntax.
 * It supports the following:
 * <p/>
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
 * <p/>
 * See: {@link XSimpleQueryParser} for more information.
 * <p/>
 * This query supports these options:
 * <p/>
 * Required:
 * {@code query} - query text to be converted into other queries
 * <p/>
 * Optional:
 * {@code analyzer} - anaylzer to be used for analyzing tokens to determine
 * which kind of query they should be converted into, defaults to "standard"
 * {@code default_operator} - default operator for boolean queries, defaults
 * to OR
 * {@code fields} - fields to search, defaults to _all if not set, allows
 * boosting a field with ^n
 **/
public class SimpleQueryStringBuilder extends BaseQueryBuilder implements QueryParser {
    private Map<String, Float> fields = new HashMap<>();
    private String analyzer;
    private Operator operator;
    private final String queryText;
    private String queryName;
    private String minimumShouldMatch;
    private int flags = -1;
    private Boolean lowercaseExpandedTerms;
    private Boolean lenient;
    private Boolean analyzeWildcard;
    private Locale locale;

    public static final String NAME = "simple_query_string";

    /**
     * Operators for the default_operator
     */
    public static enum Operator {
        AND,
        OR
    }
    
    @Inject
    public SimpleQueryStringBuilder(Settings settings) {
        this.queryText = null;
    }
    

    /**
     * Construct a new simple query with the given text
     */
    public SimpleQueryStringBuilder(String text) {
        this.queryText = text;
    }

    /**
     * Add a field to run the query against
     */
    public SimpleQueryStringBuilder field(String field) {
        this.fields.put(field, null);
        return this;
    }

    /**
     * Add a field to run the query against with a specific boost
     */
    public SimpleQueryStringBuilder field(String field, float boost) {
        this.fields.put(field, boost);
        return this;
    }

    /**
     * Specify a name for the query
     */
    public SimpleQueryStringBuilder queryName(String name) {
        this.queryName = name;
        return this;
    }

    /**
     * Specify an analyzer to use for the query
     */
    public SimpleQueryStringBuilder analyzer(String analyzer) {
        this.analyzer = analyzer;
        return this;
    }

    /**
     * Specify the default operator for the query. Defaults to "OR" if no
     * operator is specified
     */
    public SimpleQueryStringBuilder defaultOperator(Operator defaultOperator) {
        this.operator = defaultOperator;
        return this;
    }

    /**
     * Specify the enabled features of the SimpleQueryString.
     */
    public SimpleQueryStringBuilder flags(SimpleQueryStringFlag... flags) {
        int value = 0;
        if (flags.length == 0) {
            value = SimpleQueryStringFlag.ALL.value;
        } else {
            for (SimpleQueryStringFlag flag : flags) {
                value |= flag.value;
            }
        }
        this.flags = value;
        return this;
    }

    public SimpleQueryStringBuilder lowercaseExpandedTerms(boolean lowercaseExpandedTerms) {
        this.lowercaseExpandedTerms = lowercaseExpandedTerms;
        return this;
    }

    public SimpleQueryStringBuilder locale(Locale locale) {
        this.locale = locale;
        return this;
    }

    public SimpleQueryStringBuilder lenient(boolean lenient) {
        this.lenient = lenient;
        return this;
    }

    public SimpleQueryStringBuilder analyzeWildcard(boolean analyzeWildcard) {
        this.analyzeWildcard = analyzeWildcard;
        return this;
    }

    public SimpleQueryStringBuilder minimumShouldMatch(String minimumShouldMatch) {
        this.minimumShouldMatch = minimumShouldMatch;
        return this;
    }

    @Override
    public void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(SimpleQueryStringBuilder.NAME);

        builder.field("query", queryText);

        if (fields.size() > 0) {
            builder.startArray("fields");
            for (Map.Entry<String, Float> entry : fields.entrySet()) {
                String field = entry.getKey();
                Float boost = entry.getValue();
                if (boost != null) {
                    builder.value(field + "^" + boost);
                } else {
                    builder.value(field);
                }
            }
            builder.endArray();
        }

        if (flags != -1) {
            builder.field("flags", flags);
        }

        if (analyzer != null) {
            builder.field("analyzer", analyzer);
        }

        if (operator != null) {
            builder.field("default_operator", operator.name().toLowerCase(Locale.ROOT));
        }

        if (lowercaseExpandedTerms != null) {
            builder.field("lowercase_expanded_terms", lowercaseExpandedTerms);
        }

        if (lenient != null) {
            builder.field("lenient", lenient);
        }

        if (analyzeWildcard != null) {
            builder.field("analyze_wildcard", analyzeWildcard);
        }

        if (locale != null) {
            builder.field("locale", locale.toString());
        }

        if (queryName != null) {
            builder.field("_name", queryName);
        }

        if (minimumShouldMatch != null) {
            builder.field("minimum_should_match", minimumShouldMatch);
        }

        builder.endObject();
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
        String queryName = null;
        String field = null;
        String minimumShouldMatch = null;
        Map<String, Float> fieldsAndWeights = null;
        BooleanClause.Occur defaultOperator = null;
        Analyzer analyzer = null;
        int flags = -1;
        SimpleQueryParser.Settings sqsSettings = new SimpleQueryParser.Settings();

        XContentParser.Token token;
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
                            fieldsAndWeights = new HashMap<>();
                        }

                        if (Regex.isSimpleMatchPattern(fField)) {
                            for (String fieldName : parseContext.mapperService().simpleMatchToIndexNames(fField)) {
                                fieldsAndWeights.put(fieldName, fBoost);
                            }
                        } else {
                            MapperService.SmartNameFieldMappers mappers = parseContext.smartFieldMappers(fField);
                            if (mappers != null && mappers.hasMapper()) {
                                fieldsAndWeights.put(mappers.mapper().names().indexName(), fBoost);
                            } else {
                                fieldsAndWeights.put(fField, fBoost);
                            }
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
                        throw new QueryParsingException(parseContext.index(), "[" + NAME + "] analyzer [" + parser.text() + "] not found");
                    }
                } else if ("field".equals(currentFieldName)) {
                    field = parser.text();
                } else if ("default_operator".equals(currentFieldName) || "defaultOperator".equals(currentFieldName)) {
                    String op = parser.text();
                    if ("or".equalsIgnoreCase(op)) {
                        defaultOperator = BooleanClause.Occur.SHOULD;
                    } else if ("and".equalsIgnoreCase(op)) {
                        defaultOperator = BooleanClause.Occur.MUST;
                    } else {
                        throw new QueryParsingException(parseContext.index(),
                                "[" + NAME + "] default operator [" + op + "] is not allowed");
                    }
                } else if ("flags".equals(currentFieldName)) {
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
                } else if ("locale".equals(currentFieldName)) {
                    String localeStr = parser.text();
                    Locale locale = LocaleUtils.parse(localeStr);
                    sqsSettings.locale(locale);
                } else if ("lowercase_expanded_terms".equals(currentFieldName)) {
                    sqsSettings.lowercaseExpandedTerms(parser.booleanValue());
                } else if ("lenient".equals(currentFieldName)) {
                    sqsSettings.lenient(parser.booleanValue());
                } else if ("analyze_wildcard".equals(currentFieldName)) {
                    sqsSettings.analyzeWildcard(parser.booleanValue());
                } else if ("_name".equals(currentFieldName)) {
                    queryName = parser.text();
                } else if ("minimum_should_match".equals(currentFieldName)) {
                    minimumShouldMatch = parser.textOrNull();
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
        if (fieldsAndWeights == null) {
            field = parseContext.defaultField();
        }

        // Use standard analyzer by default
        if (analyzer == null) {
            analyzer = parseContext.mapperService().searchAnalyzer();
        }

        if (fieldsAndWeights == null) {
            fieldsAndWeights = Collections.singletonMap(field, 1.0F);
        }
        SimpleQueryParser sqp = new SimpleQueryParser(analyzer, fieldsAndWeights, flags, sqsSettings);

        if (defaultOperator != null) {
            sqp.setDefaultOperator(defaultOperator);
        }

        Query query = sqp.parse(queryBody);
        if (queryName != null) {
            parseContext.addNamedQuery(queryName, query);
        }

        if (minimumShouldMatch != null && query instanceof BooleanQuery) {
            Queries.applyMinimumShouldMatch((BooleanQuery) query, minimumShouldMatch);
        }
        return query;
    }
}
