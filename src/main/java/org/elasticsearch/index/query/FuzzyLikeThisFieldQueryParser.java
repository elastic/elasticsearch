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
import org.apache.lucene.sandbox.queries.FuzzyLikeThisQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.analysis.Analysis;
import org.elasticsearch.index.mapper.MapperService;

import java.io.IOException;

import static org.elasticsearch.index.query.support.QueryParsers.wrapSmartNameQuery;

/**
 * <pre>
 * {
 *  fuzzy_like_this_field : {
 *      field1 : {
 *          maxNumTerms : 12,
 *          boost : 1.1,
 *          likeText : "..."
 *      }
 * }
 * </pre>
 */
public class FuzzyLikeThisFieldQueryParser implements QueryParser {

    public static final String NAME = "flt_field";
    private static final Fuzziness DEFAULT_FUZZINESS = Fuzziness.fromSimilarity(0.5f);
    private static final ParseField FUZZINESS = Fuzziness.FIELD.withDeprecation("min_similarity");

    @Inject
    public FuzzyLikeThisFieldQueryParser() {
    }

    @Override
    public String[] names() {
        return new String[]{NAME, "fuzzy_like_this_field", Strings.toCamelCase(NAME), "fuzzyLikeThisField"};
    }

    @Override
    public Query parse(QueryParseContext parseContext) throws IOException, QueryParsingException {
        XContentParser parser = parseContext.parser();

        int maxNumTerms = 25;
        float boost = 1.0f;
        String likeText = null;
        Fuzziness fuzziness = DEFAULT_FUZZINESS;
        int prefixLength = 0;
        boolean ignoreTF = false;
        Analyzer analyzer = null;
        boolean failOnUnsupportedField = true;
        String queryName = null;

        XContentParser.Token token = parser.nextToken();
        if (token != XContentParser.Token.FIELD_NAME) {
            throw new QueryParsingException(parseContext.index(), "[flt_field] query malformed, no field");
        }
        String fieldName = parser.currentName();

        // now, we move after the field name, which starts the object
        token = parser.nextToken();
        if (token != XContentParser.Token.START_OBJECT) {
            throw new QueryParsingException(parseContext.index(), "[flt_field] query malformed, no start_object");
        }


        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if ("like_text".equals(currentFieldName) || "likeText".equals(currentFieldName)) {
                    likeText = parser.text();
                } else if ("max_query_terms".equals(currentFieldName) || "maxQueryTerms".equals(currentFieldName)) {
                    maxNumTerms = parser.intValue();
                } else if ("boost".equals(currentFieldName)) {
                    boost = parser.floatValue();
                } else if ("ignore_tf".equals(currentFieldName) || "ignoreTF".equals(currentFieldName)) {
                    ignoreTF = parser.booleanValue();
                } else if (FUZZINESS.match(currentFieldName, parseContext.parseFlags())) {
                    fuzziness = Fuzziness.parse(parser);
                } else if ("prefix_length".equals(currentFieldName) || "prefixLength".equals(currentFieldName)) {
                    prefixLength = parser.intValue();
                } else if ("analyzer".equals(currentFieldName)) {
                    analyzer = parseContext.analysisService().analyzer(parser.text());
                } else if ("fail_on_unsupported_field".equals(currentFieldName) || "failOnUnsupportedField".equals(currentFieldName)) {
                    failOnUnsupportedField = parser.booleanValue();
                } else if ("_name".equals(currentFieldName)) {
                    queryName = parser.text();
                } else {
                    throw new QueryParsingException(parseContext.index(), "[flt_field] query does not support [" + currentFieldName + "]");
                }
            }
        }

        if (likeText == null) {
            throw new QueryParsingException(parseContext.index(), "fuzzy_like_This_field requires 'like_text' to be specified");
        }

        MapperService.SmartNameFieldMappers smartNameFieldMappers = parseContext.smartFieldMappers(fieldName);
        if (smartNameFieldMappers != null) {
            if (smartNameFieldMappers.hasMapper()) {
                fieldName = smartNameFieldMappers.mapper().names().indexName();
                if (analyzer == null) {
                    analyzer = smartNameFieldMappers.mapper().searchAnalyzer();
                }
            }
        }
        if (analyzer == null) {
            analyzer = parseContext.mapperService().searchAnalyzer();
        }
        if (!Analysis.generatesCharacterTokenStream(analyzer, fieldName)) {
            if (failOnUnsupportedField) {
                throw new ElasticsearchIllegalArgumentException("fuzzy_like_this_field doesn't support binary/numeric fields: [" + fieldName + "]");
            } else {
                return null;
            }
        }

        FuzzyLikeThisQuery fuzzyLikeThisQuery = new FuzzyLikeThisQuery(maxNumTerms, analyzer);
        fuzzyLikeThisQuery.addTerms(likeText, fieldName, fuzziness.asSimilarity(), prefixLength);
        fuzzyLikeThisQuery.setBoost(boost);
        fuzzyLikeThisQuery.setIgnoreTF(ignoreTF);

        // move to the next end object, to close the field name
        token = parser.nextToken();
        if (token != XContentParser.Token.END_OBJECT) {
            throw new QueryParsingException(parseContext.index(), "[flt_field] query malformed, no end_object");
        }
        assert token == XContentParser.Token.END_OBJECT;

        Query query = wrapSmartNameQuery(fuzzyLikeThisQuery, smartNameFieldMappers, parseContext);
        if (queryName != null) {
            parseContext.addNamedQuery(queryName, query);
        }
        return query;
    }
}
