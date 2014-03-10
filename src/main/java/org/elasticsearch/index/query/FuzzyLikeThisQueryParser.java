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

import com.google.common.collect.Lists;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.sandbox.queries.FuzzyLikeThisQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.analysis.Analysis;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/**
 * <pre>
 * {
 *  fuzzy_like_this : {
 *      maxNumTerms : 12,
 *      boost : 1.1,
 *      fields : ["field1", "field2"]
 *      likeText : "..."
 *  }
 * }
 * </pre>
 */
public class FuzzyLikeThisQueryParser implements QueryParser {

    public static final String NAME = "flt";
    private static final ParseField FUZZINESS = Fuzziness.FIELD.withDeprecation("min_similarity");

    @Inject
    public FuzzyLikeThisQueryParser() {
    }

    @Override
    public String[] names() {
        return new String[]{NAME, "fuzzy_like_this", "fuzzyLikeThis"};
    }

    @Override
    public Query parse(QueryParseContext parseContext) throws IOException, QueryParsingException {
        XContentParser parser = parseContext.parser();

        int maxNumTerms = 25;
        float boost = 1.0f;
        List<String> fields = null;
        String likeText = null;
        Fuzziness fuzziness = Fuzziness.TWO;
        int prefixLength = 0;
        boolean ignoreTF = false;
        Analyzer analyzer = null;
        boolean failOnUnsupportedField = true;
        String queryName = null;

        XContentParser.Token token;
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
                    throw new QueryParsingException(parseContext.index(), "[flt] query does not support [" + currentFieldName + "]");
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                if ("fields".equals(currentFieldName)) {
                    fields = Lists.newLinkedList();
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        fields.add(parseContext.indexName(parser.text()));
                    }
                } else {
                    throw new QueryParsingException(parseContext.index(), "[flt] query does not support [" + currentFieldName + "]");
                }
            }
        }

        if (likeText == null) {
            throw new QueryParsingException(parseContext.index(), "fuzzy_like_this requires 'like_text' to be specified");
        }

        if (analyzer == null) {
            analyzer = parseContext.mapperService().searchAnalyzer();
        }

        FuzzyLikeThisQuery query = new FuzzyLikeThisQuery(maxNumTerms, analyzer);
        if (fields == null) {
            fields = Lists.newArrayList(parseContext.defaultField());
        } else if (fields.isEmpty()) {
            throw new QueryParsingException(parseContext.index(), "fuzzy_like_this requires 'fields' to be non-empty");
        }
        for (Iterator<String> it = fields.iterator(); it.hasNext(); ) {
            final String fieldName = it.next();
            if (!Analysis.generatesCharacterTokenStream(analyzer, fieldName)) {
                if (failOnUnsupportedField) {
                    throw new ElasticsearchIllegalArgumentException("more_like_this doesn't support binary/numeric fields: [" + fieldName + "]");
                } else {
                    it.remove();
                }
            }
        }
        if (fields.isEmpty()) {
            return null;
        }
        float minSimilarity = fuzziness.asFloat();
        if (minSimilarity >= 1.0f && minSimilarity != (int)minSimilarity) {
            throw new ElasticsearchIllegalArgumentException("fractional edit distances are not allowed");
        }
        if (minSimilarity < 0.0f)  {
            throw new ElasticsearchIllegalArgumentException("minimumSimilarity cannot be less than 0");
        }
        for (String field : fields) {
            query.addTerms(likeText, field, minSimilarity, prefixLength);
        }
        query.setBoost(boost);
        query.setIgnoreTF(ignoreTF);

        if (queryName != null) {
            parseContext.addNamedQuery(queryName, query);
        }
        return query;
    }
}