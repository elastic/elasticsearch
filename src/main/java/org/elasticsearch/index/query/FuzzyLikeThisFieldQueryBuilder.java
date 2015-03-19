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
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.analysis.Analysis;
import org.elasticsearch.index.mapper.MapperService;

import java.io.IOException;

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
public class FuzzyLikeThisFieldQueryBuilder extends BaseQueryBuilder implements QueryParser, BoostableQueryBuilder<FuzzyLikeThisFieldQueryBuilder> {

    public static final String NAME = "flt_field";
    private static final Fuzziness DEFAULT_FUZZINESS = Fuzziness.fromSimilarity(0.5f);
    private static final ParseField FUZZINESS = Fuzziness.FIELD.withDeprecation("min_similarity");

    private String name;

    private Float boost;

    private String likeText = null;
    private Fuzziness fuzziness;
    private Integer prefixLength;
    private Integer maxQueryTerms;
    private Boolean ignoreTF;
    private String analyzer;
    private Boolean failOnUnsupportedField;
    private String queryName;

    @Inject
    public FuzzyLikeThisFieldQueryBuilder() {
    }

    /**
     * A fuzzy more like this query on the provided field.
     *
     * @param name the name of the field
     */
    public FuzzyLikeThisFieldQueryBuilder(String name) {
        this.name = name;
    }

    /**
     * The text to use in order to find documents that are "like" this.
     */
    public FuzzyLikeThisFieldQueryBuilder likeText(String likeText) {
        this.likeText = likeText;
        return this;
    }

    public FuzzyLikeThisFieldQueryBuilder fuzziness(Fuzziness fuzziness) {
        this.fuzziness = fuzziness;
        return this;
    }

    public FuzzyLikeThisFieldQueryBuilder prefixLength(int prefixLength) {
        this.prefixLength = prefixLength;
        return this;
    }

    public FuzzyLikeThisFieldQueryBuilder maxQueryTerms(int maxQueryTerms) {
        this.maxQueryTerms = maxQueryTerms;
        return this;
    }

    public FuzzyLikeThisFieldQueryBuilder ignoreTF(boolean ignoreTF) {
        this.ignoreTF = ignoreTF;
        return this;
    }

    /**
     * The analyzer that will be used to analyze the text. Defaults to the analyzer associated with the field.
     */
    public FuzzyLikeThisFieldQueryBuilder analyzer(String analyzer) {
        this.analyzer = analyzer;
        return this;
    }

    @Override
    public FuzzyLikeThisFieldQueryBuilder boost(float boost) {
        this.boost = boost;
        return this;
    }

    /**
     * Whether to fail or return no result when this query is run against a field which is not supported such as binary/numeric fields.
     */
    public FuzzyLikeThisFieldQueryBuilder failOnUnsupportedField(boolean fail) {
        failOnUnsupportedField = fail;
        return this;
    }

    /**
     * Sets the query name for the filter that can be used when searching for matched_filters per hit.
     */
    public FuzzyLikeThisFieldQueryBuilder queryName(String queryName) {
        this.queryName = queryName;
        return this;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(FuzzyLikeThisFieldQueryBuilder.NAME);
        builder.startObject(name);
        if (likeText == null) {
            throw new ElasticsearchIllegalArgumentException("fuzzyLikeThis requires 'likeText' to be provided");
        }
        builder.field("like_text", likeText);
        if (maxQueryTerms != null) {
            builder.field("max_query_terms", maxQueryTerms);
        }
        if (fuzziness != null) {
            fuzziness.toXContent(builder, params);
        }
        if (prefixLength != null) {
            builder.field("prefix_length", prefixLength);
        }
        if (ignoreTF != null) {
            builder.field("ignore_tf", ignoreTF);
        }
        if (boost != null) {
            builder.field("boost", boost);
        }
        if (analyzer != null) {
            builder.field("analyzer", analyzer);
        }
        if (failOnUnsupportedField != null) {
            builder.field("fail_on_unsupported_field", failOnUnsupportedField);
        }
        if (queryName != null) {
            builder.field("_name", queryName);
        }
        builder.endObject();
        builder.endObject();
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

        if (queryName != null) {
            parseContext.addNamedQuery(queryName, fuzzyLikeThisQuery);
        }
        return fuzzyLikeThisQuery;
    }
}
