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
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.ExtendedCommonTermsQuery;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.util.BytesRefBuilder;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MapperService;

import java.io.IOException;

/**
 * CommonTermsQuery query is a query that executes high-frequency terms in a
 * optional sub-query to prevent slow queries due to "common" terms like
 * stopwords. This query basically builds 2 queries off the {@link #add(Term)
 * added} terms where low-frequency terms are added to a required boolean clause
 * and high-frequency terms are added to an optional boolean clause. The
 * optional clause is only executed if the required "low-frequency' clause
 * matches. Scores produced by this query will be slightly different to plain
 * {@link BooleanQuery} scorer mainly due to differences in the
 * {@link Similarity#coord(int,int) number of leave queries} in the required
 * boolean clause. In the most cases high-frequency terms are unlikely to
 * significantly contribute to the document score unless at least one of the
 * low-frequency terms are matched such that this query can improve query
 * execution times significantly if applicable.
 * <p>
 */
public class CommonTermsQueryBuilder extends BaseQueryBuilder implements QueryParser, BoostableQueryBuilder<CommonTermsQueryBuilder> {

    public static enum Operator {
        OR, AND
    }

    private String name;

    private Object text;

    private Operator highFreqOperator = null;

    private Operator lowFreqOperator = null;

    private String analyzer = null;

    private Float boost = null;

    private String lowFreqMinimumShouldMatch = null;

    private String highFreqMinimumShouldMatch = null;

    private Boolean disableCoords = null;

    private Float cutoffFrequency = null;

    private String queryName;

    public static final String NAME = "common";

    static final float DEFAULT_MAX_TERM_DOC_FREQ = 0.01f;

    static final Occur DEFAULT_HIGH_FREQ_OCCUR = Occur.SHOULD;

    static final Occur DEFAULT_LOW_FREQ_OCCUR = Occur.SHOULD;

    static final boolean DEFAULT_DISABLE_COORDS = true;

    @Inject
    public CommonTermsQueryBuilder() {
    }

    /**
     * Constructs a new common terms query.
     */
    public CommonTermsQueryBuilder(String name, Object text) {
        if (name == null) {
            throw new ElasticsearchIllegalArgumentException("Field name must not be null");
        }
        if (text == null) {
            throw new ElasticsearchIllegalArgumentException("Query must not be null");
        }
        this.text = text;
        this.name = name;
    }

    /**
     * Sets the operator to use for terms with a high document frequency
     * (greater than or equal to {@link #cutoffFrequency(float)}. Defaults to
     * <tt>AND</tt>.
     */
    public CommonTermsQueryBuilder highFreqOperator(Operator operator) {
        this.highFreqOperator = operator;
        return this;
    }

    /**
     * Sets the operator to use for terms with a low document frequency (less
     * than {@link #cutoffFrequency(float)}. Defaults to <tt>AND</tt>.
     */
    public CommonTermsQueryBuilder lowFreqOperator(Operator operator) {
        this.lowFreqOperator = operator;
        return this;
    }

    /**
     * Explicitly set the analyzer to use. Defaults to use explicit mapping
     * config for the field, or, if not set, the default search analyzer.
     */
    public CommonTermsQueryBuilder analyzer(String analyzer) {
        this.analyzer = analyzer;
        return this;
    }

    /**
     * Set the boost to apply to the query.
     */
    @Override
    public CommonTermsQueryBuilder boost(float boost) {
        this.boost = boost;
        return this;
    }

    /**
     * Sets the cutoff document frequency for high / low frequent terms. A value
     * in [0..1] (or absolute number >=1) representing the maximum threshold of
     * a terms document frequency to be considered a low frequency term.
     * Defaults to
     * <tt>{@value CommonTermsQueryParser#DEFAULT_MAX_TERM_DOC_FREQ}</tt>
     */
    public CommonTermsQueryBuilder cutoffFrequency(float cutoffFrequency) {
        this.cutoffFrequency = cutoffFrequency;
        return this;
    }

    /**
     * Sets the minimum number of high frequent query terms that need to match in order to
     * produce a hit when there are no low frequen terms.
     */
    public CommonTermsQueryBuilder highFreqMinimumShouldMatch(String highFreqMinimumShouldMatch) {
        this.highFreqMinimumShouldMatch = highFreqMinimumShouldMatch;
        return this;
    }

    /**
     * Sets the minimum number of low frequent query terms that need to match in order to
     * produce a hit.
     */
    public CommonTermsQueryBuilder lowFreqMinimumShouldMatch(String lowFreqMinimumShouldMatch) {
        this.lowFreqMinimumShouldMatch = lowFreqMinimumShouldMatch;
        return this;
    }

    /**
     * Sets the query name for the filter that can be used when searching for matched_filters per hit.
     */
    public CommonTermsQueryBuilder queryName(String queryName) {
        this.queryName = queryName;
        return this;
    }

    @Override
    public void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(CommonTermsQueryBuilder.NAME);
        builder.startObject(name);

        builder.field("query", text);
        if (disableCoords != null) {
            builder.field("disable_coords", disableCoords);
        }
        if (highFreqOperator != null) {
            builder.field("high_freq_operator", highFreqOperator.toString());
        }
        if (lowFreqOperator != null) {
            builder.field("low_freq_operator", lowFreqOperator.toString());
        }
        if (analyzer != null) {
            builder.field("analyzer", analyzer);
        }
        if (boost != null) {
            builder.field("boost", boost);
        }
        if (cutoffFrequency != null) {
            builder.field("cutoff_frequency", cutoffFrequency);
        }
        if (lowFreqMinimumShouldMatch != null || highFreqMinimumShouldMatch != null) {
            builder.startObject("minimum_should_match");
            if (lowFreqMinimumShouldMatch != null) {
                builder.field("low_freq", lowFreqMinimumShouldMatch);
            }
            if (highFreqMinimumShouldMatch != null) {
                builder.field("high_freq", highFreqMinimumShouldMatch);
            }
            builder.endObject();
        }
        if (queryName != null) {
            builder.field("_name", queryName);
        }

        builder.endObject();
        builder.endObject();
    }

    @Override
    public String[] names() {
        return new String[] { NAME };
    }

    @Override
    public Query parse(QueryParseContext parseContext) throws IOException, QueryParsingException {
        XContentParser parser = parseContext.parser();
        XContentParser.Token token = parser.nextToken();
        if (token != XContentParser.Token.FIELD_NAME) {
            throw new QueryParsingException(parseContext.index(), "[common] query malformed, no field");
        }
        String fieldName = parser.currentName();
        Object value = null;
        float boost = 1.0f;
        String queryAnalyzer = null;
        String lowFreqMinimumShouldMatch = null;
        String highFreqMinimumShouldMatch = null;
        boolean disableCoords = DEFAULT_DISABLE_COORDS;
        Occur highFreqOccur = DEFAULT_HIGH_FREQ_OCCUR;
        Occur lowFreqOccur = DEFAULT_LOW_FREQ_OCCUR;
        float maxTermFrequency = DEFAULT_MAX_TERM_DOC_FREQ;
        String queryName = null;
        token = parser.nextToken();
        if (token == XContentParser.Token.START_OBJECT) {
            String currentFieldName = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token == XContentParser.Token.START_OBJECT) {
                    if ("minimum_should_match".equals(currentFieldName) || "minimumShouldMatch".equals(currentFieldName)) {
                        String innerFieldName = null;
                        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                            if (token == XContentParser.Token.FIELD_NAME) {
                                innerFieldName = parser.currentName();
                            } else if (token.isValue()) {
                                if ("low_freq".equals(innerFieldName) || "lowFreq".equals(innerFieldName)) {
                                    lowFreqMinimumShouldMatch = parser.text();
                                } else if ("high_freq".equals(innerFieldName) || "highFreq".equals(innerFieldName)) {
                                    highFreqMinimumShouldMatch = parser.text();
                                } else {
                                    throw new QueryParsingException(parseContext.index(), "[common] query does not support [" + innerFieldName + "] for [" + currentFieldName + "]");
                                }
                            }
                        }
                    } else {
                        throw new QueryParsingException(parseContext.index(), "[common] query does not support [" + currentFieldName + "]");
                    }
                } else if (token.isValue()) {
                    if ("query".equals(currentFieldName)) {
                        value = parser.objectText();
                    } else if ("analyzer".equals(currentFieldName)) {
                        String analyzer = parser.text();
                        if (parseContext.analysisService().analyzer(analyzer) == null) {
                            throw new QueryParsingException(parseContext.index(), "[common] analyzer [" + parser.text() + "] not found");
                        }
                        queryAnalyzer = analyzer;
                    } else if ("disable_coord".equals(currentFieldName) || "disableCoord".equals(currentFieldName)) {
                        disableCoords = parser.booleanValue();
                    } else if ("boost".equals(currentFieldName)) {
                        boost = parser.floatValue();
                    } else if ("high_freq_operator".equals(currentFieldName) || "highFreqOperator".equals(currentFieldName)) {
                        String op = parser.text();
                        if ("or".equalsIgnoreCase(op)) {
                            highFreqOccur = BooleanClause.Occur.SHOULD;
                        } else if ("and".equalsIgnoreCase(op)) {
                            highFreqOccur = BooleanClause.Occur.MUST;
                        } else {
                            throw new QueryParsingException(parseContext.index(),
                                    "[common] query requires operator to be either 'and' or 'or', not [" + op + "]");
                        }
                    } else if ("low_freq_operator".equals(currentFieldName) || "lowFreqOperator".equals(currentFieldName)) {
                        String op = parser.text();
                        if ("or".equalsIgnoreCase(op)) {
                            lowFreqOccur = BooleanClause.Occur.SHOULD;
                        } else if ("and".equalsIgnoreCase(op)) {
                            lowFreqOccur = BooleanClause.Occur.MUST;
                        } else {
                            throw new QueryParsingException(parseContext.index(),
                                    "[common] query requires operator to be either 'and' or 'or', not [" + op + "]");
                        }
                    } else if ("minimum_should_match".equals(currentFieldName) || "minimumShouldMatch".equals(currentFieldName)) {
                        lowFreqMinimumShouldMatch = parser.text();
                    } else if ("cutoff_frequency".equals(currentFieldName)) {
                        maxTermFrequency = parser.floatValue();
                    } else if ("_name".equals(currentFieldName)) {
                        queryName = parser.text();
                    } else {
                        throw new QueryParsingException(parseContext.index(), "[common] query does not support [" + currentFieldName + "]");
                    }
                }
            }
            parser.nextToken();
        } else {
            value = parser.objectText();
            // move to the next token
            token = parser.nextToken();
            if (token != XContentParser.Token.END_OBJECT) {
                throw new QueryParsingException(
                        parseContext.index(),
                        "[common] query parsed in simplified form, with direct field name, but included more options than just the field name, possibly use its 'options' form, with 'query' element?");
            }
        }

        if (value == null) {
            throw new QueryParsingException(parseContext.index(), "No text specified for text query");
        }
        FieldMapper<?> mapper = null;
        String field;
        MapperService.SmartNameFieldMappers smartNameFieldMappers = parseContext.smartFieldMappers(fieldName);
        if (smartNameFieldMappers != null && smartNameFieldMappers.hasMapper()) {
            mapper = smartNameFieldMappers.mapper();
            field = mapper.names().indexName();
        } else {
            field = fieldName;
        }

        Analyzer analyzer = null;
        if (queryAnalyzer == null) {
            if (mapper != null) {
                analyzer = mapper.searchAnalyzer();
            }
            if (analyzer == null && smartNameFieldMappers != null) {
                analyzer = smartNameFieldMappers.searchAnalyzer();
            }
            if (analyzer == null) {
                analyzer = parseContext.mapperService().searchAnalyzer();
            }
        } else {
            analyzer = parseContext.mapperService().analysisService().analyzer(queryAnalyzer);
            if (analyzer == null) {
                throw new ElasticsearchIllegalArgumentException("No analyzer found for [" + queryAnalyzer + "]");
            }
        }

        ExtendedCommonTermsQuery commonsQuery = new ExtendedCommonTermsQuery(highFreqOccur, lowFreqOccur, maxTermFrequency, disableCoords, mapper);
        commonsQuery.setBoost(boost);
        Query query = parseQueryString(commonsQuery, value.toString(), field, parseContext, analyzer, lowFreqMinimumShouldMatch, highFreqMinimumShouldMatch, smartNameFieldMappers);
        if (queryName != null) {
            parseContext.addNamedQuery(queryName, query);
        }
        return query;
    }


    private final Query parseQueryString(ExtendedCommonTermsQuery query, String queryString, String field, QueryParseContext parseContext,
            Analyzer analyzer, String lowFreqMinimumShouldMatch, String highFreqMinimumShouldMatch, MapperService.SmartNameFieldMappers smartNameFieldMappers) throws IOException {
        // Logic similar to QueryParser#getFieldQuery
        int count = 0;
        try (TokenStream source = analyzer.tokenStream(field, queryString.toString())) {
            source.reset();
            CharTermAttribute termAtt = source.addAttribute(CharTermAttribute.class);
            BytesRefBuilder builder = new BytesRefBuilder();
            while (source.incrementToken()) {
                // UTF-8
                builder.copyChars(termAtt);
                query.add(new Term(field, builder.toBytesRef()));
                count++;
            }
        }

        if (count == 0) {
            return null;
        }
        query.setLowFreqMinimumNumberShouldMatch(lowFreqMinimumShouldMatch);
        query.setHighFreqMinimumNumberShouldMatch(highFreqMinimumShouldMatch);
        return query;
    }
}
