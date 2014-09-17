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

import com.google.common.collect.Sets;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.search.Query;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.search.MoreLikeThisQuery;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.analysis.Analysis;
import org.elasticsearch.index.mapper.MapperService;

import java.io.IOException;
import java.util.Set;

import static org.elasticsearch.index.query.support.QueryParsers.wrapSmartNameQuery;

/**
 *
 */
public class MoreLikeThisFieldQueryParser implements QueryParser {

    public static final String NAME = "mlt_field";

    @Inject
    public MoreLikeThisFieldQueryParser() {
    }

    @Override
    public String[] names() {
        return new String[]{NAME, "more_like_this_field", Strings.toCamelCase(NAME), "moreLikeThisField"};
    }

    
    @Override
    public Query parse(QueryParseContext parseContext) throws IOException, QueryParsingException {
        XContentParser parser = parseContext.parser();

        XContentParser.Token token = parser.nextToken();
        assert token == XContentParser.Token.FIELD_NAME;
        String fieldName = parser.currentName();

        // now, we move after the field name, which starts the object
        token = parser.nextToken();
        assert token == XContentParser.Token.START_OBJECT;


        MoreLikeThisQuery mltQuery = new MoreLikeThisQuery();
        mltQuery.setSimilarity(parseContext.searchSimilarity());
        Analyzer analyzer = null;
        boolean failOnUnsupportedField = true;
        String queryName = null;

        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if (MoreLikeThisQueryParser.Fields.LIKE_TEXT.match(currentFieldName,parseContext.parseFlags()) ) {
                    mltQuery.setLikeText(parser.text());
                } else if (MoreLikeThisQueryParser.Fields.MIN_TERM_FREQ.match(currentFieldName,parseContext.parseFlags()) ) {
                    mltQuery.setMinTermFrequency(parser.intValue());
                } else if (MoreLikeThisQueryParser.Fields.MAX_QUERY_TERMS.match(currentFieldName,parseContext.parseFlags())) {
                    mltQuery.setMaxQueryTerms(parser.intValue());
                } else if (MoreLikeThisQueryParser.Fields.MIN_DOC_FREQ.match(currentFieldName,parseContext.parseFlags())) {
                    mltQuery.setMinDocFreq(parser.intValue());
                } else if (MoreLikeThisQueryParser.Fields.MAX_DOC_FREQ.match(currentFieldName,parseContext.parseFlags())) {
                    mltQuery.setMaxDocFreq(parser.intValue());
                } else if (MoreLikeThisQueryParser.Fields.MIN_WORD_LENGTH.match(currentFieldName,parseContext.parseFlags())) {
                    mltQuery.setMinWordLen(parser.intValue());
                } else if (MoreLikeThisQueryParser.Fields.MAX_WORD_LENGTH.match(currentFieldName,parseContext.parseFlags())) {
                    mltQuery.setMaxWordLen(parser.intValue());
                } else if (MoreLikeThisQueryParser.Fields.BOOST_TERMS.match(currentFieldName,parseContext.parseFlags())) {
                    float boostFactor = parser.floatValue();
                    if (boostFactor != 0) {
                        mltQuery.setBoostTerms(true);
                        mltQuery.setBoostTermsFactor(boostFactor);
                    }
                } else if (MoreLikeThisQueryParser.Fields.PERCENT_TERMS_TO_MATCH.match(currentFieldName,parseContext.parseFlags())) {
                    mltQuery.setPercentTermsToMatch(parser.floatValue());
                } else if ("analyzer".equals(currentFieldName)) {
                    analyzer = parseContext.analysisService().analyzer(parser.text());
                } else if ("boost".equals(currentFieldName)) {
                    mltQuery.setBoost(parser.floatValue());
                } else if (MoreLikeThisQueryParser.Fields.FAIL_ON_UNSUPPORTED_FIELD.match(currentFieldName,parseContext.parseFlags())) {
                    failOnUnsupportedField = parser.booleanValue();
                } else if ("_name".equals(currentFieldName)) {
                    queryName = parser.text();
                } else {
                    throw new QueryParsingException(parseContext.index(), "[mlt_field] query does not support [" + currentFieldName + "]");
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                if (MoreLikeThisQueryParser.Fields.STOP_WORDS.match(currentFieldName,parseContext.parseFlags())) {

                    Set<String> stopWords = Sets.newHashSet();
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        stopWords.add(parser.text());
                    }
                    mltQuery.setStopWords(stopWords);
                } else {
                    throw new QueryParsingException(parseContext.index(), "[mlt_field] query does not support [" + currentFieldName + "]");
                }
            }
        }

        if (mltQuery.getLikeText() == null) {
            throw new QueryParsingException(parseContext.index(), "more_like_this_field requires 'like_text' to be specified");
        }

        // move to the next end object, to close the field name
        token = parser.nextToken();
        assert token == XContentParser.Token.END_OBJECT;

        MapperService.SmartNameFieldMappers smartNameFieldMappers = parseContext.smartFieldMappers(fieldName);
        if (smartNameFieldMappers != null) {
            if (smartNameFieldMappers.hasMapper()) {
                fieldName = smartNameFieldMappers.mapper().names().indexName();
            }
            if (analyzer == null) {
                analyzer = smartNameFieldMappers.searchAnalyzer();
            }
        }
        if (analyzer == null) {
            analyzer = parseContext.mapperService().searchAnalyzer();
        }
        if (!Analysis.generatesCharacterTokenStream(analyzer, fieldName)) {
            if (failOnUnsupportedField) {
                throw new ElasticsearchIllegalArgumentException("more_like_this_field doesn't support binary/numeric fields: [" + fieldName + "]");
            } else {
                return null;
            }
        }
        mltQuery.setAnalyzer(analyzer);
        mltQuery.setMoreLikeFields(new String[]{fieldName});
        Query query = wrapSmartNameQuery(mltQuery, smartNameFieldMappers, parseContext);
        if (queryName != null) {
            parseContext.addNamedQuery(queryName, query);
        }
        return query;
    }
}
