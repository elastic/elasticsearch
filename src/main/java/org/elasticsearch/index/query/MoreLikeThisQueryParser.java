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
import com.google.common.collect.Sets;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.search.Query;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.search.MoreLikeThisQuery;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.analysis.Analysis;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 *
 */
public class MoreLikeThisQueryParser implements QueryParser {

    public static final String NAME = "mlt";
    
    
    public static class Fields {
        public static final ParseField LIKE_TEXT = new ParseField("like_text");
        public static final ParseField MIN_TERM_FREQ = new ParseField("min_term_freq");
        public static final ParseField MAX_QUERY_TERMS = new ParseField("max_query_terms");
        public static final ParseField MIN_WORD_LENGTH = new ParseField("min_word_length", "min_word_len");
        public static final ParseField MAX_WORD_LENGTH = new ParseField("max_word_length", "max_word_len");
        public static final ParseField MIN_DOC_FREQ = new ParseField("min_doc_freq");
        public static final ParseField MAX_DOC_FREQ = new ParseField("max_doc_freq");
        public static final ParseField BOOST_TERMS = new ParseField("boost_terms");
        public static final ParseField PERCENT_TERMS_TO_MATCH = new ParseField("percent_terms_to_match");
        public static final ParseField FAIL_ON_UNSUPPORTED_FIELD = new ParseField("fail_on_unsupported_field");
        public static final ParseField STOP_WORDS = new ParseField("stop_words");
    }    

    @Inject
    public MoreLikeThisQueryParser() {
    }

    @Override
    public String[] names() {
        return new String[]{NAME, "more_like_this", "moreLikeThis"};
    }

    @Override
    public Query parse(QueryParseContext parseContext) throws IOException, QueryParsingException {
        XContentParser parser = parseContext.parser();

        MoreLikeThisQuery mltQuery = new MoreLikeThisQuery();
        mltQuery.setSimilarity(parseContext.searchSimilarity());
        Analyzer analyzer = null;
        List<String> moreLikeFields = null;
        boolean failOnUnsupportedField = true;
        String queryName = null;

        XContentParser.Token token;
        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if (Fields.LIKE_TEXT.match(currentFieldName, parseContext.parseFlags())) {
                    mltQuery.setLikeText(parser.text());
                } else if (Fields.MIN_TERM_FREQ.match(currentFieldName, parseContext.parseFlags())) {
                    mltQuery.setMinTermFrequency(parser.intValue());
                } else if (Fields.MAX_QUERY_TERMS.match(currentFieldName, parseContext.parseFlags())) {
                    mltQuery.setMaxQueryTerms(parser.intValue());
                } else if (Fields.MIN_DOC_FREQ.match(currentFieldName, parseContext.parseFlags())) {
                    mltQuery.setMinDocFreq(parser.intValue());
                } else if (Fields.MAX_DOC_FREQ.match(currentFieldName, parseContext.parseFlags())) {
                    mltQuery.setMaxDocFreq(parser.intValue());
                } else if (Fields.MIN_WORD_LENGTH.match(currentFieldName, parseContext.parseFlags())) {
                    mltQuery.setMinWordLen(parser.intValue());
                } else if (Fields.MAX_WORD_LENGTH.match(currentFieldName, parseContext.parseFlags())) {
                    mltQuery.setMaxWordLen(parser.intValue());
                } else if (Fields.BOOST_TERMS.match(currentFieldName, parseContext.parseFlags())) {
                    float boostFactor = parser.floatValue();
                    if (boostFactor != 0) {
                        mltQuery.setBoostTerms(true);
                        mltQuery.setBoostTermsFactor(boostFactor);
                    }
                } else if (Fields.PERCENT_TERMS_TO_MATCH.match(currentFieldName, parseContext.parseFlags())) {
                    mltQuery.setPercentTermsToMatch(parser.floatValue());
                } else if ("analyzer".equals(currentFieldName)) {
                    analyzer = parseContext.analysisService().analyzer(parser.text());
                } else if ("boost".equals(currentFieldName)) {
                    mltQuery.setBoost(parser.floatValue());
                } else if (Fields.FAIL_ON_UNSUPPORTED_FIELD.match(currentFieldName, parseContext.parseFlags())) {
                    failOnUnsupportedField = parser.booleanValue();
                } else if ("_name".equals(currentFieldName)) {
                    queryName = parser.text();
                } else {
                    throw new QueryParsingException(parseContext.index(), "[mlt] query does not support [" + currentFieldName + "]");
                }
       } else if (token == XContentParser.Token.START_ARRAY) {
                if (Fields.STOP_WORDS.match(currentFieldName, parseContext.parseFlags())) {
                    Set<String> stopWords = Sets.newHashSet();
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        stopWords.add(parser.text());
                    }
                    mltQuery.setStopWords(stopWords);
                } else if ("fields".equals(currentFieldName)) {
                    moreLikeFields = Lists.newLinkedList();
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        moreLikeFields.add(parseContext.indexName(parser.text()));
                    }
                } else {
                    throw new QueryParsingException(parseContext.index(), "[mlt] query does not support [" + currentFieldName + "]");
                }
            }
        }

        if (mltQuery.getLikeText() == null) {
            throw new QueryParsingException(parseContext.index(), "more_like_this requires 'like_text' to be specified");
        }

        if (analyzer == null) {
            analyzer = parseContext.mapperService().searchAnalyzer();
        }
        mltQuery.setAnalyzer(analyzer);

        if (moreLikeFields == null) {
            moreLikeFields = Lists.newArrayList(parseContext.defaultField());
        } else if (moreLikeFields.isEmpty()) {
            throw new QueryParsingException(parseContext.index(), "more_like_this requires 'fields' to be non-empty");
        }

        for (Iterator<String> it = moreLikeFields.iterator(); it.hasNext(); ) {
            final String fieldName = it.next();
            if (!Analysis.generatesCharacterTokenStream(analyzer, fieldName)) {
                if (failOnUnsupportedField) {
                    throw new ElasticsearchIllegalArgumentException("more_like_this doesn't support binary/numeric fields: [" + fieldName + "]");
                } else {
                    it.remove();
                }
            }
        }
        if (moreLikeFields.isEmpty()) {
            return null;
        }
        mltQuery.setMoreLikeFields(moreLikeFields.toArray(Strings.EMPTY_ARRAY));
        if (queryName != null) {
            parseContext.addNamedQuery(queryName, mltQuery);
        }
        return mltQuery;
    }
}