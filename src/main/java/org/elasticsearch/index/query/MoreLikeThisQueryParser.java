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

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.search.Query;
import org.elasticsearch.ElasticSearchIllegalArgumentException;
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
                if ("like_text".equals(currentFieldName) || "likeText".equals(currentFieldName)) {
                    mltQuery.setLikeText(parser.text());
                } else if ("min_term_freq".equals(currentFieldName) || "minTermFreq".equals(currentFieldName)) {
                    mltQuery.setMinTermFrequency(parser.intValue());
                } else if ("max_query_terms".equals(currentFieldName) || "maxQueryTerms".equals(currentFieldName)) {
                    mltQuery.setMaxQueryTerms(parser.intValue());
                } else if ("min_doc_freq".equals(currentFieldName) || "minDocFreq".equals(currentFieldName)) {
                    mltQuery.setMinDocFreq(parser.intValue());
                } else if ("max_doc_freq".equals(currentFieldName) || "maxDocFreq".equals(currentFieldName)) {
                    mltQuery.setMaxDocFreq(parser.intValue());
                } else if ("min_word_len".equals(currentFieldName) || "minWordLen".equals(currentFieldName)) {
                    mltQuery.setMinWordLen(parser.intValue());
                } else if ("max_word_len".equals(currentFieldName) || "maxWordLen".equals(currentFieldName)) {
                    mltQuery.setMaxWordLen(parser.intValue());
                } else if ("boost_terms".equals(currentFieldName) || "boostTerms".equals(currentFieldName)) {
                    mltQuery.setBoostTerms(true);
                    mltQuery.setBoostTermsFactor(parser.floatValue());
                } else if ("percent_terms_to_match".equals(currentFieldName) || "percentTermsToMatch".equals(currentFieldName)) {
                    mltQuery.setPercentTermsToMatch(parser.floatValue());
                } else if ("analyzer".equals(currentFieldName)) {
                    analyzer = parseContext.analysisService().analyzer(parser.text());
                } else if ("boost".equals(currentFieldName)) {
                    mltQuery.setBoost(parser.floatValue());
                } else if ("fail_on_unsupported_field".equals(currentFieldName) || "failOnUnsupportedField".equals(currentFieldName)) {
                    failOnUnsupportedField = parser.booleanValue();
                } else if ("_name".equals(currentFieldName)) {
                    queryName = parser.text();
                } else {
                    throw new QueryParsingException(parseContext.index(), "[mlt] query does not support [" + currentFieldName + "]");
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                if ("stop_words".equals(currentFieldName) || "stopWords".equals(currentFieldName)) {
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
                    throw new ElasticSearchIllegalArgumentException("more_like_this doesn't support binary/numeric fields: [" + fieldName + "]");
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