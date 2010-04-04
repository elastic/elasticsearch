/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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

package org.elasticsearch.index.query.json;

import com.google.common.collect.Sets;
import org.apache.lucene.search.Query;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.mapper.AllFieldMapper;
import org.elasticsearch.index.query.QueryParsingException;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.util.lucene.search.MoreLikeThisQuery;
import org.elasticsearch.util.settings.Settings;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import static com.google.common.collect.Lists.*;

/**
 * @author kimchy (shay.banon)
 */
public class MoreLikeThisJsonQueryParser extends AbstractIndexComponent implements JsonQueryParser {

    public static final String NAME = "mlt";

    public MoreLikeThisJsonQueryParser(Index index, @IndexSettings Settings indexSettings) {
        super(index, indexSettings);
    }

    @Override public String[] names() {
        return new String[]{NAME, "more_like_this"};
    }

    @Override public Query parse(JsonQueryParseContext parseContext) throws IOException, QueryParsingException {
        JsonParser jp = parseContext.jp();

        MoreLikeThisQuery mltQuery = new MoreLikeThisQuery();
        mltQuery.setMoreLikeFields(new String[]{AllFieldMapper.NAME});
        mltQuery.setSimilarity(parseContext.searchSimilarity());

        JsonToken token;
        String currentFieldName = null;
        while ((token = jp.nextToken()) != JsonToken.END_OBJECT) {
            if (token == JsonToken.FIELD_NAME) {
                currentFieldName = jp.getCurrentName();
            } else if (token == JsonToken.VALUE_STRING) {
                if ("like_text".equals(currentFieldName)) {
                    mltQuery.setLikeText(jp.getText());
                } else if ("min_term_freq".equals(currentFieldName)) {
                    mltQuery.setMinTermFrequency(Integer.parseInt(jp.getText()));
                } else if ("max_query_terms".equals(currentFieldName)) {
                    mltQuery.setMaxQueryTerms(Integer.parseInt(jp.getText()));
                } else if ("min_doc_freq".equals(currentFieldName)) {
                    mltQuery.setMinDocFreq(Integer.parseInt(jp.getText()));
                } else if ("max_doc_freq".equals(currentFieldName)) {
                    mltQuery.setMaxDocFreq(Integer.parseInt(jp.getText()));
                } else if ("min_word_len".equals(currentFieldName)) {
                    mltQuery.setMinWordLen(Integer.parseInt(jp.getText()));
                } else if ("max_word_len".equals(currentFieldName)) {
                    mltQuery.setMaxWordLen(Integer.parseInt(jp.getText()));
                } else if ("boost_terms".equals(currentFieldName)) {
                    mltQuery.setBoostTerms(true);
                    mltQuery.setBoostTermsFactor(Float.parseFloat(jp.getText()));
                } else if ("percent_terms_to_match".equals(currentFieldName)) {
                    mltQuery.setPercentTermsToMatch(Float.parseFloat(jp.getText()));
                }
            } else if (token == JsonToken.VALUE_NUMBER_INT) {
                if ("min_term_freq".equals(currentFieldName)) {
                    mltQuery.setMinTermFrequency(jp.getIntValue());
                } else if ("max_query_terms".equals(currentFieldName)) {
                    mltQuery.setMaxQueryTerms(jp.getIntValue());
                } else if ("min_doc_freq".equals(currentFieldName)) {
                    mltQuery.setMinDocFreq(jp.getIntValue());
                } else if ("max_doc_freq".equals(currentFieldName)) {
                    mltQuery.setMaxDocFreq(jp.getIntValue());
                } else if ("min_word_len".equals(currentFieldName)) {
                    mltQuery.setMinWordLen(jp.getIntValue());
                } else if ("max_word_len".equals(currentFieldName)) {
                    mltQuery.setMaxWordLen(jp.getIntValue());
                } else if ("boost_terms".equals(currentFieldName)) {
                    mltQuery.setBoostTerms(true);
                    mltQuery.setBoostTermsFactor(jp.getIntValue());
                } else if ("percent_terms_to_match".equals(currentFieldName)) {
                    mltQuery.setPercentTermsToMatch(jp.getIntValue());
                } else if ("boost".equals(currentFieldName)) {
                    mltQuery.setBoost(jp.getIntValue());
                }
            } else if (token == JsonToken.VALUE_NUMBER_FLOAT) {
                if ("boost_terms".equals(currentFieldName)) {
                    mltQuery.setBoostTerms(true);
                    mltQuery.setBoostTermsFactor(jp.getFloatValue());
                } else if ("percent_terms_to_match".equals(currentFieldName)) {
                    mltQuery.setPercentTermsToMatch(jp.getFloatValue());
                } else if ("boost".equals(currentFieldName)) {
                    mltQuery.setBoost(jp.getFloatValue());
                }
            } else if (token == JsonToken.START_ARRAY) {
                if ("stop_words".equals(currentFieldName)) {
                    Set<String> stopWords = Sets.newHashSet();
                    while ((token = jp.nextToken()) != JsonToken.END_ARRAY) {
                        stopWords.add(jp.getText());
                    }
                    mltQuery.setStopWords(stopWords);
                } else if ("fields".equals(currentFieldName)) {
                    List<String> fields = newArrayList();
                    while ((token = jp.nextToken()) != JsonToken.END_ARRAY) {
                        fields.add(parseContext.indexName(jp.getText()));
                    }
                    mltQuery.setMoreLikeFields(fields.toArray(new String[fields.size()]));
                }
            }
        }

        if (mltQuery.getLikeText() == null) {
            throw new QueryParsingException(index, "more_like_this requires 'like_text' to be specified");
        }
        if (mltQuery.getMoreLikeFields() == null || mltQuery.getMoreLikeFields().length == 0) {
            throw new QueryParsingException(index, "more_like_this requires 'fields' to be specified");
        }

        // move to the next end object, to close the field name
        token = jp.nextToken();
        assert token == JsonToken.END_OBJECT;

        mltQuery.setAnalyzer(parseContext.mapperService().searchAnalyzer());
        return mltQuery;
    }
}