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

package org.elasticsearch.index.query.xcontent;

import org.apache.lucene.search.Query;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.QueryParsingException;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.util.Strings;
import org.elasticsearch.util.collect.Sets;
import org.elasticsearch.util.lucene.search.MoreLikeThisQuery;
import org.elasticsearch.util.settings.Settings;
import org.elasticsearch.util.xcontent.XContentParser;

import java.io.IOException;
import java.util.Set;

import static org.elasticsearch.index.query.support.QueryParsers.*;

/**
 * @author kimchy (shay.banon)
 */
public class MoreLikeThisFieldQueryParser extends AbstractIndexComponent implements XContentQueryParser {

    public static final String NAME = "mlt_field";

    public MoreLikeThisFieldQueryParser(Index index, @IndexSettings Settings indexSettings) {
        super(index, indexSettings);
    }

    @Override public String[] names() {
        return new String[]{NAME, "more_like_this_field", Strings.toCamelCase(NAME), "moreLikeThisField"};
    }

    @Override public Query parse(QueryParseContext parseContext) throws IOException, QueryParsingException {
        XContentParser parser = parseContext.parser();

        XContentParser.Token token = parser.nextToken();
        assert token == XContentParser.Token.FIELD_NAME;
        String fieldName = parser.currentName();

        // now, we move after the field name, which starts the object
        token = parser.nextToken();
        assert token == XContentParser.Token.START_OBJECT;


        MoreLikeThisQuery mltQuery = new MoreLikeThisQuery();
        mltQuery.setSimilarity(parseContext.searchSimilarity());

        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if ("like_text".equals(currentFieldName)) {
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
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                if ("stop_words".equals(currentFieldName) || "stopWords".equals(currentFieldName)) {
                    Set<String> stopWords = Sets.newHashSet();
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        stopWords.add(parser.text());
                    }
                    mltQuery.setStopWords(stopWords);
                }
            }
        }

        if (mltQuery.getLikeText() == null) {
            throw new QueryParsingException(index, "more_like_this_field requires 'like_text' to be specified");
        }

        // move to the next end object, to close the field name
        token = parser.nextToken();
        assert token == XContentParser.Token.END_OBJECT;

        MapperService.SmartNameFieldMappers smartNameFieldMappers = parseContext.smartFieldMappers(fieldName);
        if (smartNameFieldMappers != null) {
            if (smartNameFieldMappers.hasMapper()) {
                fieldName = smartNameFieldMappers.mapper().names().indexName();
                mltQuery.setAnalyzer(smartNameFieldMappers.mapper().searchAnalyzer());
            }
        }
        if (mltQuery.getAnalyzer() == null) {
            mltQuery.setAnalyzer(parseContext.mapperService().searchAnalyzer());
        }
        mltQuery.setMoreLikeFields(new String[]{fieldName});
        return wrapSmartNameQuery(mltQuery, smartNameFieldMappers, parseContext.indexCache());
    }
}
