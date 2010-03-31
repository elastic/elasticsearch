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
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.QueryParsingException;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.util.Booleans;
import org.elasticsearch.util.lucene.search.MoreLikeThisQuery;
import org.elasticsearch.util.settings.Settings;

import java.io.IOException;
import java.util.Set;

import static org.elasticsearch.index.query.support.QueryParsers.*;

/**
 * @author kimchy (shay.banon)
 */
public class MoreLikeThisFieldJsonQueryParser extends AbstractIndexComponent implements JsonQueryParser {

    public static final String NAME = "moreLikeThisField";

    public MoreLikeThisFieldJsonQueryParser(Index index, @IndexSettings Settings indexSettings) {
        super(index, indexSettings);
    }

    @Override public String name() {
        return NAME;
    }

    @Override public Query parse(JsonQueryParseContext parseContext) throws IOException, QueryParsingException {
        JsonParser jp = parseContext.jp();

        JsonToken token = jp.nextToken();
        assert token == JsonToken.FIELD_NAME;
        String fieldName = jp.getCurrentName();

        // now, we move after the field name, which starts the object
        token = jp.nextToken();
        assert token == JsonToken.START_OBJECT;


        MoreLikeThisQuery mltQuery = new MoreLikeThisQuery();
        mltQuery.setSimilarity(parseContext.searchSimilarity());

        String currentFieldName = null;
        while ((token = jp.nextToken()) != JsonToken.END_OBJECT) {
            if (token == JsonToken.FIELD_NAME) {
                currentFieldName = jp.getCurrentName();
            } else if (token == JsonToken.VALUE_STRING) {
                if ("likeText".equals(currentFieldName)) {
                    mltQuery.setLikeText(jp.getText());
                } else if ("minTermFrequency".equals(currentFieldName)) {
                    mltQuery.setMinTermFrequency(Integer.parseInt(jp.getText()));
                } else if ("maxQueryTerms".equals(currentFieldName)) {
                    mltQuery.setMaxQueryTerms(Integer.parseInt(jp.getText()));
                } else if ("minDocFreq".equals(currentFieldName)) {
                    mltQuery.setMinDocFreq(Integer.parseInt(jp.getText()));
                } else if ("maxDocFreq".equals(currentFieldName)) {
                    mltQuery.setMaxDocFreq(Integer.parseInt(jp.getText()));
                } else if ("minWordLen".equals(currentFieldName)) {
                    mltQuery.setMinWordLen(Integer.parseInt(jp.getText()));
                } else if ("maxWordLen".equals(currentFieldName)) {
                    mltQuery.setMaxWordLen(Integer.parseInt(jp.getText()));
                } else if ("boostTerms".equals(currentFieldName)) {
                    mltQuery.setBoostTerms(Booleans.parseBoolean(jp.getText(), false));
                } else if ("boostTermsFactor".equals(currentFieldName)) {
                    mltQuery.setBoostTermsFactor(Float.parseFloat(jp.getText()));
                } else if ("percentTermsToMatch".equals(currentFieldName)) {
                    mltQuery.setPercentTermsToMatch(Float.parseFloat(jp.getText()));
                }
            } else if (token == JsonToken.VALUE_NUMBER_INT) {
                if ("minTermFrequency".equals(currentFieldName)) {
                    mltQuery.setMinTermFrequency(jp.getIntValue());
                } else if ("maxQueryTerms".equals(currentFieldName)) {
                    mltQuery.setMaxQueryTerms(jp.getIntValue());
                } else if ("minDocFreq".equals(currentFieldName)) {
                    mltQuery.setMinDocFreq(jp.getIntValue());
                } else if ("maxDocFreq".equals(currentFieldName)) {
                    mltQuery.setMaxDocFreq(jp.getIntValue());
                } else if ("minWordLen".equals(currentFieldName)) {
                    mltQuery.setMinWordLen(jp.getIntValue());
                } else if ("maxWordLen".equals(currentFieldName)) {
                    mltQuery.setMaxWordLen(jp.getIntValue());
                } else if ("boostTerms".equals(currentFieldName)) {
                    mltQuery.setBoostTerms(jp.getIntValue() != 0);
                } else if ("boostTermsFactor".equals(currentFieldName)) {
                    mltQuery.setBoostTermsFactor(jp.getIntValue());
                } else if ("percentTermsToMatch".equals(currentFieldName)) {
                    mltQuery.setPercentTermsToMatch(jp.getIntValue());
                }
            } else if (token == JsonToken.VALUE_NUMBER_FLOAT) {
                if ("boostTermsFactor".equals(currentFieldName)) {
                    mltQuery.setBoostTermsFactor(jp.getFloatValue());
                } else if ("percentTermsToMatch".equals(currentFieldName)) {
                    mltQuery.setPercentTermsToMatch(jp.getFloatValue());
                }
            } else if (token == JsonToken.START_ARRAY) {
                if ("stopWords".equals(currentFieldName)) {
                    Set<String> stopWords = Sets.newHashSet();
                    while ((token = jp.nextToken()) != JsonToken.END_ARRAY) {
                        stopWords.add(jp.getText());
                    }
                    mltQuery.setStopWords(stopWords);
                }
            }
        }

        if (mltQuery.getLikeText() == null) {
            throw new QueryParsingException(index, "moreLikeThisField requires 'likeText' to be specified");
        }

        // move to the next end object, to close the field name
        token = jp.nextToken();
        assert token == JsonToken.END_OBJECT;

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
        return wrapSmartNameQuery(mltQuery, smartNameFieldMappers, parseContext.filterCache());
    }
}
