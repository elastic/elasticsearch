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

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;

/**
 *
 */
public class TextQueryParser implements QueryParser {

    public static final String NAME = "text";

    @Inject
    public TextQueryParser() {
    }

    @Override
    public String[] names() {
        return new String[]{NAME, "text_phrase", "textPhrase", "text_phrase_prefix", "textPhrasePrefix", "fuzzyText", "fuzzy_text"};
    }

    @Override
    public Query parse(QueryParseContext parseContext) throws IOException, QueryParsingException {
        XContentParser parser = parseContext.parser();

        org.elasticsearch.index.search.TextQueryParser.Type type = org.elasticsearch.index.search.TextQueryParser.Type.BOOLEAN;
        if ("text_phrase".equals(parser.currentName()) || "textPhrase".equals(parser.currentName())) {
            type = org.elasticsearch.index.search.TextQueryParser.Type.PHRASE;
        } else if ("text_phrase_prefix".equals(parser.currentName()) || "textPhrasePrefix".equals(parser.currentName())) {
            type = org.elasticsearch.index.search.TextQueryParser.Type.PHRASE_PREFIX;
        }

        XContentParser.Token token = parser.nextToken();
        if (token != XContentParser.Token.FIELD_NAME) {
            throw new QueryParsingException(parseContext.index(), "[text] query malformed, no field");
        }
        String fieldName = parser.currentName();

        String text = null;
        float boost = 1.0f;
        int phraseSlop = 0;
        String analyzer = null;
        String fuzziness = null;
        int prefixLength = FuzzyQuery.defaultPrefixLength;
        int maxExpansions = FuzzyQuery.defaultMaxExpansions;
        BooleanClause.Occur occur = BooleanClause.Occur.SHOULD;

        token = parser.nextToken();
        if (token == XContentParser.Token.START_OBJECT) {
            String currentFieldName = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token.isValue()) {
                    if ("query".equals(currentFieldName)) {
                        text = parser.text();
                    } else if ("type".equals(currentFieldName)) {
                        String tStr = parser.text();
                        if ("boolean".equals(tStr)) {
                            type = org.elasticsearch.index.search.TextQueryParser.Type.BOOLEAN;
                        } else if ("phrase".equals(tStr)) {
                            type = org.elasticsearch.index.search.TextQueryParser.Type.PHRASE;
                        } else if ("phrase_prefix".equals(tStr) || "phrasePrefix".equals(currentFieldName)) {
                            type = org.elasticsearch.index.search.TextQueryParser.Type.PHRASE_PREFIX;
                        }
                    } else if ("analyzer".equals(currentFieldName)) {
                        analyzer = parser.text();
                        if (parseContext.analysisService().analyzer(analyzer) == null) {
                            throw new QueryParsingException(parseContext.index(), "[text] analyzer [" + parser.text() + "] not found");
                        }
                    } else if ("boost".equals(currentFieldName)) {
                        boost = parser.floatValue();
                    } else if ("slop".equals(currentFieldName) || "phrase_slop".equals(currentFieldName) || "phraseSlop".equals(currentFieldName)) {
                        phraseSlop = parser.intValue();
                    } else if ("fuzziness".equals(currentFieldName)) {
                        fuzziness = parser.textOrNull();
                    } else if ("prefix_length".equals(currentFieldName) || "prefixLength".equals(currentFieldName)) {
                        prefixLength = parser.intValue();
                    } else if ("max_expansions".equals(currentFieldName) || "maxExpansions".equals(currentFieldName)) {
                        maxExpansions = parser.intValue();
                    } else if ("operator".equals(currentFieldName)) {
                        String op = parser.text();
                        if ("or".equalsIgnoreCase(op)) {
                            occur = BooleanClause.Occur.SHOULD;
                        } else if ("and".equalsIgnoreCase(op)) {
                            occur = BooleanClause.Occur.MUST;
                        } else {
                            throw new QueryParsingException(parseContext.index(), "text query requires operator to be either 'and' or 'or', not [" + op + "]");
                        }
                    } else {
                        throw new QueryParsingException(parseContext.index(), "[text] query does not support [" + currentFieldName + "]");
                    }
                }
            }
            parser.nextToken();
        } else {
            text = parser.text();
            // move to the next token
            parser.nextToken();
        }

        if (text == null) {
            throw new QueryParsingException(parseContext.index(), "No text specified for text query");
        }

        org.elasticsearch.index.search.TextQueryParser tQP = new org.elasticsearch.index.search.TextQueryParser(parseContext, fieldName, text);
        tQP.setPhraseSlop(phraseSlop);
        tQP.setAnalyzer(analyzer);
        tQP.setFuzziness(fuzziness);
        tQP.setFuzzyPrefixLength(prefixLength);
        tQP.setMaxExpansions(maxExpansions);
        tQP.setOccur(occur);

        Query query = tQP.parse(type);
        query.setBoost(boost);
        return query;
    }
}