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

package org.elasticsearch.index.query.distancescoring;

import java.io.IOException;
import java.util.HashMap;
import java.util.Set;

import org.apache.lucene.search.Query;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.search.function.FunctionScoreQuery;
import org.elasticsearch.common.lucene.search.function.ScoreFunction;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.query.QueryParser;
import org.elasticsearch.index.query.QueryParsingException;

/**
 *
 */
public class FunctionScoreQueryParser implements QueryParser {

    public static final String NAME = "distance_score";

    protected HashMap<String, ScoreFunctionParser> functionParsers;

    @Inject
    public FunctionScoreQueryParser(Set<ScoreFunctionParser> parsers) {

        functionParsers = new HashMap<String, ScoreFunctionParser>();
        for (ScoreFunctionParser scoreFunctionParser : parsers) {
            addParser(scoreFunctionParser);
        }
    }

    private void addParser(ScoreFunctionParser scoreFunctionParser) {
        functionParsers.put(scoreFunctionParser.getName(), scoreFunctionParser);
    }

    @Override
    public String[] names() {
        return new String[] { NAME, Strings.toCamelCase(NAME) };
    }

    @Override
    public Query parse(QueryParseContext parseContext) throws IOException, QueryParsingException {
        XContentParser parser = parseContext.parser();

        Query query = null;
        String currentFieldName = null;
        ScoreFunction scoreFunction = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                if ("query".equals(currentFieldName)) {
                    query = parseContext.parseInnerQuery();
                } else {
                    ScoreFunctionParser scoreFunctionParser = functionParsers.get(currentFieldName);
                    if (scoreFunctionParser == null) {
                        throw new QueryParsingException(parseContext.index(), "[distance_score] query does not support ["
                                + currentFieldName + "]");
                    } else {
                        scoreFunction = scoreFunctionParser.parse(parseContext, parser);
                    }
                }
            }
        }

        if (scoreFunction == null) {
            throw new QueryParsingException(parseContext.index(), "[distance_score] requires a function");
        }
        if (query == null) {
            throw new QueryParsingException(parseContext.index(), "[distance_score] requires a 'query'");
        }
        FunctionScoreQuery fQuery = new FunctionScoreQuery(query, scoreFunction);
        fQuery.setBoost(1);
        return fQuery;

    }
}