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

import com.carrotsearch.hppc.FloatArrayList;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.search.function.BoostScoreFunction;
import org.elasticsearch.common.lucene.search.function.FiltersFunctionScoreQuery;
import org.elasticsearch.common.lucene.search.function.ScoreFunction;
import org.elasticsearch.common.lucene.search.function.ScriptScoreFunction;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryParser;
import org.elasticsearch.script.SearchScript;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

/**
 * @deprecated use {@link FunctionScoreQueryParser} instead.
 */
public class CustomFiltersScoreQueryParser implements QueryParser {

    public static final String NAME = "custom_filters_score";

    @Inject
    public CustomFiltersScoreQueryParser() {
    }

    @Override
    public String[] names() {
        return new String[] { NAME, Strings.toCamelCase(NAME) };
    }

    @Override
    public Query parse(QueryParseContext parseContext) throws IOException, QueryParsingException {
        XContentParser parser = parseContext.parser();

        Query query = null;
        boolean queryFound = false;
        float boost = 1.0f;
        String scriptLang = null;
        Map<String, Object> vars = null;

        FiltersFunctionScoreQuery.ScoreMode scoreMode = FiltersFunctionScoreQuery.ScoreMode.First;
        ArrayList<Filter> filters = new ArrayList<Filter>();
        boolean filtersFound = false;
        ArrayList<String> scripts = new ArrayList<String>();
        FloatArrayList boosts = new FloatArrayList();
        float maxBoost = Float.MAX_VALUE;

        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                if ("query".equals(currentFieldName)) {
                    query = parseContext.parseInnerQuery();
                    queryFound = true;
                } else if ("params".equals(currentFieldName)) {
                    vars = parser.map();
                } else {
                    throw new QueryParsingException(parseContext.index(), "[custom_filters_score] query does not support ["
                            + currentFieldName + "]");
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                if ("filters".equals(currentFieldName)) {
                    filtersFound = true;
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        String script = null;
                        Filter filter = null;
                        boolean filterFound = false;
                        float fboost = Float.NaN;
                        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                            if (token == XContentParser.Token.FIELD_NAME) {
                                currentFieldName = parser.currentName();
                            } else if (token == XContentParser.Token.START_OBJECT) {
                                if ("filter".equals(currentFieldName)) {
                                    filter = parseContext.parseInnerFilter();
                                    filterFound = true;
                                }
                            } else if (token.isValue()) {
                                if ("script".equals(currentFieldName)) {
                                    script = parser.text();
                                } else if ("boost".equals(currentFieldName)) {
                                    fboost = parser.floatValue();
                                }
                            }
                        }
                        if (script == null && fboost == -1) {
                            throw new QueryParsingException(parseContext.index(),
                                    "[custom_filters_score] missing 'script' or 'boost' in filters array element");
                        }
                        if (!filterFound) {
                            throw new QueryParsingException(parseContext.index(),
                                    "[custom_filters_score] missing 'filter' in filters array element");
                        }
                        if (filter != null) {
                            filters.add(filter);
                            scripts.add(script);
                            boosts.add(fboost);
                        }
                    }
                } else {
                    throw new QueryParsingException(parseContext.index(), "[custom_filters_score] query does not support ["
                            + currentFieldName + "]");
                }
            } else if (token.isValue()) {
                if ("lang".equals(currentFieldName)) {
                    scriptLang = parser.text();
                } else if ("boost".equals(currentFieldName)) {
                    boost = parser.floatValue();
                } else if ("score_mode".equals(currentFieldName) || "scoreMode".equals(currentFieldName)) {
                    String sScoreMode = parser.text();
                    if ("avg".equals(sScoreMode)) {
                        scoreMode = FiltersFunctionScoreQuery.ScoreMode.Avg;
                    } else if ("max".equals(sScoreMode)) {
                        scoreMode = FiltersFunctionScoreQuery.ScoreMode.Max;
                    } else if ("min".equals(sScoreMode)) {
                        scoreMode = FiltersFunctionScoreQuery.ScoreMode.Min;
                    } else if ("total".equals(sScoreMode)) {
                        scoreMode = FiltersFunctionScoreQuery.ScoreMode.Sum;
                    } else if ("multiply".equals(sScoreMode)) {
                        scoreMode = FiltersFunctionScoreQuery.ScoreMode.Multiply;
                    } else if ("first".equals(sScoreMode)) {
                        scoreMode = FiltersFunctionScoreQuery.ScoreMode.First;
                    } else {
                        throw new QueryParsingException(parseContext.index(), "[custom_filters_score] illegal score_mode [" + sScoreMode
                                + "]");
                    }
                } else if ("max_boost".equals(currentFieldName) || "maxBoost".equals(currentFieldName)) {
                    maxBoost = parser.floatValue();
                } else {
                    throw new QueryParsingException(parseContext.index(), "[custom_filters_score] query does not support ["
                            + currentFieldName + "]");
                }
            }
        }
        if (!queryFound) {
            throw new QueryParsingException(parseContext.index(), "[custom_filters_score] requires 'query' field");
        }
        if (query == null) {
            return null;
        }
        if (!filtersFound) {
            throw new QueryParsingException(parseContext.index(), "[custom_filters_score] requires 'filters' field");
        }
        // if all filter elements returned null, just use the query
        if (filters.isEmpty()) {
            return query;
        }

        FiltersFunctionScoreQuery.FilterFunction[] filterFunctions = new FiltersFunctionScoreQuery.FilterFunction[filters.size()];
        for (int i = 0; i < filterFunctions.length; i++) {
            ScoreFunction scoreFunction;
            String script = scripts.get(i);
            if (script != null) {
                SearchScript searchScript = parseContext.scriptService().search(parseContext.lookup(), scriptLang, script, vars);
                scoreFunction = new ScriptScoreFunction(script, vars, searchScript);
            } else {
                scoreFunction = new BoostScoreFunction(boosts.get(i));
            }
            filterFunctions[i] = new FiltersFunctionScoreQuery.FilterFunction(filters.get(i), scoreFunction);
        }
        FiltersFunctionScoreQuery functionScoreQuery = new FiltersFunctionScoreQuery(query, scoreMode, filterFunctions, maxBoost);
        functionScoreQuery.setBoost(boost);
        return functionScoreQuery;
    }
}