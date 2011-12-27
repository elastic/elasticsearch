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

import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Query;
import org.elasticsearch.ElasticSearchIllegalStateException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.search.function.BoostScoreFunction;
import org.elasticsearch.common.lucene.search.function.FiltersFunctionScoreQuery;
import org.elasticsearch.common.lucene.search.function.ScoreFunction;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

/**
 *
 */
public class CustomFiltersScoreQueryParser implements QueryParser {

    public static final String NAME = "custom_filters_score";

    @Inject
    public CustomFiltersScoreQueryParser() {
    }

    @Override
    public String[] names() {
        return new String[]{NAME, Strings.toCamelCase(NAME)};
    }

    @Override
    public Query parse(QueryParseContext parseContext) throws IOException, QueryParsingException {
        XContentParser parser = parseContext.parser();

        Query query = null;
        float boost = 1.0f;
        String scriptLang = null;
        Map<String, Object> vars = null;

        FiltersFunctionScoreQuery.ScoreMode scoreMode = FiltersFunctionScoreQuery.ScoreMode.First;
        NestedGroup rootGroup = new NestedGroup(null, scoreMode);

        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                if ("query".equals(currentFieldName)) {
                    query = parseContext.parseInnerQuery();
                } else if ("params".equals(currentFieldName)) {
                    vars = parser.map();
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                if ("filters".equals(currentFieldName)) {
                    parseFilterLevel(parseContext, parser, rootGroup);
                }
            } else if (token.isValue()) {
                if ("lang".equals(currentFieldName)) {
                    scriptLang = parser.text();
                } else if ("boost".equals(currentFieldName)) {
                    boost = parser.floatValue();
                } else if ("score_mode".equals(currentFieldName) || "scoreMode".equals(currentFieldName)) {
                    String sScoreMode = parser.text();
                    rootGroup.scoreMode = stringToScoreMode(sScoreMode);
                    if (rootGroup.scoreMode == null) {
                        throw new QueryParsingException(parseContext.index(), "illegal score_mode for nested query [" + sScoreMode + "]");
                    }
                }
            }
        }
        if (query == null) {
            throw new QueryParsingException(parseContext.index(), "[custom_filters_score] requires 'query' field");
        }

        SearchContext context = SearchContext.current();
        if (context == null) {
            throw new ElasticSearchIllegalStateException("No search context on going...");
        }

        FiltersFunctionScoreQuery.FilterScoreGroup filterGroups = buildFunctionGroups(context, scriptLang, vars, rootGroup);

        FiltersFunctionScoreQuery functionScoreQuery = new FiltersFunctionScoreQuery(query, filterGroups);
        functionScoreQuery.setBoost(boost);
        return functionScoreQuery;
    }

    private FiltersFunctionScoreQuery.ScoreMode stringToScoreMode(final String sScoreMode) {
        if ("avg".equals(sScoreMode)) {
            return FiltersFunctionScoreQuery.ScoreMode.Avg;
        } else if ("max".equals(sScoreMode)) {
            return FiltersFunctionScoreQuery.ScoreMode.Max;
        } else if ("min".equals(sScoreMode)) {
            return FiltersFunctionScoreQuery.ScoreMode.Min;
        } else if ("total".equals(sScoreMode)) {
            return FiltersFunctionScoreQuery.ScoreMode.Total;
        } else if ("multiply".equals(sScoreMode)) {
            return FiltersFunctionScoreQuery.ScoreMode.Multiply;
        } else if ("first".equals(sScoreMode)) {
            return FiltersFunctionScoreQuery.ScoreMode.First;
        } else {
            return null;
        }
    }

    private void parseFilterLevel(final QueryParseContext parseContext, final XContentParser parser, final NestedGroup currentGroup) throws IOException, QueryParsingException {
        String currentFieldName = null;
        XContentParser.Token token;

        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
            String script = null;
            Filter filter = null;
            float fboost = Float.NaN;
            boolean possibleNonNestedObjectStarted = false;

            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token == XContentParser.Token.START_ARRAY) {
                    FiltersFunctionScoreQuery.ScoreMode currentScoreMode = stringToScoreMode(currentFieldName);
                    if (currentScoreMode == null) {
                        throw new QueryParsingException(parseContext.index(), "illegal nested grouping label: " + currentFieldName);
                    }
                    NestedGroup subGroup = new NestedGroup(currentGroup, currentScoreMode);
                    parseFilterLevel(parseContext, parser, subGroup);
                    currentGroup.add(subGroup);
                    possibleNonNestedObjectStarted = false;
                    continue;
                } else if (token == XContentParser.Token.START_OBJECT) {
                    possibleNonNestedObjectStarted = true;
                    if ("filter".equals(currentFieldName)) {
                        filter = parseContext.parseInnerFilter();
                    }
                } else if (token.isValue()) {
                    if ("script".equals(currentFieldName)) {
                        script = parser.text();
                    } else if ("boost".equals(currentFieldName)) {
                        fboost = parser.floatValue();
                    }
                }
            }

            if (possibleNonNestedObjectStarted) {
                if (script == null && fboost == -1) {
                    throw new QueryParsingException(parseContext.index(), "[custom_filters_score] missing 'script' or 'boost' in filters array element");
                }
                if (filter == null) {
                    throw new QueryParsingException(parseContext.index(), "[custom_filters_score] missing 'filter' in filters array element");
                }

                currentGroup.add(new NestedFilter(filter, script, fboost));
            }
        }

        if (currentGroup.isEmpty()) {
            throw new QueryParsingException(parseContext.index(), "[custom_filters_score] requires 'filters' field");
        }
    }

    private FiltersFunctionScoreQuery.FilterScoreGroup buildFunctionGroups(final SearchContext context, final String scriptLang, final Map<String, Object> vars, final NestedGroup currentNesting) {
        ArrayList<FiltersFunctionScoreQuery.FilterItem> filterItems = new ArrayList<FiltersFunctionScoreQuery.FilterItem>(currentNesting.size());
        for (NestedItem nestedItem : currentNesting.items) {
            if (nestedItem instanceof NestedGroup) {
                filterItems.add(buildFunctionGroups(context, scriptLang, vars, (NestedGroup) nestedItem));
            } else if (nestedItem instanceof NestedFilter) {
                NestedFilter filter = (NestedFilter) nestedItem;
                ScoreFunction scoreFunction;
                String script = filter.script;
                if (script != null) {
                    SearchScript searchScript = context.scriptService().search(context.lookup(), scriptLang, script, vars);
                    scoreFunction = new CustomScoreQueryParser.ScriptScoreFunction(script, vars, searchScript);
                } else {
                    scoreFunction = new BoostScoreFunction(filter.boost);
                }
                filterItems.add(new FiltersFunctionScoreQuery.FilterFunction(filter.filter, scoreFunction));
            }
        }
        return new FiltersFunctionScoreQuery.FilterScoreGroup(currentNesting.scoreMode, filterItems.toArray(new FiltersFunctionScoreQuery.FilterItem[filterItems.size()]));
    }


    private abstract class NestedItem {

    }

    private class NestedFilter extends NestedItem {
        Filter filter;
        String script;
        Float boost;

        private NestedFilter(Filter filter, String script, Float boost) {
            this.filter = filter;
            this.script = script;
            this.boost = boost;
        }
    }

    private class NestedGroup extends NestedItem {
        NestedGroup parent;
        FiltersFunctionScoreQuery.ScoreMode scoreMode;
        ArrayList<NestedItem> items = new ArrayList<NestedItem>();


        public NestedGroup(NestedGroup parent, FiltersFunctionScoreQuery.ScoreMode scoreMode) {
            this.parent = parent;
            this.scoreMode = scoreMode;
        }

        public void add(NestedItem item) {
            items.add(item);
        }

        public boolean isEmpty() {
            return items.isEmpty();
        }

        public int size() {
            return items.size();
        }

    }
}