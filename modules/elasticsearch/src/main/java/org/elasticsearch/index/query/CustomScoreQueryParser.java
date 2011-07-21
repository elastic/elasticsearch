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

package org.elasticsearch.index.query;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.Query;
import org.elasticsearch.ElasticSearchIllegalStateException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.search.function.FunctionScoreQuery;
import org.elasticsearch.common.lucene.search.function.ScoreFunction;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Map;

/**
 * @author kimchy (shay.banon)
 */
public class CustomScoreQueryParser implements QueryParser {

    public static final String NAME = "custom_score";

    @Inject public CustomScoreQueryParser() {
    }

    @Override public String[] names() {
        return new String[]{NAME, Strings.toCamelCase(NAME)};
    }

    @Override public Query parse(QueryParseContext parseContext) throws IOException, QueryParsingException {
        XContentParser parser = parseContext.parser();

        Query query = null;
        float boost = 1.0f;
        String script = null;
        String scriptLang = null;
        Map<String, Object> vars = null;

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
            } else if (token.isValue()) {
                if ("script".equals(currentFieldName)) {
                    script = parser.text();
                } else if ("lang".equals(currentFieldName)) {
                    scriptLang = parser.text();
                } else if ("boost".equals(currentFieldName)) {
                    boost = parser.floatValue();
                }
            }
        }
        if (query == null) {
            throw new QueryParsingException(parseContext.index(), "[custom_score] requires 'query' field");
        }
        if (script == null) {
            throw new QueryParsingException(parseContext.index(), "[custom_score] requires 'script' field");
        }

        SearchContext context = SearchContext.current();
        if (context == null) {
            throw new ElasticSearchIllegalStateException("No search context on going...");
        }
        SearchScript searchScript = context.scriptService().search(context.lookup(), scriptLang, script, vars);
        FunctionScoreQuery functionScoreQuery = new FunctionScoreQuery(query, new ScriptScoreFunction(searchScript));
        functionScoreQuery.setBoost(boost);
        return functionScoreQuery;
    }

    public static class ScriptScoreFunction implements ScoreFunction {

        private final SearchScript script;

        public ScriptScoreFunction(SearchScript script) {
            this.script = script;
        }

        @Override public void setNextReader(IndexReader reader) {
            script.setNextReader(reader);
        }

        @Override public float score(int docId, float subQueryScore) {
            script.setNextDocId(docId);
            script.setNextScore(subQueryScore);
            return script.runAsFloat();
        }

        @Override public Explanation explain(int docId, Explanation subQueryExpl) {
            float score = score(docId, subQueryExpl.getValue());
            Explanation exp = new Explanation(score, "script score function: product of:");
            exp.addDetail(subQueryExpl);
            return exp;
        }
    }
}