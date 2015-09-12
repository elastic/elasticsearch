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



package org.elasticsearch.index.query.functionscore.script;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.search.function.ScoreFunction;
import org.elasticsearch.common.lucene.search.function.ScriptScoreFunction;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.query.QueryParsingException;
import org.elasticsearch.index.query.functionscore.ScoreFunctionParser;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.Script.ScriptField;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptParameterParser;
import org.elasticsearch.script.ScriptParameterParser.ScriptParameterValue;
import org.elasticsearch.script.SearchScript;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public class ScriptScoreFunctionParser implements ScoreFunctionParser {

    public static String[] NAMES = { "script_score", "scriptScore" };

    @Inject
    public ScriptScoreFunctionParser() {
    }

    @Override
    public String[] getNames() {
        return NAMES;
    }

    @Override
    public ScoreFunction parse(QueryParseContext parseContext, XContentParser parser) throws IOException, QueryParsingException {
        ScriptParameterParser scriptParameterParser = new ScriptParameterParser();
        Script script = null;
        Map<String, Object> vars = null;
        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (parseContext.parseFieldMatcher().match(currentFieldName, ScriptField.SCRIPT)) {
                    script = Script.parse(parser, parseContext.parseFieldMatcher());
                } else if ("params".equals(currentFieldName)) { // TODO remove in 3.0 (here to support old script APIs)
                    vars = parser.map();
                } else {
                    throw new QueryParsingException(parseContext, NAMES[0] + " query does not support [" + currentFieldName + "]");
                }
            } else if (token.isValue()) {
                if (!scriptParameterParser.token(currentFieldName, token, parser, parseContext.parseFieldMatcher())) {
                    throw new QueryParsingException(parseContext, NAMES[0] + " query does not support [" + currentFieldName + "]");
                }
            }
        }

        if (script == null) { // Didn't find anything using the new API so try using the old one instead
            ScriptParameterValue scriptValue = scriptParameterParser.getDefaultScriptParameterValue();
            if (scriptValue != null) {
                if (vars == null) {
                    vars = new HashMap<>();
                }
                script = new Script(scriptValue.script(), scriptValue.scriptType(), scriptParameterParser.lang(), vars);
            }
        } else if (vars != null) {
            throw new QueryParsingException(parseContext, "script params must be specified inside script object");
        }

        if (script == null) {
            throw new QueryParsingException(parseContext, NAMES[0] + " requires 'script' field");
        }

        SearchScript searchScript;
        try {
            searchScript = parseContext.scriptService().search(parseContext.lookup(), script, ScriptContext.Standard.SEARCH);
            return new ScriptScoreFunction(script, searchScript);
        } catch (Exception e) {
            throw new QueryParsingException(parseContext, NAMES[0] + " the script could not be loaded", e);
        }
    }
}