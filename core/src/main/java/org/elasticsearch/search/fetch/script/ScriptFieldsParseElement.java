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

package org.elasticsearch.search.fetch.script;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.Script.ScriptField;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptParameterParser;
import org.elasticsearch.script.ScriptParameterParser.ScriptParameterValue;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.SearchParseElement;
import org.elasticsearch.search.SearchParseException;
import org.elasticsearch.search.internal.SearchContext;

import java.util.HashMap;
import java.util.Map;

/**
 * <pre>
 * "script_fields" : {
 *  "test1" : {
 *      "script" : "doc['field_name'].value"
 *  },
 *  "test2" : {
 *      "script" : "..."
 *  }
 * }
 * </pre>
 *
 *
 */
public class ScriptFieldsParseElement implements SearchParseElement {

    @Override
    public void parse(XContentParser parser, SearchContext context) throws Exception {
        XContentParser.Token token;
        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                String fieldName = currentFieldName;
                ScriptParameterParser scriptParameterParser = new ScriptParameterParser();
                Script script = null;
                Map<String, Object> params = null;
                boolean ignoreException = false;
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        currentFieldName = parser.currentName();
                    } else if (token == XContentParser.Token.START_OBJECT) {
                        if (context.parseFieldMatcher().match(currentFieldName, ScriptField.SCRIPT)) {
                            script = Script.parse(parser, context.parseFieldMatcher());
                        } else if ("params".equals(currentFieldName)) {
                            params = parser.map();
                        }
                    } else if (token.isValue()) {
                        if ("ignore_failure".equals(currentFieldName)) {
                            ignoreException = parser.booleanValue();
                        } else {
                            scriptParameterParser.token(currentFieldName, token, parser, context.parseFieldMatcher());
                        }
                    }
                }

                if (script == null) { // Didn't find anything using the new API so try using the old one instead
                    ScriptParameterValue scriptValue = scriptParameterParser.getDefaultScriptParameterValue();
                    if (scriptValue != null) {
                        if (params == null) {
                            params = new HashMap<>();
                        }
                        script = new Script(scriptValue.script(), scriptValue.scriptType(), scriptParameterParser.lang(), params);
                    }
                } else if (params != null) {
                    throw new SearchParseException(context, "script params must be specified inside script object",
                            parser.getTokenLocation());
                }

                if (script == null) {
                    throw new SearchParseException(context, "must specify a script in script fields", parser.getTokenLocation());
                }

                SearchScript searchScript = context.scriptService().search(context.lookup(), script, ScriptContext.Standard.SEARCH);
                context.scriptFields().add(new ScriptFieldsContext.ScriptField(fieldName, searchScript, ignoreException));
            }
        }
    }
}