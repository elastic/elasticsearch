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
import org.elasticsearch.script.*;
import org.elasticsearch.script.ScriptParameterParser.ScriptParameterValue;
import org.elasticsearch.search.SearchParseElement;
import org.elasticsearch.search.internal.SearchContext;

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
                String script = null;
                ScriptService.ScriptType scriptType = null;
                Map<String, Object> params = null;
                boolean ignoreException = false;
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        currentFieldName = parser.currentName();
                    } else if (token == XContentParser.Token.START_OBJECT) {
                        params = parser.map();
                    } else if (token.isValue()) {
                        if ("ignore_failure".equals(currentFieldName)) {
                            ignoreException = parser.booleanValue();
                        } else {
                            scriptParameterParser.token(currentFieldName, token, parser);
                        }
                    }
                }

                ScriptParameterValue scriptValue = scriptParameterParser.getDefaultScriptParameterValue();
                if (scriptValue != null) {
                    script = scriptValue.script();
                    scriptType = scriptValue.scriptType();
                }
                SearchScript searchScript = context.scriptService().search(context.lookup(), new Script(scriptParameterParser.lang(), script, scriptType, params), ScriptContext.Standard.SEARCH);
                context.scriptFields().add(new ScriptFieldsContext.ScriptField(fieldName, searchScript, ignoreException));
            }
        }
    }
}