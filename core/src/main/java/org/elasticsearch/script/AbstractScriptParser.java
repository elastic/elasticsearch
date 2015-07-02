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

package org.elasticsearch.script;

import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.script.Script.ScriptField;
import org.elasticsearch.script.Script.ScriptParseException;
import org.elasticsearch.script.ScriptService.ScriptType;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

public abstract class AbstractScriptParser<S extends Script> {

    protected abstract String parseInlineScript(XContentParser parser) throws IOException;

    protected abstract S createScript(String script, ScriptType type, String lang, Map<String, Object> params);

    protected abstract S createSimpleScript(XContentParser parser) throws IOException;

    @Deprecated
    protected Map<String, ScriptType> getAdditionalScriptParameters() {
        return Collections.emptyMap();
    }

    public S parse(XContentParser parser, ParseFieldMatcher parseFieldMatcher) throws IOException {

        XContentParser.Token token = parser.currentToken();
        // If the parser hasn't yet been pushed to the first token, do it now
        if (token == null) {
            token = parser.nextToken();
        }

        if (token == XContentParser.Token.VALUE_STRING) {
            return createSimpleScript(parser);
        }
        if (token != XContentParser.Token.START_OBJECT) {
            throw new ScriptParseException("expected a string value or an object, but found [{}] instead", token);
        }

        String script = null;
        ScriptType type = null;
        String lang = getDefaultScriptLang();
        Map<String, Object> params = null;

        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (parseFieldMatcher.match(currentFieldName, ScriptType.INLINE.getParseField()) || parseFieldMatcher.match(currentFieldName, ScriptService.SCRIPT_INLINE)) {
                type = ScriptType.INLINE;
                script = parseInlineScript(parser);
            } else if (parseFieldMatcher.match(currentFieldName, ScriptType.FILE.getParseField()) || parseFieldMatcher.match(currentFieldName, ScriptService.SCRIPT_FILE)) {
                type = ScriptType.FILE;
                if (token == XContentParser.Token.VALUE_STRING) {
                    script = parser.text();
                } else {
                    throw new ScriptParseException("expected a string value for field [{}], but found [{}]", currentFieldName, token);
                }
            } else if (parseFieldMatcher.match(currentFieldName, ScriptType.INDEXED.getParseField()) || parseFieldMatcher.match(currentFieldName, ScriptService.SCRIPT_ID)) {
                type = ScriptType.INDEXED;
                if (token == XContentParser.Token.VALUE_STRING) {
                    script = parser.text();
                } else {
                    throw new ScriptParseException("expected a string value for field [{}], but found [{}]", currentFieldName, token);
                }
            } else if (parseFieldMatcher.match(currentFieldName, ScriptField.LANG) || parseFieldMatcher.match(currentFieldName, ScriptService.SCRIPT_LANG)) {
                if (token == XContentParser.Token.VALUE_STRING) {
                    lang = parser.text();
                } else {
                    throw new ScriptParseException("expected a string value for field [{}], but found [{}]", currentFieldName, token);
                }
            } else if (parseFieldMatcher.match(currentFieldName, ScriptField.PARAMS)) {
                if (token == XContentParser.Token.START_OBJECT) {
                    params = parser.map();
                } else {
                    throw new ScriptParseException("expected an object for field [{}], but found [{}]", currentFieldName, token);
                }
            } else {
                // TODO remove this in 3.0
                ScriptType paramScriptType = getAdditionalScriptParameters().get(currentFieldName);
                if (paramScriptType != null) {
                    script = parseInlineScript(parser);
                    type = paramScriptType;
                } else {
                    throw new ScriptParseException("unexpected field [{}]", currentFieldName);
                }
            }
        }
        if (script == null) {
            throw new ScriptParseException("expected one of [{}], [{}] or [{}] fields, but found none", ScriptType.INLINE.getParseField()
                    .getPreferredName(), ScriptType.FILE.getParseField().getPreferredName(), ScriptType.INDEXED.getParseField()
                    .getPreferredName());
        }
        assert type != null : "if script is not null, type should definitely not be null";
        return createScript(script, type, lang, params);

    }

    /**
     * @return the default script language for this parser or <code>null</code>
     *         to use the default set in the ScriptService
     */
    protected String getDefaultScriptLang() {
        return null;
    }

    public S parse(Map<String, Object> config, boolean removeMatchedEntries, ParseFieldMatcher parseFieldMatcher) {
        String script = null;
        ScriptType type = null;
        String lang = null;
        Map<String, Object> params = null;
        for (Iterator<Entry<String, Object>> itr = config.entrySet().iterator(); itr.hasNext();) {
            Entry<String, Object> entry = itr.next();
            String parameterName = entry.getKey();
            Object parameterValue = entry.getValue();
            if (parseFieldMatcher.match(parameterName, ScriptField.LANG) || parseFieldMatcher.match(parameterName, ScriptService.SCRIPT_LANG)) {
                if (parameterValue instanceof String || parameterValue == null) {
                    lang = (String) parameterValue;
                    if (removeMatchedEntries) {
                        itr.remove();
                    }
                } else {
                    throw new ScriptParseException("Value must be of type String: [" + parameterName + "]");
                }
            } else if (parseFieldMatcher.match(parameterName, ScriptField.PARAMS)) {
                if (parameterValue instanceof Map || parameterValue == null) {
                    params = (Map<String, Object>) parameterValue;
                    if (removeMatchedEntries) {
                        itr.remove();
                    }
                } else {
                    throw new ScriptParseException("Value must be of type String: [" + parameterName + "]");
                }
            } else if (parseFieldMatcher.match(parameterName, ScriptType.INLINE.getParseField()) || parseFieldMatcher.match(parameterName, ScriptService.SCRIPT_INLINE)) {
                if (parameterValue instanceof String || parameterValue == null) {
                    script = (String) parameterValue;
                    type = ScriptType.INLINE;
                    if (removeMatchedEntries) {
                        itr.remove();
                    }
                } else {
                    throw new ScriptParseException("Value must be of type String: [" + parameterName + "]");
                }
            } else if (parseFieldMatcher.match(parameterName, ScriptType.FILE.getParseField()) || parseFieldMatcher.match(parameterName, ScriptService.SCRIPT_FILE)) {
                if (parameterValue instanceof String || parameterValue == null) {
                    script = (String) parameterValue;
                    type = ScriptType.FILE;
                    if (removeMatchedEntries) {
                        itr.remove();
                    }
                } else {
                    throw new ScriptParseException("Value must be of type String: [" + parameterName + "]");
                }
            } else if (parseFieldMatcher.match(parameterName, ScriptType.INDEXED.getParseField()) || parseFieldMatcher.match(parameterName, ScriptService.SCRIPT_ID)) {
                if (parameterValue instanceof String || parameterValue == null) {
                    script = (String) parameterValue;
                    type = ScriptType.INDEXED;
                    if (removeMatchedEntries) {
                        itr.remove();
                    }
                } else {
                    throw new ScriptParseException("Value must be of type String: [" + parameterName + "]");
                }
            }
        }
        if (script == null) {
            throw new ScriptParseException("expected one of [{}], [{}] or [{}] fields, but found none", ScriptType.INLINE.getParseField()
                    .getPreferredName(), ScriptType.FILE.getParseField().getPreferredName(), ScriptType.INDEXED.getParseField()
                    .getPreferredName());
        }
        assert type != null : "if script is not null, type should definitely not be null";
        return createScript(script, type, lang, params);
    }

}