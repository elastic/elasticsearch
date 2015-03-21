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

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ToXContent.Params;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.script.ScriptService.ScriptType;

import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;

public class ScriptParameterParser {

    public static final String FILE_SUFFIX = "_file";
    public static final String INDEXED_SUFFIX = "_id";

    private Map<String, ScriptParameterValue> parameterValues = new HashMap<>();
    private Set<ParseField> inlineParameters;
    private Set<ParseField> fileParameters;
    private Set<ParseField> indexedParameters;
    private String lang = null;

    public ScriptParameterParser() {
        this(null);
    }

    public ScriptParameterParser(Set<String> parameterNames) {
        if (parameterNames == null || parameterNames.isEmpty()) {
            inlineParameters = Collections.singleton(ScriptService.SCRIPT_INLINE);
            fileParameters = Collections.singleton(ScriptService.SCRIPT_FILE);
            indexedParameters = Collections.singleton(ScriptService.SCRIPT_ID);
        } else {
            inlineParameters = new HashSet<>();
            fileParameters = new HashSet<>();
            indexedParameters = new HashSet<>();
            for (String parameterName : parameterNames) {
                if (ScriptService.SCRIPT_LANG.match(parameterName)) {
                    throw new IllegalArgumentException("lang is reserved and cannot be used as a parameter name");
                }
                inlineParameters.add(new ParseField(parameterName));
                fileParameters.add(new ParseField(parameterName + FILE_SUFFIX));
                indexedParameters.add(new ParseField(parameterName + INDEXED_SUFFIX));
            }
        }
    }

    public boolean token(String currentFieldName, XContentParser.Token token, XContentParser parser) throws IOException {
        if (token == XContentParser.Token.VALUE_STRING) {
            if (ScriptService.SCRIPT_LANG.match(currentFieldName)) {
                lang  = parser.text();
                return true;
            } else {
                for (ParseField parameter : inlineParameters) {
                    if (parameter.match(currentFieldName)) {
                        String coreParameterName = parameter.getPreferredName();
                        putParameterValue(coreParameterName, parser.textOrNull(), ScriptType.INLINE);
                        return true;
                    }
                }
                for (ParseField parameter : fileParameters) {
                    if (parameter.match(currentFieldName)) {
                        String coreParameterName = parameter.getPreferredName().replace(FILE_SUFFIX, "");
                        putParameterValue(coreParameterName, parser.textOrNull(), ScriptType.FILE);
                        return true;
                    }
                }
                for (ParseField parameter : indexedParameters) {
                    if (parameter.match(currentFieldName)) {
                        String coreParameterName = parameter.getPreferredName().replace(INDEXED_SUFFIX, "");
                        putParameterValue(coreParameterName, parser.textOrNull(), ScriptType.INDEXED);
                        return true;
                    }
                }
            }
        }
        return false;
    }

    public void parseConfig(Map<String, Object> config, boolean removeMatchedEntries) {
        for (Iterator<Entry<String, Object>> itr = config.entrySet().iterator(); itr.hasNext();) {
            Entry<String, Object> entry = itr.next();
            String parameterName = entry.getKey();
            Object parameterValue = entry.getValue();
            if (ScriptService.SCRIPT_LANG.match(parameterName)) {
               if (parameterValue instanceof String) {
                   lang = (String) parameterValue;
                   if (removeMatchedEntries) {
                       itr.remove();
                   }
               } else {
                   throw new ScriptParameterParseException("Value must be of type String: [" + parameterName + "]");
               }
            } else {
                for (ParseField parameter : inlineParameters) {
                    if (parameter.match(parameterName)) {
                        String coreParameterName = parameter.getPreferredName();
                        String stringValue = null;
                        if (parameterValue instanceof String) {
                            stringValue = (String) parameterValue;
                        } else {
                            throw new ScriptParameterParseException("Value must be of type String: [" + parameterName + "]");
                        }
                        putParameterValue(coreParameterName, stringValue, ScriptType.INLINE);
                        if (removeMatchedEntries) {
                            itr.remove();
                        }
                    }
                }
                for (ParseField parameter : fileParameters) {
                    if (parameter.match(parameterName)) {
                        String coreParameterName = parameter.getPreferredName().replace(FILE_SUFFIX, "");;
                        String stringValue = null;
                        if (parameterValue instanceof String) {
                            stringValue = (String) parameterValue;
                        } else {
                            throw new ScriptParameterParseException("Value must be of type String: [" + parameterName + "]");
                        }
                        putParameterValue(coreParameterName, stringValue, ScriptType.FILE);
                        if (removeMatchedEntries) {
                            itr.remove();
                        }
                    }
                }
                for (ParseField parameter : indexedParameters) {
                    if (parameter.match(parameterName)) {
                        String coreParameterName = parameter.getPreferredName().replace(INDEXED_SUFFIX, "");
                        String stringValue = null;
                        if (parameterValue instanceof String) {
                            stringValue = (String) parameterValue;
                        } else {
                            throw new ScriptParameterParseException("Value must be of type String: [" + parameterName + "]");
                        }
                        putParameterValue(coreParameterName, stringValue, ScriptType.INDEXED);
                        if (removeMatchedEntries) {
                            itr.remove();
                        }
                    }
                }
            }
        }
    }

    private void putParameterValue(String coreParameterName, String script, ScriptType scriptType) {
        if (parameterValues.get(coreParameterName) == null) {
            parameterValues.put(coreParameterName, new ScriptParameterValue(script, scriptType));
        } else {
            throw new ScriptParameterParseException("Only one of [" + coreParameterName + ", " + coreParameterName
                    + FILE_SUFFIX + ", " + coreParameterName + INDEXED_SUFFIX + "] is allowed.");
        }
    }

    public void parseParams(Params params) {
        lang = params.param(ScriptService.SCRIPT_LANG.getPreferredName());
        for (ParseField parameter : inlineParameters) {
            String value = params.param(parameter.getPreferredName());
            if (value != null) {
                String coreParameterName = parameter.getPreferredName();
                putParameterValue(coreParameterName, value, ScriptType.INLINE);
                
            }
        }
        for (ParseField parameter : fileParameters) {
            String value = params.param(parameter.getPreferredName());
            if (value != null) {
                String coreParameterName = parameter.getPreferredName().replace(FILE_SUFFIX, "");
                putParameterValue(coreParameterName, value, ScriptType.FILE);
                
            }
        }
        for (ParseField parameter : indexedParameters) {
            String value = params.param(parameter.getPreferredName());
            if (value != null) {
                String coreParameterName = parameter.getPreferredName().replace(INDEXED_SUFFIX, "");
                putParameterValue(coreParameterName, value, ScriptType.INDEXED);
                
            }
        }
    }

    public ScriptParameterValue getDefaultScriptParameterValue() {
        return getScriptParameterValue(ScriptService.SCRIPT_INLINE.getPreferredName());
    }

    public ScriptParameterValue getScriptParameterValue(String parameterName) {
        return parameterValues.get(parameterName);
    }

    public String lang() {
        return lang;
    }

    public static class ScriptParameterValue {
        private String script;
        private ScriptType scriptType;

        public ScriptParameterValue(String script, ScriptType scriptType) {
            this.script = script;
            this.scriptType = scriptType;
        }

        public String script() {
            return script;
        }

        public ScriptType scriptType() {
            return scriptType;
        }
    }

    public static class ScriptParameterParseException extends ElasticsearchException {

        public ScriptParameterParseException(String msg) {
            super(msg);
        }
    }
}
