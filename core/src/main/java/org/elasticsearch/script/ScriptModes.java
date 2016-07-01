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

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.script.ScriptService.ScriptType;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * Holds the boolean indicating the enabled mode for each of the different scripting languages available, each script source and each
 * scripted operation.
 */
public class ScriptModes {

    private static final String SCRIPT_SETTINGS_PREFIX = "script";
    private static final String ENGINE_SETTINGS_PREFIX = "script.engine";

    final Map<String, Boolean> scriptEnabled;

    ScriptModes(ScriptSettings scriptSettings, Settings settings) {
        HashMap<String, Boolean> scriptModes = new HashMap<>();
        for (Setting<Boolean> scriptModeSetting : scriptSettings.getScriptLanguageSettings()) {
            scriptModes.put(scriptModeSetting.getKey(), scriptModeSetting.get(settings));
        }
        this.scriptEnabled = Collections.unmodifiableMap(scriptModes);
    }

    /**
     * Returns the script mode for a script of a certain written in a certain language,
     * of a certain type and executing as part of a specific operation/api.
     *
     * @param lang the language that the script is written in
     * @param scriptType the type of the script
     * @param scriptContext the operation that requires the execution of the script
     * @return whether scripts are enabled (true) or disabled (false)
     */
    public boolean getScriptEnabled(String lang, ScriptType scriptType, ScriptContext scriptContext) {
        //native scripts are always enabled as they are static by definition
        if (NativeScriptEngineService.NAME.equals(lang)) {
            return true;
        }
        Boolean scriptMode = scriptEnabled.get(getKey(lang, scriptType, scriptContext));
        if (scriptMode == null) {
            throw new IllegalArgumentException("script mode not found for lang [" + lang + "], script_type [" + scriptType + "], operation [" + scriptContext.getKey() + "]");
        }
        return scriptMode;
    }

    static String operationKey(ScriptContext scriptContext) {
        return SCRIPT_SETTINGS_PREFIX + "." + scriptContext.getKey();
    }

    static String sourceKey(ScriptType scriptType) {
        return SCRIPT_SETTINGS_PREFIX + "." + scriptType.getScriptType();
    }

    static String getGlobalKey(String lang, ScriptType scriptType) {
        return ENGINE_SETTINGS_PREFIX + "." + lang + "." + scriptType;
    }

    static String getKey(String lang, ScriptType scriptType, ScriptContext scriptContext) {
        return ENGINE_SETTINGS_PREFIX + "." + lang + "." + scriptType + "." + scriptContext.getKey();
    }

    @Override
    public String toString() {
        //order settings by key before printing them out, for readability
        TreeMap<String, Boolean> scriptModesTreeMap = new TreeMap<>();
        scriptModesTreeMap.putAll(scriptEnabled);
        StringBuilder stringBuilder = new StringBuilder();
        for (Map.Entry<String, Boolean> stringScriptModeEntry : scriptModesTreeMap.entrySet()) {
            stringBuilder.append(stringScriptModeEntry.getKey()).append(": ").append(stringScriptModeEntry.getValue()).append("\n");
        }
        return stringBuilder.toString();
    }
}
