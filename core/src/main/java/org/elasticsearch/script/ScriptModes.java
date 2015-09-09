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

import com.google.common.collect.ImmutableMap;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.script.ScriptService.ScriptType;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * Holds the {@link org.elasticsearch.script.ScriptMode}s for each of the different scripting languages available,
 * each script source and each scripted operation.
 */
public class ScriptModes {

    static final String SCRIPT_SETTINGS_PREFIX = "script.";
    static final String ENGINE_SETTINGS_PREFIX = "script.engine";

    final ImmutableMap<String, ScriptMode> scriptModes;

    ScriptModes(Map<String, ScriptEngineService> scriptEngines, ScriptContextRegistry scriptContextRegistry, Settings settings) {
        //filter out the native engine as we don't want to apply fine grained settings to it.
        //native scripts are always on as they are static by definition.
        Map<String, ScriptEngineService> filteredEngines = new HashMap<>(scriptEngines);
        filteredEngines.remove(NativeScriptEngineService.NAME);
        this.scriptModes = buildScriptModeSettingsMap(settings, filteredEngines, scriptContextRegistry);
    }

    private static ImmutableMap<String, ScriptMode> buildScriptModeSettingsMap(Settings settings, Map<String, ScriptEngineService> scriptEngines, ScriptContextRegistry scriptContextRegistry) {
        HashMap<String, ScriptMode> scriptModesMap = new HashMap<>();

        //file scripts are enabled by default, for any language
        addGlobalScriptTypeModes(scriptEngines.keySet(), scriptContextRegistry, ScriptType.FILE, ScriptMode.ON, scriptModesMap);
        //indexed scripts are enabled by default only for sandboxed languages
        addGlobalScriptTypeModes(scriptEngines.keySet(), scriptContextRegistry, ScriptType.INDEXED, ScriptMode.SANDBOX, scriptModesMap);
        //dynamic scripts are enabled by default only for sandboxed languages
        addGlobalScriptTypeModes(scriptEngines.keySet(), scriptContextRegistry, ScriptType.INLINE, ScriptMode.SANDBOX, scriptModesMap);

        processSourceBasedGlobalSettings(settings, scriptEngines, scriptContextRegistry, scriptModesMap);
        processOperationBasedGlobalSettings(settings, scriptEngines, scriptContextRegistry, scriptModesMap);
        processEngineSpecificSettings(settings, scriptEngines, scriptContextRegistry, scriptModesMap);
        return ImmutableMap.copyOf(scriptModesMap);
    }

    private static void processSourceBasedGlobalSettings(Settings settings, Map<String, ScriptEngineService> scriptEngines, ScriptContextRegistry scriptContextRegistry, Map<String, ScriptMode> scriptModes) {
        //read custom source based settings for all operations (e.g. script.indexed: on)
        for (ScriptType scriptType : ScriptType.values()) {
            String scriptTypeSetting = settings.get(SCRIPT_SETTINGS_PREFIX + scriptType);
            if (Strings.hasLength(scriptTypeSetting)) {
                ScriptMode scriptTypeMode = ScriptMode.parse(scriptTypeSetting);
                addGlobalScriptTypeModes(scriptEngines.keySet(), scriptContextRegistry, scriptType, scriptTypeMode, scriptModes);
            }
        }
    }

    private static void processOperationBasedGlobalSettings(Settings settings, Map<String, ScriptEngineService> scriptEngines, ScriptContextRegistry scriptContextRegistry, Map<String, ScriptMode> scriptModes) {
        //read custom op based settings for all sources (e.g. script.aggs: off)
        //op based settings take precedence over source based settings, hence they get expanded later
        for (ScriptContext scriptContext : scriptContextRegistry.scriptContexts()) {
            ScriptMode scriptMode = getScriptContextMode(settings, SCRIPT_SETTINGS_PREFIX, scriptContext);
            if (scriptMode != null) {
                addGlobalScriptContextModes(scriptEngines.keySet(), scriptContext, scriptMode, scriptModes);
            }
        }
    }

    private static void processEngineSpecificSettings(Settings settings, Map<String, ScriptEngineService> scriptEngines, ScriptContextRegistry scriptContextRegistry, Map<String, ScriptMode> scriptModes) {
        Map<String, Settings> langGroupedSettings = settings.getGroups(ENGINE_SETTINGS_PREFIX, true);
        for (Map.Entry<String, Settings> langSettings : langGroupedSettings.entrySet()) {
            //read engine specific settings that refer to a non existing script lang will be ignored
            ScriptEngineService scriptEngineService = scriptEngines.get(langSettings.getKey());
            if (scriptEngineService != null) {
                for (ScriptType scriptType : ScriptType.values()) {
                    String scriptTypePrefix = scriptType + ".";
                    for (ScriptContext scriptContext : scriptContextRegistry.scriptContexts()) {
                        ScriptMode scriptMode = getScriptContextMode(langSettings.getValue(), scriptTypePrefix, scriptContext);
                        if (scriptMode != null) {
                            addScriptMode(scriptEngineService, scriptType, scriptContext, scriptMode, scriptModes);
                        }
                    }
                }
            }
        }
    }

    private static ScriptMode getScriptContextMode(Settings settings, String prefix, ScriptContext scriptContext) {
        String settingValue = settings.get(prefix + scriptContext.getKey());
        if (Strings.hasLength(settingValue)) {
            return ScriptMode.parse(settingValue);
        }
        return null;
    }

    private static void addGlobalScriptTypeModes(Set<String> langs, ScriptContextRegistry scriptContextRegistry, ScriptType scriptType, ScriptMode scriptMode, Map<String, ScriptMode> scriptModes) {
        for (String lang : langs) {
            for (ScriptContext scriptContext : scriptContextRegistry.scriptContexts()) {
                addScriptMode(lang, scriptType, scriptContext, scriptMode, scriptModes);
            }
        }
    }

    private static void addGlobalScriptContextModes(Set<String> langs, ScriptContext scriptContext, ScriptMode scriptMode, Map<String, ScriptMode> scriptModes) {
        for (String lang : langs) {
            for (ScriptType scriptType : ScriptType.values()) {
                addScriptMode(lang, scriptType, scriptContext, scriptMode, scriptModes);
            }
        }
    }

    private static void addScriptMode(ScriptEngineService scriptEngineService, ScriptType scriptType, ScriptContext scriptContext,
                                      ScriptMode scriptMode, Map<String, ScriptMode> scriptModes) {
        //expand the lang specific settings to all of the different names given to each scripting language
        for (String scriptEngineName : scriptEngineService.types()) {
            addScriptMode(scriptEngineName, scriptType, scriptContext, scriptMode, scriptModes);
        }
    }

    private static void addScriptMode(String lang, ScriptType scriptType, ScriptContext scriptContext, ScriptMode scriptMode, Map<String, ScriptMode> scriptModes) {
        scriptModes.put(ENGINE_SETTINGS_PREFIX + "." + lang + "." + scriptType + "." + scriptContext.getKey(), scriptMode);
    }

    /**
     * Returns the script mode for a script of a certain written in a certain language,
     * of a certain type and executing as part of a specific operation/api.
     *
     * @param lang the language that the script is written in
     * @param scriptType the type of the script
     * @param scriptContext the operation that requires the execution of the script
     * @return whether scripts are on, off, or enabled only for sandboxed languages
     */
    public ScriptMode getScriptMode(String lang, ScriptType scriptType, ScriptContext scriptContext) {
        //native scripts are always on as they are static by definition
        if (NativeScriptEngineService.NAME.equals(lang)) {
            return ScriptMode.ON;
        }
        ScriptMode scriptMode = scriptModes.get(ENGINE_SETTINGS_PREFIX + "." + lang + "." + scriptType + "." + scriptContext.getKey());
        if (scriptMode == null) {
            throw new IllegalArgumentException("script mode not found for lang [" + lang + "], script_type [" + scriptType + "], operation [" + scriptContext.getKey() + "]");
        }
        return scriptMode;
    }

    @Override
    public String toString() {
        //order settings by key before printing them out, for readability
        TreeMap<String, ScriptMode> scriptModesTreeMap = new TreeMap<>();
        scriptModesTreeMap.putAll(scriptModes);
        StringBuilder stringBuilder = new StringBuilder();
        for (Map.Entry<String, ScriptMode> stringScriptModeEntry : scriptModesTreeMap.entrySet()) {
            stringBuilder.append(stringScriptModeEntry.getKey()).append(": ").append(stringScriptModeEntry.getValue()).append("\n");
        }
        return stringBuilder.toString();
    }
}
