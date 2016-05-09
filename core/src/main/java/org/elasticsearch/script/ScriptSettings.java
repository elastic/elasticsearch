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

import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.script.ScriptService;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class ScriptSettings {

    public final static String DEFAULT_LANG = "groovy";

    private final static Map<ScriptService.ScriptType, Setting<ScriptMode>> SCRIPT_TYPE_SETTING_MAP;

    static {
        Map<ScriptService.ScriptType, Setting<ScriptMode>> scriptTypeSettingMap = new HashMap<>();
        for (ScriptService.ScriptType scriptType : ScriptService.ScriptType.values()) {
            scriptTypeSettingMap.put(scriptType, new Setting<>(
                ScriptModes.sourceKey(scriptType),
                scriptType.getDefaultScriptMode().getMode(),
                ScriptMode::parse,
                Property.NodeScope));
        }
        SCRIPT_TYPE_SETTING_MAP = Collections.unmodifiableMap(scriptTypeSettingMap);
    }

    private final Map<ScriptContext, Setting<ScriptMode>> scriptContextSettingMap;
    private final List<Setting<ScriptMode>> scriptLanguageSettings;
    private final Setting<String> defaultScriptLanguageSetting;

    public ScriptSettings(ScriptEngineRegistry scriptEngineRegistry, ScriptContextRegistry scriptContextRegistry) {
        Map<ScriptContext, Setting<ScriptMode>> scriptContextSettingMap = contextSettings(scriptContextRegistry);
        this.scriptContextSettingMap = Collections.unmodifiableMap(scriptContextSettingMap);

        List<Setting<ScriptMode>> scriptLanguageSettings = languageSettings(SCRIPT_TYPE_SETTING_MAP, scriptContextSettingMap, scriptEngineRegistry, scriptContextRegistry);
        this.scriptLanguageSettings = Collections.unmodifiableList(scriptLanguageSettings);

        this.defaultScriptLanguageSetting = new Setting<>("script.default_lang", DEFAULT_LANG, setting -> {
            if (!"groovy".equals(setting) && !scriptEngineRegistry.getRegisteredLanguages().containsKey(setting)) {
                throw new IllegalArgumentException("unregistered default language [" + setting + "]");
            }
            return setting;
        }, Property.NodeScope);
    }

    private static Map<ScriptContext, Setting<ScriptMode>> contextSettings(ScriptContextRegistry scriptContextRegistry) {
        Map<ScriptContext, Setting<ScriptMode>> scriptContextSettingMap = new HashMap<>();
        for (ScriptContext scriptContext : scriptContextRegistry.scriptContexts()) {
            scriptContextSettingMap.put(scriptContext, new Setting<>(
                ScriptModes.operationKey(scriptContext),
                ScriptMode.OFF.getMode(),
                ScriptMode::parse,
                Property.NodeScope
            ));
        }
        return scriptContextSettingMap;
    }

    private static List<Setting<ScriptMode>> languageSettings(
        Map<ScriptService.ScriptType, Setting<ScriptMode>> scriptTypeSettingMap,
        Map<ScriptContext, Setting<ScriptMode>> scriptContextSettingMap,
        ScriptEngineRegistry scriptEngineRegistry,
        ScriptContextRegistry scriptContextRegistry) {
        List<Setting<ScriptMode>> scriptModeSettings = new ArrayList<>();
        for (Class<? extends ScriptEngineService> scriptEngineService : scriptEngineRegistry.getRegisteredScriptEngineServices()) {
            List<String> languages = scriptEngineRegistry.getLanguages(scriptEngineService);

            for (String language : languages) {
                if (NativeScriptEngineService.TYPES.contains(language)) {
                    // native scripts are always enabled, and their settings can not be changed
                    continue;
                }
                ScriptMode defaultNonFileScriptMode = scriptEngineRegistry.getDefaultInlineScriptModes().get(language);
                for (final ScriptService.ScriptType scriptType : ScriptService.ScriptType.values()) {
                    // This will be easier once ScriptMode is transitioned to booleans
                    if (scriptType.getDefaultScriptMode() == ScriptMode.ON) {
                        defaultNonFileScriptMode = ScriptMode.ON;
                    }
                    final ScriptMode defaultScriptMode = defaultNonFileScriptMode;
                    // Like "script.engine.groovy.inline"
                    final Setting<ScriptMode> langGlobalSetting = new Setting<>(ScriptModes.getGlobalKey(language, scriptType),
                            defaultScriptMode.toString(), ScriptMode::parse, Property.NodeScope);
                    scriptModeSettings.add(langGlobalSetting);

                    for (ScriptContext scriptContext : scriptContextRegistry.scriptContexts()) {
                        Function<Settings, String> defaultSetting = settings -> {
                            // fallback logic for script mode settings

                            // the first fallback is other types registered by the same script engine service
                            // e.g., "py.inline.aggs" is in the settings but a script with lang "python" is executed
                            Map<String, List<String>> languageSettings =
                                languages
                                    .stream()
                                    .map(lang -> Tuple.tuple(lang, settings.get(ScriptModes.getKey(lang, scriptType, scriptContext))))
                                    .filter(tuple -> tuple.v2() != null)
                                    .collect(Collectors.groupingBy(Tuple::v2, Collectors.mapping(Tuple::v1, Collectors.toList())));
                            if (!languageSettings.isEmpty()) {
                                if (languageSettings.size() > 1) {
                                    throw new IllegalArgumentException("conflicting settings [" + languageSettings.toString() + "] for language [" + language + "]");
                                }
                                return languageSettings.keySet().iterator().next();
                            }

                            // the next fallback is settings configured for this engine and script type
                            if (langGlobalSetting.exists(settings)) {
                                return langGlobalSetting.get(settings).getMode();
                            }

                            // the next fallback is global operation-based settings (e.g., "script.aggs: false")
                            Setting<ScriptMode> setting = scriptContextSettingMap.get(scriptContext);
                            if (setting.exists(settings)) {
                                return setting.get(settings).getMode();
                            }

                            // the next fallback is global source-based settings (e.g., "script.inline: false")
                            Setting<ScriptMode> scriptTypeSetting = scriptTypeSettingMap.get(scriptType);
                            if (scriptTypeSetting.exists(settings)) {
                                return scriptTypeSetting.get(settings).getMode();
                            }

                            // the final fallback is the default for the type
                            return defaultScriptMode.toString();
                        };
                        Setting<ScriptMode> setting =
                            new Setting<>(
                                ScriptModes.getKey(language, scriptType, scriptContext),
                                defaultSetting,
                                ScriptMode::parse,
                                Property.NodeScope);
                        scriptModeSettings.add(setting);
                    }
                }
            }
        }
        return scriptModeSettings;
    }

    public Iterable<Setting<ScriptMode>> getScriptTypeSettings() {
        return Collections.unmodifiableCollection(SCRIPT_TYPE_SETTING_MAP.values());
    }

    public Iterable<Setting<ScriptMode>> getScriptContextSettings() {
        return Collections.unmodifiableCollection(scriptContextSettingMap.values());
    }

    public Iterable<Setting<ScriptMode>> getScriptLanguageSettings() {
        return scriptLanguageSettings;
    }

    public Setting<String> getDefaultScriptLanguageSetting() {
        return defaultScriptLanguageSetting;
    }
}
