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

    private final static Map<ScriptService.ScriptType, Setting<Boolean>> SCRIPT_TYPE_SETTING_MAP;

    static {
        Map<ScriptService.ScriptType, Setting<Boolean>> scriptTypeSettingMap = new HashMap<>();
        for (ScriptService.ScriptType scriptType : ScriptService.ScriptType.values()) {
            scriptTypeSettingMap.put(scriptType, Setting.boolSetting(
                ScriptModes.sourceKey(scriptType),
                scriptType.getDefaultScriptEnabled(),
                Property.NodeScope));
        }
        SCRIPT_TYPE_SETTING_MAP = Collections.unmodifiableMap(scriptTypeSettingMap);
    }

    private final Map<ScriptContext, Setting<Boolean>> scriptContextSettingMap;
    private final List<Setting<Boolean>> scriptLanguageSettings;
    private final Setting<String> defaultScriptLanguageSetting;

    public ScriptSettings(ScriptEngineRegistry scriptEngineRegistry, ScriptContextRegistry scriptContextRegistry) {
        Map<ScriptContext, Setting<Boolean>> scriptContextSettingMap = contextSettings(scriptContextRegistry);
        this.scriptContextSettingMap = Collections.unmodifiableMap(scriptContextSettingMap);

        List<Setting<Boolean>> scriptLanguageSettings = languageSettings(SCRIPT_TYPE_SETTING_MAP, scriptContextSettingMap, scriptEngineRegistry, scriptContextRegistry);
        this.scriptLanguageSettings = Collections.unmodifiableList(scriptLanguageSettings);

        this.defaultScriptLanguageSetting = new Setting<>("script.default_lang", DEFAULT_LANG, setting -> {
            if (!"groovy".equals(setting) && !scriptEngineRegistry.getRegisteredLanguages().containsKey(setting)) {
                throw new IllegalArgumentException("unregistered default language [" + setting + "]");
            }
            return setting;
        }, Property.NodeScope);
    }

    private static Map<ScriptContext, Setting<Boolean>> contextSettings(ScriptContextRegistry scriptContextRegistry) {
        Map<ScriptContext, Setting<Boolean>> scriptContextSettingMap = new HashMap<>();
        for (ScriptContext scriptContext : scriptContextRegistry.scriptContexts()) {
            scriptContextSettingMap.put(scriptContext,
                    Setting.boolSetting(ScriptModes.operationKey(scriptContext), false, Property.NodeScope));
        }
        return scriptContextSettingMap;
    }

    private static List<Setting<Boolean>> languageSettings(Map<ScriptService.ScriptType, Setting<Boolean>> scriptTypeSettingMap,
                                                              Map<ScriptContext, Setting<Boolean>> scriptContextSettingMap,
                                                              ScriptEngineRegistry scriptEngineRegistry,
                                                              ScriptContextRegistry scriptContextRegistry) {
        final List<Setting<Boolean>> scriptModeSettings = new ArrayList<>();

        for (final Class<? extends ScriptEngineService> scriptEngineService : scriptEngineRegistry.getRegisteredScriptEngineServices()) {
            if (scriptEngineService == NativeScriptEngineService.class) {
                // native scripts are always enabled, and their settings can not be changed
                continue;
            }
            final String language = scriptEngineRegistry.getLanguage(scriptEngineService);
            for (final ScriptService.ScriptType scriptType : ScriptService.ScriptType.values()) {
                // Top level, like "script.engine.groovy.inline"
                final boolean defaultNonFileScriptMode = scriptEngineRegistry.getDefaultInlineScriptEnableds().get(language);
                boolean defaultLangAndType = defaultNonFileScriptMode;
                // Files are treated differently because they are never default-deny
                if (ScriptService.ScriptType.FILE == scriptType) {
                    defaultLangAndType = ScriptService.ScriptType.FILE.getDefaultScriptEnabled();
                }
                final boolean defaultIfNothingSet = defaultLangAndType;

                // Setting for something like "script.engine.groovy.inline"
                final Setting<Boolean> langAndTypeSetting = Setting.boolSetting(ScriptModes.getGlobalKey(language, scriptType),
                        defaultLangAndType, Property.NodeScope);
                scriptModeSettings.add(langAndTypeSetting);

                for (ScriptContext scriptContext : scriptContextRegistry.scriptContexts()) {
                    final String langAndTypeAndContextName = ScriptModes.getKey(language, scriptType, scriptContext);
                    // A function that, given a setting, will return what the default should be. Since the fine-grained script settings
                    // read from a bunch of different places this is implemented in this way.
                    Function<Settings, String> defaultSettingFn = settings -> {
                        final Setting<Boolean> globalOpSetting = scriptContextSettingMap.get(scriptContext);
                        final Setting<Boolean> globalTypeSetting = scriptTypeSettingMap.get(scriptType);
                        final Setting<Boolean> langAndTypeAndContextSetting = Setting.boolSetting(langAndTypeAndContextName,
                                defaultIfNothingSet, Property.NodeScope);

                        // fallback logic for script mode settings
                        if (langAndTypeAndContextSetting.exists(settings)) {
                            // like: "script.engine.groovy.inline.aggs: true"
                            return langAndTypeAndContextSetting.get(settings).toString();
                        } else if (langAndTypeSetting.exists(settings)) {
                            // like: "script.engine.groovy.inline: true"
                            return langAndTypeSetting.get(settings).toString();
                        } else if (globalOpSetting.exists(settings)) {
                            // like: "script.aggs: true"
                            return globalOpSetting.get(settings).toString();
                        } else if (globalTypeSetting.exists(settings)) {
                            // like: "script.inline: true"
                            return globalTypeSetting.get(settings).toString();
                        } else {
                            // Nothing is set!
                            return Boolean.toString(defaultIfNothingSet);
                        }
                    };
                    // The actual setting for finest grained script settings
                    Setting<Boolean> setting = Setting.boolSetting(langAndTypeAndContextName, defaultSettingFn, Property.NodeScope);
                    scriptModeSettings.add(setting);
                }
            }
        }
        return scriptModeSettings;
    }

    public Iterable<Setting<Boolean>> getScriptTypeSettings() {
        return Collections.unmodifiableCollection(SCRIPT_TYPE_SETTING_MAP.values());
    }

    public Iterable<Setting<Boolean>> getScriptContextSettings() {
        return Collections.unmodifiableCollection(scriptContextSettingMap.values());
    }

    public Iterable<Setting<Boolean>> getScriptLanguageSettings() {
        return scriptLanguageSettings;
    }

    public Setting<String> getDefaultScriptLanguageSetting() {
        return defaultScriptLanguageSetting;
    }
}
