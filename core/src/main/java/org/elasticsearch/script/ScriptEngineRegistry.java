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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.elasticsearch.common.Strings;

public class ScriptEngineRegistry {

    private final Map<Class<? extends ScriptEngineService>, String> registeredScriptEngineServices;
    private final Map<String, Class<? extends ScriptEngineService>> registeredLanguages;
    private final Map<String, ScriptMode> defaultInlineScriptModes;

    public ScriptEngineRegistry(Iterable<ScriptEngineRegistration> registrations) {
        Objects.requireNonNull(registrations);
        Map<Class<? extends ScriptEngineService>, String> registeredScriptEngineServices = new HashMap<>();
        Map<String, Class<? extends ScriptEngineService>> registeredLanguages = new HashMap<>();
        Map<String, ScriptMode> inlineScriptModes = new HashMap<>();
        for (ScriptEngineRegistration registration : registrations) {
            String oldLanguage = registeredScriptEngineServices.putIfAbsent(registration.getScriptEngineService(),
                    registration.getScriptEngineLanguage());
            if (oldLanguage != null) {
                throw new IllegalArgumentException("script engine service [" + registration.getScriptEngineService() +
                                "] already registered for language [" + oldLanguage + "]");
            }
            String language = registration.getScriptEngineLanguage();
            Class<? extends ScriptEngineService> scriptEngineServiceClazz =
                    registeredLanguages.putIfAbsent(language, registration.getScriptEngineService());
            if (scriptEngineServiceClazz != null) {
                throw new IllegalArgumentException("scripting language [" + language + "] already registered for script engine service [" +
                                scriptEngineServiceClazz.getCanonicalName() + "]");
            }
            inlineScriptModes.put(language, registration.getDefaultInlineScriptMode());
        }

        this.registeredScriptEngineServices = Collections.unmodifiableMap(registeredScriptEngineServices);
        this.registeredLanguages = Collections.unmodifiableMap(registeredLanguages);
        this.defaultInlineScriptModes = Collections.unmodifiableMap(inlineScriptModes);
    }

    Iterable<Class<? extends ScriptEngineService>> getRegisteredScriptEngineServices() {
        return registeredScriptEngineServices.keySet();
    }

    String getLanguage(Class<? extends ScriptEngineService> scriptEngineService) {
        Objects.requireNonNull(scriptEngineService);
        return registeredScriptEngineServices.get(scriptEngineService);
    }

    Map<String, Class<? extends ScriptEngineService>> getRegisteredLanguages() {
        return registeredLanguages;
    }

    Map<String, ScriptMode> getDefaultInlineScriptModes() {
        return this.defaultInlineScriptModes;
    }

    public static class ScriptEngineRegistration {
        private final Class<? extends ScriptEngineService> scriptEngineService;
        private final String scriptEngineLanguage;
        private final ScriptMode defaultInlineScriptMode;

        /**
         * Register a script engine service with the default of inline scripts disabled
         */
        public ScriptEngineRegistration(Class<? extends ScriptEngineService> scriptEngineService, String scriptEngineLanguage) {
            this(scriptEngineService, scriptEngineLanguage, ScriptMode.OFF);
        }

        /**
         * Register a script engine service with the given default mode for inline scripts
         */
        public ScriptEngineRegistration(Class<? extends ScriptEngineService> scriptEngineService, String scriptEngineLanguage,
                                        ScriptMode defaultInlineScriptMode) {
            Objects.requireNonNull(scriptEngineService);
            if (Strings.hasText(scriptEngineLanguage) == false) {
                throw new IllegalArgumentException("languages for script engine service [" +
                                scriptEngineService.getCanonicalName() + "] should be a non-empty string");
            }
            this.scriptEngineService = scriptEngineService;
            this.scriptEngineLanguage = scriptEngineLanguage;
            this.defaultInlineScriptMode = defaultInlineScriptMode;
        }

        Class<? extends ScriptEngineService> getScriptEngineService() {
            return scriptEngineService;
        }

        String getScriptEngineLanguage() {
            return scriptEngineLanguage;
        }

        ScriptMode getDefaultInlineScriptMode() {
            return defaultInlineScriptMode;
        }
    }

}
