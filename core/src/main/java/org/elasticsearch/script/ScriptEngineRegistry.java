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

public class ScriptEngineRegistry {

    private final Map<Class<? extends ScriptEngineService>, List<String>> registeredScriptEngineServices;
    private final Map<String, Class<? extends ScriptEngineService>> registeredLanguages;

    public ScriptEngineRegistry(Iterable<ScriptEngineRegistration> registrations) {
        Objects.requireNonNull(registrations);
        Map<Class<? extends ScriptEngineService>, List<String>> registeredScriptEngineServices = new HashMap<>();
        Map<String, Class<? extends ScriptEngineService>> registeredLanguages = new HashMap<>();
        for (ScriptEngineRegistration registration : registrations) {
            List<String> languages =
                registeredScriptEngineServices.putIfAbsent(registration.getScriptEngineService(), Collections.unmodifiableList(registration.getScriptEngineLanguages()));
            if (languages != null) {
                throw new IllegalArgumentException("script engine service [" + registration.getScriptEngineService() + "] already registered for languages [" + String.join(",", languages) + "]");
            }

            for (String language : registration.getScriptEngineLanguages()) {
                Class<? extends ScriptEngineService> scriptEngineServiceClazz =
                    registeredLanguages.putIfAbsent(language, registration.getScriptEngineService());
                if (scriptEngineServiceClazz != null) {
                    throw new IllegalArgumentException("scripting language [" + language + "] already registered for script engine service [" + scriptEngineServiceClazz.getCanonicalName() + "]");
                }
            }
        }

        this.registeredScriptEngineServices = Collections.unmodifiableMap(registeredScriptEngineServices);
        this.registeredLanguages = Collections.unmodifiableMap(registeredLanguages);
    }

    Iterable<Class<? extends ScriptEngineService>> getRegisteredScriptEngineServices() {
        return registeredScriptEngineServices.keySet();
    }

    List<String> getLanguages(Class<? extends ScriptEngineService> scriptEngineService) {
        Objects.requireNonNull(scriptEngineService);
        return registeredScriptEngineServices.get(scriptEngineService);
    }

    Map<String, Class<? extends ScriptEngineService>> getRegisteredLanguages() {
        return registeredLanguages;
    }

    public static class ScriptEngineRegistration {
        private final Class<? extends ScriptEngineService> scriptEngineService;
        private final List<String> scriptEngineLanguages;

        public ScriptEngineRegistration(Class<? extends ScriptEngineService> scriptEngineService, List<String> scriptEngineLanguages) {
            Objects.requireNonNull(scriptEngineService);
            Objects.requireNonNull(scriptEngineLanguages);
            if (scriptEngineLanguages.isEmpty()) {
                throw new IllegalArgumentException("languages for script engine service [" + scriptEngineService.getCanonicalName() + "] should be non-empty");
            }
            this.scriptEngineService = scriptEngineService;
            this.scriptEngineLanguages = scriptEngineLanguages;
        }

        Class<? extends ScriptEngineService> getScriptEngineService() {
            return scriptEngineService;
        }

        List<String> getScriptEngineLanguages() {
            return scriptEngineLanguages;
        }
    }

}
