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
    private final Map<String, ScriptEngineService> registeredLanguages;
    private final Map<String, Boolean> defaultInlineScriptEnableds;

    public ScriptEngineRegistry(Iterable<ScriptEngineService> registrations) {
        Objects.requireNonNull(registrations);
        Map<Class<? extends ScriptEngineService>, String> registeredScriptEngineServices = new HashMap<>();
        Map<String, ScriptEngineService> registeredLanguages = new HashMap<>();
        Map<String, Boolean> inlineScriptEnableds = new HashMap<>();
        for (ScriptEngineService service : registrations) {
            String oldLanguage = registeredScriptEngineServices.putIfAbsent(service.getClass(),
                    service.getType());
            if (oldLanguage != null) {
                throw new IllegalArgumentException("script engine service [" + service.getClass() +
                                "] already registered for language [" + oldLanguage + "]");
            }
            String language = service.getType();
            ScriptEngineService scriptEngineService =
                    registeredLanguages.putIfAbsent(language, service);
            if (scriptEngineService != null) {
                throw new IllegalArgumentException("scripting language [" + language + "] already registered for script engine service [" +
                    scriptEngineService.getClass().getCanonicalName() + "]");
            }
            inlineScriptEnableds.put(language, service.isInlineScriptEnabled());
        }

        this.registeredScriptEngineServices = Collections.unmodifiableMap(registeredScriptEngineServices);
        this.registeredLanguages = Collections.unmodifiableMap(registeredLanguages);
        this.defaultInlineScriptEnableds = Collections.unmodifiableMap(inlineScriptEnableds);
    }

    Iterable<Class<? extends ScriptEngineService>> getRegisteredScriptEngineServices() {
        return registeredScriptEngineServices.keySet();
    }

    String getLanguage(Class<? extends ScriptEngineService> scriptEngineService) {
        Objects.requireNonNull(scriptEngineService);
        return registeredScriptEngineServices.get(scriptEngineService);
    }

    public Map<String, ScriptEngineService> getRegisteredLanguages() {
        return registeredLanguages;
    }

    public Map<String, Boolean> getDefaultInlineScriptEnableds() {
        return this.defaultInlineScriptEnableds;
    }

}
