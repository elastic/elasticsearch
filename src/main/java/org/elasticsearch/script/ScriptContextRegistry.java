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

import com.google.common.collect.ImmutableSet;
import org.elasticsearch.ElasticsearchIllegalArgumentException;

import java.util.Set;

/**
 * Registry for operations that use a script as part of their execution.
 * Note that the suggest api is considered part of search for simplicity, as well as the percolate api.
 * Allows plugins to register custom operations that they use scripts for, via {@link ScriptModule}
 */
public final class ScriptContextRegistry {
    public static final String MAPPING = "mapping";
    public static final String UPDATE = "update";
    public static final String SEARCH = "search";
    public static final String AGGS = "aggs";
    public static final String PLUGINS = "plugins";

    static final String[] DEFAULT_CONTEXTS = new String[]{AGGS, MAPPING, PLUGINS, SEARCH, UPDATE};

    static final Set<String> RESERVED_SCRIPT_CONTEXTS = reservedScriptContexts();

    private final ImmutableSet<String> scriptContexts;

    ScriptContextRegistry(Set<String> customScriptContexts) {
        for (String customScriptContext : customScriptContexts) {
            validateScriptContext(customScriptContext);
        }
        this.scriptContexts = ImmutableSet.<String>builder().add(DEFAULT_CONTEXTS).addAll(customScriptContexts).build();
    }

    ImmutableSet<String> scriptContexts() {
        return scriptContexts;
    }

    //script contexts can be used in fine-grained settings, we need to be careful with what we allow here
    private void validateScriptContext(String scriptContext) {
        if (RESERVED_SCRIPT_CONTEXTS.contains(scriptContext)) {
            throw new ElasticsearchIllegalArgumentException("[" + scriptContext + "] is a reserved name, it cannot be registered as a custom script context");
        }
    }

    private static ImmutableSet<String> reservedScriptContexts() {
        ImmutableSet.Builder<String> builder = ImmutableSet.builder();
        for (ScriptService.ScriptType scriptType : ScriptService.ScriptType.values()) {
            builder.add(scriptType.toString());
        }
        builder.add(DEFAULT_CONTEXTS);
        builder.add("script").add("engine");
        return builder.build();
    }
}
