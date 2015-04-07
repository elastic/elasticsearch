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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.elasticsearch.ElasticsearchIllegalArgumentException;

import java.util.List;

/**
 * Registry for operations that use scripts as part of their execution. Can be standard operations of custom defined ones (via plugin).
 * Allows plugins to register custom operations that they use scripts for, via {@link ScriptModule#registerScriptContext(ScriptContext)}.
 * Scripts can be enabled/disabled via fine-grained settings for each single registered operation.
 */
public final class ScriptContextRegistry {
    static final ImmutableSet<String> RESERVED_SCRIPT_CONTEXTS = reservedScriptContexts();

    private final ImmutableList<ScriptContext> scriptContexts;
    private final ImmutableSet<String> scriptContextsKeys;

    ScriptContextRegistry(List<ScriptContext> customScriptContexts) {
        for (ScriptContext customScriptContext : customScriptContexts) {
            validateScriptContext(customScriptContext.key());
        }

        this.scriptContexts = ImmutableList.<ScriptContext>builder().add(ScriptContext.Standard.values()).addAll(customScriptContexts).build();

        ImmutableSet.Builder<String> builder = ImmutableSet.builder();
        for (ScriptContext.Standard scriptContext : ScriptContext.Standard.values()) {
            builder.add(scriptContext.key());
        }
        for (ScriptContext customScriptContext : customScriptContexts) {
            builder.add(customScriptContext.key());
        }
        this.scriptContextsKeys = builder.build();
    }

    /**
     * @return a list that contains all the supported {@link ScriptContext}s, both standard ones and registered via plugins
     */
    ImmutableList<ScriptContext> scriptContexts() {
        return scriptContexts;
    }

    /**
     * @return <tt>true</tt> if the provided {@link ScriptContext} is supported, <tt>false</tt> otherwise
     */
    boolean isSupportedContext(ScriptContext scriptContext) {
        return scriptContextsKeys.contains(scriptContext.key());
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
        for (ScriptContext.Standard scriptContext : ScriptContext.Standard.values()) {
            builder.add(scriptContext.key());
        }
        builder.add("script").add("engine").add(ScriptContext.GENERIC_PLUGIN.key());
        return builder.build();
    }
}
