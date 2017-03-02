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

import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;

/**
 * Encapsulates logic for checking if a script is allowed to be compiled. Has two kinds of checks:
 * <ol>
 *   <li>Is this script allowed to be compiled in this context?
 *   <li>Have there been too many inline compilations lately?
 * </ol>
 */
public class ScriptPermits {
    private final ScriptModes scriptModes;
    private final ScriptContextRegistry scriptContextRegistry;

    private int totalCompilesPerMinute;
    private long lastInlineCompileTime;
    private double scriptsPerMinCounter;
    private double compilesAllowedPerNano;

    public ScriptPermits(Settings settings, ScriptSettings scriptSettings, ScriptContextRegistry scriptContextRegistry) {
        this.scriptModes = new ScriptModes(scriptSettings, settings);
        this.scriptContextRegistry = scriptContextRegistry;
        this.lastInlineCompileTime = System.nanoTime();
        this.setMaxCompilationsPerMinute(ScriptService.SCRIPT_MAX_COMPILATIONS_PER_MINUTE.get(settings));
    }

    void registerClusterSettingsListeners(ClusterSettings clusterSettings) {
        clusterSettings.addSettingsUpdateConsumer(ScriptService.SCRIPT_MAX_COMPILATIONS_PER_MINUTE, this::setMaxCompilationsPerMinute);
    }

    void setMaxCompilationsPerMinute(Integer newMaxPerMinute) {
        this.totalCompilesPerMinute = newMaxPerMinute;
        // Reset the counter to allow new compilations
        this.scriptsPerMinCounter = totalCompilesPerMinute;
        this.compilesAllowedPerNano = ((double) totalCompilesPerMinute) / TimeValue.timeValueMinutes(1).nanos();
    }

    /**
     * Check whether there have been too many compilations within the last minute, throwing a circuit breaking exception if so.
     * This is a variant of the token bucket algorithm: https://en.wikipedia.org/wiki/Token_bucket
     *
     * It can be thought of as a bucket with water, every time the bucket is checked, water is added proportional to the amount of time that
     * elapsed since the last time it was checked. If there is enough water, some is removed and the request is allowed. If there is not
     * enough water the request is denied. Just like a normal bucket, if water is added that overflows the bucket, the extra water/capacity
     * is discarded - there can never be more water in the bucket than the size of the bucket.
     */
    public void checkCompilationLimit() {
        long now = System.nanoTime();
        long timePassed = now - lastInlineCompileTime;
        lastInlineCompileTime = now;

        scriptsPerMinCounter += (timePassed) * compilesAllowedPerNano;

        // It's been over the time limit anyway, readjust the bucket to be level
        if (scriptsPerMinCounter > totalCompilesPerMinute) {
            scriptsPerMinCounter = totalCompilesPerMinute;
        }

        // If there is enough tokens in the bucket, allow the request and decrease the tokens by 1
        if (scriptsPerMinCounter >= 1) {
            scriptsPerMinCounter -= 1.0;
        } else {
            // Otherwise reject the request
            throw new CircuitBreakingException("[script] Too many dynamic script compilations within one minute, max: [" +
                            totalCompilesPerMinute + "/min]; please use on-disk, indexed, or scripts with parameters instead; " +
                            "this limit can be changed by the [" + ScriptService.SCRIPT_MAX_COMPILATIONS_PER_MINUTE.getKey() + "] setting");
        }
    }

    public boolean canExecuteScript(String lang, ScriptType scriptType, ScriptContext scriptContext) {
        assert lang != null;
        if (scriptContextRegistry.isSupportedContext(scriptContext) == false) {
            throw new IllegalArgumentException("script context [" + scriptContext.getKey() + "] not supported");
        }
        return scriptModes.getScriptEnabled(lang, scriptType, scriptContext);
    }
}
