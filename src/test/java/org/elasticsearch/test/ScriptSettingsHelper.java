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

package org.elasticsearch.test;

import com.carrotsearch.randomizedtesting.generators.RandomInts;
import com.carrotsearch.randomizedtesting.generators.RandomPicks;
import com.google.common.collect.Sets;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptModes;
import org.elasticsearch.script.ScriptService;

import java.util.Random;
import java.util.Set;

import static org.elasticsearch.test.ElasticsearchTestCase.randomFrom;

class ScriptSettingsHelper {

    public static void addScriptSettings(ImmutableSettings.Builder builder, Random random, RequiresScripts requiresScripts) {
        if (requiresScripts == null) {
            //randomize script settings if scripting is not required (annotation is not there)
            if (random.nextBoolean()) {
                randomScriptTypeSetting(builder, random);
            }
            if (random.nextBoolean()) {
                randomScriptContextSetting(builder, random);
            }
        } else {
            addScriptSettings(builder, random, requiresScripts.lang(), requiresScripts.type(), requiresScripts.context());
        }
    }

    private static void addScriptSettings(ImmutableSettings.Builder builder, Random random, String[] langs, ScriptService.ScriptType[] scriptTypes, ScriptContext[] scriptContexts) {
        int randomInt = RandomInts.randomIntBetween(random, 1, 3);
        switch(randomInt) {
            case 1:
                //enable scripts for any lang, required sources only, any operation
                for (ScriptService.ScriptType scriptType : scriptTypes) {
                    builder.put(ScriptModes.SCRIPT_SETTINGS_PREFIX + scriptType, "on");
                }
                break;
            case 2:
                //per operation settings have the precedence over script type ones, hence we can randomize
                if (random.nextBoolean()) {
                    randomScriptTypeSetting(builder, random);
                }
                for (ScriptContext scriptContext : scriptContexts) {
                    //enable scripts for any lang, any source, required operations only
                    builder.put(ScriptModes.SCRIPT_SETTINGS_PREFIX + scriptContext, "on");
                }
                break;
            case 3:
                //per engine settings have the precedence over operation based ones and script type based ones, hence we can randomize
                if (random.nextBoolean()) {
                    randomScriptTypeSetting(builder, random);
                }
                if (random.nextBoolean()) {
                    randomScriptContextSetting(builder, random);
                }
                //enable scripts for specific lang only, required sources and required operations
                Set<ScriptContext> requiredScriptContextSet = Sets.newHashSet(scriptContexts);
                for (String lang : langs) {
                    for (ScriptService.ScriptType scriptType : scriptTypes) {
                        for (ScriptContext scriptContext : ScriptContext.values()) {
                            if (requiredScriptContextSet.contains(scriptContext)) {
                                builder.put(ScriptModes.ENGINE_SETTINGS_PREFIX + "." + lang + "." + scriptType + "." + scriptContext , "on");
                            } else {
                                //randomize non required operations
                                builder.put(ScriptModes.ENGINE_SETTINGS_PREFIX + "." + lang + "." + scriptType + "." + scriptContext , randomFrom("on", "off", "sandbox"));
                            }
                        }
                    }
                }
                break;
        }
    }

    private static void randomScriptTypeSetting(ImmutableSettings.Builder builder, Random random) {
        builder.put(ScriptModes.SCRIPT_SETTINGS_PREFIX + RandomPicks.randomFrom(random, ScriptService.ScriptType.values()), RandomPicks.randomFrom(random, new String[]{"on", "off", "sandbox"}));
    }

    private static void randomScriptContextSetting(ImmutableSettings.Builder builder, Random random) {
        builder.put(ScriptModes.SCRIPT_SETTINGS_PREFIX + RandomPicks.randomFrom(random, ScriptContext.values()), RandomPicks.randomFrom(random, new String[]{"on", "off", "sandbox"}));
    }
}
