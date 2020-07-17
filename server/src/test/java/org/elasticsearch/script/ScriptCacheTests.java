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
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.ESTestCase;

import java.util.stream.Collectors;

public class ScriptCacheTests extends ESTestCase {
    // even though circuit breaking is allowed to be configured per minute, we actually weigh this over five minutes
    // simply by multiplying by five, so even setting it to one, requires five compilations to break
    public void testCompilationCircuitBreaking() throws Exception {
        String context = randomFrom(
            ScriptModule.CORE_CONTEXTS.values().stream().filter(
                c -> c.maxCompilationRateDefault.equals(ScriptCache.UNLIMITED_COMPILATION_RATE) == false
            ).collect(Collectors.toList())
        ).name;
        final TimeValue expire = ScriptService.SCRIPT_CACHE_EXPIRE_SETTING.getConcreteSettingForNamespace(context).get(Settings.EMPTY);
        final Integer size = ScriptService.SCRIPT_CACHE_SIZE_SETTING.getConcreteSettingForNamespace(context).get(Settings.EMPTY);
        Setting<ScriptCache.CompilationRate> rateSetting =
            ScriptService.SCRIPT_MAX_COMPILATIONS_RATE_SETTING.getConcreteSettingForNamespace(context);
        ScriptCache.CompilationRate rate =
            ScriptService.SCRIPT_MAX_COMPILATIONS_RATE_SETTING.getConcreteSettingForNamespace(context).get(Settings.EMPTY);
        String rateSettingName = rateSetting.getKey();
        ScriptCache cache = new ScriptCache(size, expire,
            new ScriptCache.CompilationRate(1, TimeValue.timeValueMinutes(1)), rateSettingName);
        cache.checkCompilationLimit(); // should pass
        expectThrows(CircuitBreakingException.class, cache::checkCompilationLimit);
        cache = new ScriptCache(size, expire, new ScriptCache.CompilationRate(2, TimeValue.timeValueMinutes(1)), rateSettingName);
        cache.checkCompilationLimit(); // should pass
        cache.checkCompilationLimit(); // should pass
        expectThrows(CircuitBreakingException.class, cache::checkCompilationLimit);
        int count = randomIntBetween(5, 50);
        cache = new ScriptCache(size, expire, new ScriptCache.CompilationRate(count, TimeValue.timeValueMinutes(1)), rateSettingName);
        for (int i = 0; i < count; i++) {
            cache.checkCompilationLimit(); // should pass
        }
        expectThrows(CircuitBreakingException.class, cache::checkCompilationLimit);
        cache = new ScriptCache(size, expire, new ScriptCache.CompilationRate(0, TimeValue.timeValueMinutes(1)), rateSettingName);
        expectThrows(CircuitBreakingException.class, cache::checkCompilationLimit);
        cache = new ScriptCache(size, expire,
                                new ScriptCache.CompilationRate(Integer.MAX_VALUE, TimeValue.timeValueMinutes(1)), rateSettingName);
        int largeLimit = randomIntBetween(1000, 10000);
        for (int i = 0; i < largeLimit; i++) {
            cache.checkCompilationLimit();
        }
    }

    public void testUnlimitedCompilationRate() {
        String context = randomFrom(
            ScriptModule.CORE_CONTEXTS.values().stream().filter(
                c -> c.maxCompilationRateDefault.equals(ScriptCache.UNLIMITED_COMPILATION_RATE) == false
            ).collect(Collectors.toList())
        ).name;
        final Integer size = ScriptService.SCRIPT_CACHE_SIZE_SETTING.getConcreteSettingForNamespace(context).get(Settings.EMPTY);
        final TimeValue expire = ScriptService.SCRIPT_CACHE_EXPIRE_SETTING.getConcreteSettingForNamespace(context).get(Settings.EMPTY);
        String settingName = ScriptService.SCRIPT_MAX_COMPILATIONS_RATE_SETTING.getConcreteSettingForNamespace(context).getKey();
        ScriptCache cache = new ScriptCache(size, expire, ScriptCache.UNLIMITED_COMPILATION_RATE, settingName);
        ScriptCache.TokenBucketState initialState = cache.tokenBucketState.get();
        for(int i=0; i < 3000; i++) {
            cache.checkCompilationLimit();
            ScriptCache.TokenBucketState currentState = cache.tokenBucketState.get();
            assertEquals(initialState.lastInlineCompileTime, currentState.lastInlineCompileTime);
            assertEquals(initialState.availableTokens, currentState.availableTokens, 0.0); // delta of 0.0 because it should never change
        }
    }
}
