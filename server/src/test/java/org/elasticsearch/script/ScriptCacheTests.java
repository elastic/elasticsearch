/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.script;

import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;

import java.util.function.LongSupplier;

public class ScriptCacheTests extends ESTestCase {
    private static final LongSupplier time = () -> 1L;

    // even though circuit breaking is allowed to be configured per minute, we actually weigh this over five minutes
    // simply by multiplying by five, so even setting it to one, requires five compilations to break
    public void testCompilationCircuitBreaking() throws Exception {
        String context = randomFrom(ScriptModule.CORE_CONTEXTS.values().stream().filter(c -> c.compilationRateLimited).toList()).name;
        final TimeValue expire = ScriptService.SCRIPT_CACHE_EXPIRE_SETTING.getConcreteSettingForNamespace(context).get(Settings.EMPTY);
        final Integer size = ScriptService.SCRIPT_CACHE_SIZE_SETTING.getConcreteSettingForNamespace(context).get(Settings.EMPTY);
        Setting<ScriptCache.CompilationRate> rateSetting = ScriptService.SCRIPT_MAX_COMPILATIONS_RATE_SETTING
            .getConcreteSettingForNamespace(context);
        ScriptCache.CompilationRate rate = ScriptService.SCRIPT_MAX_COMPILATIONS_RATE_SETTING.getConcreteSettingForNamespace(context)
            .get(Settings.EMPTY);
        String rateSettingName = rateSetting.getKey();
        ScriptCache cache = new ScriptCache(
            size,
            expire,
            new ScriptCache.CompilationRate(1, TimeValue.timeValueMinutes(1)),
            rateSettingName,
            () -> 1L
        );
        cache.checkCompilationLimit(); // should pass
        expectThrows(CircuitBreakingException.class, cache::checkCompilationLimit);
        cache = new ScriptCache(size, expire, new ScriptCache.CompilationRate(2, TimeValue.timeValueMinutes(1)), rateSettingName, time);
        cache.checkCompilationLimit(); // should pass
        cache.checkCompilationLimit(); // should pass
        expectThrows(CircuitBreakingException.class, cache::checkCompilationLimit);
        int count = randomIntBetween(5, 50);
        cache = new ScriptCache(size, expire, new ScriptCache.CompilationRate(count, TimeValue.timeValueMinutes(1)), rateSettingName, time);
        for (int i = 0; i < count; i++) {
            cache.checkCompilationLimit(); // should pass
        }
        expectThrows(CircuitBreakingException.class, cache::checkCompilationLimit);
        cache = new ScriptCache(size, expire, new ScriptCache.CompilationRate(0, TimeValue.timeValueMinutes(1)), rateSettingName, time);
        expectThrows(CircuitBreakingException.class, cache::checkCompilationLimit);
        cache = new ScriptCache(
            size,
            expire,
            new ScriptCache.CompilationRate(Integer.MAX_VALUE, TimeValue.timeValueMinutes(1)),
            rateSettingName,
            () -> 1L
        );
        int largeLimit = randomIntBetween(1000, 10000);
        for (int i = 0; i < largeLimit; i++) {
            cache.checkCompilationLimit();
        }
    }

    public void testGeneralCompilationCircuitBreaking() throws Exception {
        final TimeValue expire = ScriptService.SCRIPT_GENERAL_CACHE_EXPIRE_SETTING.get(Settings.EMPTY);
        final Integer size = ScriptService.SCRIPT_GENERAL_CACHE_SIZE_SETTING.get(Settings.EMPTY);
        String settingName = ScriptService.SCRIPT_GENERAL_MAX_COMPILATIONS_RATE_SETTING.getKey();
        ScriptCache cache = new ScriptCache(
            size,
            expire,
            new ScriptCache.CompilationRate(1, TimeValue.timeValueMinutes(1)),
            settingName,
            () -> 1L
        );
        cache.checkCompilationLimit(); // should pass
        expectThrows(CircuitBreakingException.class, cache::checkCompilationLimit);
        cache = new ScriptCache(size, expire, new ScriptCache.CompilationRate(2, TimeValue.timeValueMinutes(1)), settingName, () -> 1L);
        cache.checkCompilationLimit(); // should pass
        cache.checkCompilationLimit(); // should pass
        expectThrows(CircuitBreakingException.class, cache::checkCompilationLimit);
        int count = randomIntBetween(5, 50);
        cache = new ScriptCache(size, expire, new ScriptCache.CompilationRate(count, TimeValue.timeValueMinutes(1)), settingName, () -> 1L);
        for (int i = 0; i < count; i++) {
            cache.checkCompilationLimit(); // should pass
        }
        expectThrows(CircuitBreakingException.class, cache::checkCompilationLimit);
        cache = new ScriptCache(size, expire, new ScriptCache.CompilationRate(0, TimeValue.timeValueMinutes(1)), settingName, () -> 1L);
        expectThrows(CircuitBreakingException.class, cache::checkCompilationLimit);
        cache = new ScriptCache(
            size,
            expire,
            new ScriptCache.CompilationRate(Integer.MAX_VALUE, TimeValue.timeValueMinutes(1)),
            settingName,
            () -> 1L
        );
        int largeLimit = randomIntBetween(1000, 10000);
        for (int i = 0; i < largeLimit; i++) {
            cache.checkCompilationLimit();
        }
    }

    public void testUnlimitedCompilationRate() {
        String context = randomFrom(ScriptModule.CORE_CONTEXTS.values().stream().filter(c -> c.compilationRateLimited).toList()).name;
        final Integer size = ScriptService.SCRIPT_CACHE_SIZE_SETTING.getConcreteSettingForNamespace(context).get(Settings.EMPTY);
        final TimeValue expire = ScriptService.SCRIPT_CACHE_EXPIRE_SETTING.getConcreteSettingForNamespace(context).get(Settings.EMPTY);
        String settingName = ScriptService.SCRIPT_MAX_COMPILATIONS_RATE_SETTING.getConcreteSettingForNamespace(context).getKey();
        ScriptCache cache = new ScriptCache(size, expire, ScriptCache.UNLIMITED_COMPILATION_RATE, settingName, time);
        ScriptCache.TokenBucketState initialState = cache.tokenBucketState.get();
        for (int i = 0; i < 3000; i++) {
            cache.checkCompilationLimit();
            ScriptCache.TokenBucketState currentState = cache.tokenBucketState.get();
            assertEquals(initialState.lastInlineCompileTime, currentState.lastInlineCompileTime);
            assertEquals(initialState.availableTokens, currentState.availableTokens, 0.0); // delta of 0.0 because it should never change
        }
    }

    public void testGeneralUnlimitedCompilationRate() {
        final Integer size = ScriptService.SCRIPT_GENERAL_CACHE_SIZE_SETTING.get(Settings.EMPTY);
        final TimeValue expire = ScriptService.SCRIPT_GENERAL_CACHE_EXPIRE_SETTING.get(Settings.EMPTY);
        String settingName = ScriptService.SCRIPT_GENERAL_MAX_COMPILATIONS_RATE_SETTING.getKey();
        ScriptCache cache = new ScriptCache(size, expire, ScriptCache.UNLIMITED_COMPILATION_RATE, settingName, () -> 1L);
        ScriptCache.TokenBucketState initialState = cache.tokenBucketState.get();
        for (int i = 0; i < 3000; i++) {
            cache.checkCompilationLimit();
            ScriptCache.TokenBucketState currentState = cache.tokenBucketState.get();
            assertEquals(initialState.lastInlineCompileTime, currentState.lastInlineCompileTime);
            assertEquals(initialState.availableTokens, currentState.availableTokens, 0.0); // delta of 0.0 because it should never change
        }
    }
}
