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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.common.cache.RemovalListener;
import org.elasticsearch.common.cache.RemovalNotification;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.unit.TimeValue;

import java.util.Map;
import java.util.Objects;

/**
 * Script cache and compilation rate limiter.
 */
public class ScriptCache {

    private static final Logger logger = LogManager.getLogger(ScriptService.class);

    private Cache<CacheKey, Object> cache;
    private final ScriptMetrics scriptMetrics = new ScriptMetrics();

    private final Object lock = new Object();

    private Tuple<Integer, TimeValue> rate;
    private long lastInlineCompileTime;
    private double scriptsPerTimeWindow;
    private double compilesAllowedPerNano;

    // Cache settings
    private int cacheSize;
    private TimeValue cacheExpire;

    public ScriptCache(
            int cacheMaxSize,
            TimeValue cacheExpire,
            Tuple<Integer, TimeValue> maxCompilationRate
    ) {
        CacheBuilder<CacheKey, Object> cacheBuilder = CacheBuilder.builder();
        if (cacheMaxSize >= 0) {
            cacheBuilder.setMaximumWeight(cacheMaxSize);
        }

        if (cacheExpire.getNanos() != 0) {
            cacheBuilder.setExpireAfterAccess(cacheExpire);
        }

        logger.debug("using script cache with max_size [{}], expire [{}]", cacheMaxSize, cacheExpire);
        this.cache = cacheBuilder.removalListener(new ScriptCacheRemovalListener()).build();

        this.lastInlineCompileTime = System.nanoTime();

        this.cacheSize = cacheMaxSize;
        this.cacheExpire = cacheExpire;
        this.setMaxCompilationRate(maxCompilationRate);
    }

    private Cache<CacheKey,Object> buildCache() {
        CacheBuilder<CacheKey, Object> cacheBuilder = CacheBuilder.builder();
        if (cacheSize >= 0) {
            cacheBuilder.setMaximumWeight(cacheSize);
        }
        if (cacheExpire.getNanos() != 0) {
            cacheBuilder.setExpireAfterAccess(cacheExpire);
        }
        return cacheBuilder.removalListener(new ScriptCacheRemovalListener()).build();
    }

    <FactoryType> FactoryType compile(
        ScriptContext<FactoryType> context,
        ScriptEngine scriptEngine,
        String id,
        String idOrCode,
        ScriptType type,
        Map<String, String> options
    ) {
        String lang = scriptEngine.getType();
        CacheKey cacheKey = new CacheKey(lang, idOrCode, context.name, options);
        Object compiledScript = cache.get(cacheKey);

        if (compiledScript != null) {
            return context.factoryClazz.cast(compiledScript);
        }

        // Synchronize so we don't compile scripts many times during multiple shards all compiling a script
        synchronized (lock) {
            // Retrieve it again in case it has been put by a different thread
            compiledScript = cache.get(cacheKey);

            if (compiledScript == null) {
                try {
                    // Either an un-cached inline script or indexed script
                    // If the script type is inline the name will be the same as the code for identification in exceptions
                    // but give the script engine the chance to be better, give it separate name + source code
                    // for the inline case, then its anonymous: null.
                    if (logger.isTraceEnabled()) {
                        logger.trace("context [{}]: compiling script, type: [{}], lang: [{}], options: [{}]", context.name, type,
                            lang, options);
                    }
                    // Check whether too many compilations have happened
                    checkCompilationLimit();
                    compiledScript = scriptEngine.compile(id, idOrCode, context, options);
                } catch (ScriptException good) {
                    // TODO: remove this try-catch completely, when all script engines have good exceptions!
                    throw good; // its already good
                } catch (Exception exception) {
                    throw new GeneralScriptException("Failed to compile " + type + " script [" + id + "] using lang [" + lang + "]",
                            exception);
                }

                // Since the cache key is the script content itself we don't need to
                // invalidate/check the cache if an indexed script changes.
                scriptMetrics.onCompilation();
                cache.put(cacheKey, compiledScript);
            }

        }

        return context.factoryClazz.cast(compiledScript);
    }

    public ScriptStats stats() {
        return scriptMetrics.stats();
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
    void checkCompilationLimit() {
        if (rate.v1() == 0 && rate.v2().getNanos() == 0) {
            // unlimited
            return;
        }

        long now = System.nanoTime();
        long timePassed = now - lastInlineCompileTime;
        lastInlineCompileTime = now;

        scriptsPerTimeWindow += (timePassed) * compilesAllowedPerNano;

        // It's been over the time limit anyway, readjust the bucket to be level
        if (scriptsPerTimeWindow > rate.v1()) {
            scriptsPerTimeWindow = rate.v1();
        }

        // If there is enough tokens in the bucket, allow the request and decrease the tokens by 1
        if (scriptsPerTimeWindow >= 1) {
            scriptsPerTimeWindow -= 1.0;
        } else {
            scriptMetrics.onCompilationLimit();
            // Otherwise reject the request
            throw new CircuitBreakingException("[script] Too many dynamic script compilations within, max: [" +
                rate.v1() + "/" + rate.v2() +"]; please use indexed, or scripts with parameters instead; " +
                "this limit can be changed by the [script.max_compilations_rate] setting",
                CircuitBreaker.Durability.TRANSIENT);
        }
    }

    /**
     * This configures the maximum script compilations per five minute window.
     *
     * @param newRate the new expected maximum number of compilations per five minute window
     */
    void setMaxCompilationRate(Tuple<Integer, TimeValue> newRate) {
        synchronized (lock) {
            this.rate = newRate;
            // Reset the counter to allow new compilations
            this.scriptsPerTimeWindow = rate.v1();
            this.compilesAllowedPerNano = ((double) rate.v1()) / newRate.v2().nanos();

            this.cache = buildCache();
        }
    }

    /**
     * A small listener for the script cache that calls each
     * {@code ScriptEngine}'s {@code scriptRemoved} method when the
     * script has been removed from the cache
     */
    private class ScriptCacheRemovalListener implements RemovalListener<CacheKey, Object> {
        @Override
        public void onRemoval(RemovalNotification<CacheKey, Object> notification) {
            if (logger.isDebugEnabled()) {
                logger.debug(
                    "removed [{}] from cache, reason: [{}]",
                    notification.getValue(),
                    notification.getRemovalReason()
                );
            }
            scriptMetrics.onCacheEviction();
        }
    }

    private static final class CacheKey {
        final String lang;
        final String idOrCode;
        final String context;
        final Map<String, String> options;

        private CacheKey(String lang, String idOrCode, String context, Map<String, String> options) {
            this.lang = lang;
            this.idOrCode = idOrCode;
            this.context = context;
            this.options = options;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            CacheKey cacheKey = (CacheKey) o;
            return Objects.equals(lang, cacheKey.lang) &&
                Objects.equals(idOrCode, cacheKey.idOrCode) &&
                Objects.equals(context, cacheKey.context) &&
                Objects.equals(options, cacheKey.options);
        }

        @Override
        public int hashCode() {
            return Objects.hash(lang, idOrCode, context, options);
        }
    }
}
