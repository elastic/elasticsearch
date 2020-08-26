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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Script cache and compilation rate limiter.
 */
public class ScriptCache {

    private static final Logger logger = LogManager.getLogger(ScriptService.class);

    static final CompilationRate UNLIMITED_COMPILATION_RATE = new CompilationRate(0, TimeValue.ZERO);

    private final Cache<CacheKey, Object> cache;
    private final ScriptMetrics scriptMetrics;
    final AtomicReference<TokenBucketState> tokenBucketState;

    // Cache settings or derived from settings
    final int cacheSize;
    final TimeValue cacheExpire;
    final CompilationRate rate;
    private final double compilesAllowedPerNano;
    private final String contextRateSetting;

    ScriptCache(
            int cacheMaxSize,
            TimeValue cacheExpire,
            CompilationRate maxCompilationRate,
            String contextRateSetting
    ) {
        this.cacheSize = cacheMaxSize;
        this.cacheExpire = cacheExpire;
        this.contextRateSetting = contextRateSetting;

        CacheBuilder<CacheKey, Object> cacheBuilder = CacheBuilder.builder();
        if (this.cacheSize >= 0) {
            cacheBuilder.setMaximumWeight(this.cacheSize);
        }

        if (this.cacheExpire.getNanos() != 0) {
            cacheBuilder.setExpireAfterAccess(this.cacheExpire);
        }

        logger.debug("using script cache with max_size [{}], expire [{}]", this.cacheSize, this.cacheExpire);
        this.cache = cacheBuilder.removalListener(new ScriptCacheRemovalListener()).build();

        this.rate = maxCompilationRate;
        this.compilesAllowedPerNano = ((double) rate.count) / rate.time.nanos();
        this.scriptMetrics = new ScriptMetrics();
        this.tokenBucketState = new AtomicReference<TokenBucketState>(new TokenBucketState(this.rate.count));
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

        // Relying on computeIfAbsent to avoid multiple threads from compiling the same script
        try {
            return context.factoryClazz.cast(cache.computeIfAbsent(cacheKey, key -> {
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
                Object compiledScript = scriptEngine.compile(id, idOrCode, context, options);
                // Since the cache key is the script content itself we don't need to
                // invalidate/check the cache if an indexed script changes.
                scriptMetrics.onCompilation();
                return compiledScript;
            }));
        } catch (ExecutionException executionException) {
            Throwable cause = executionException.getCause();
            if (cause instanceof ScriptException) {
                throw (ScriptException) cause;
            } else if (cause instanceof Exception) {
                throw new GeneralScriptException("Failed to compile " + type + " script [" + id + "] using lang [" + lang + "]", cause);
            } else {
                rethrow(cause);
                throw new AssertionError(cause);
            }
        }
    }

    /** Hack to rethrow unknown Exceptions from compile: */
    @SuppressWarnings("unchecked")
    static <T extends Throwable> void rethrow(Throwable t) throws T {
        throw (T) t;
    }

    public ScriptContextStats stats(String context) {
        return scriptMetrics.stats(context);
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
        if (rate.equals(UNLIMITED_COMPILATION_RATE)) {
            return;
        }

        TokenBucketState tokenBucketState = this.tokenBucketState.updateAndGet(current -> {
            long now = System.nanoTime();
            long timePassed = now - current.lastInlineCompileTime;
            double scriptsPerTimeWindow = current.availableTokens + (timePassed) * compilesAllowedPerNano;

            // It's been over the time limit anyway, readjust the bucket to be level
            if (scriptsPerTimeWindow > rate.count) {
                scriptsPerTimeWindow = rate.count;
            }

            // If there is enough tokens in the bucket, allow the request and decrease the tokens by 1
            if (scriptsPerTimeWindow >= 1) {
                scriptsPerTimeWindow -= 1.0;
                return new TokenBucketState(now, scriptsPerTimeWindow, true);
            } else {
                return new TokenBucketState(now, scriptsPerTimeWindow, false);
            }
        });

        if(!tokenBucketState.tokenSuccessfullyTaken) {
            scriptMetrics.onCompilationLimit();
            // Otherwise reject the request
            throw new CircuitBreakingException("[script] Too many dynamic script compilations within, max: [" +
                rate + "]; please use indexed, or scripts with parameters instead; " +
                "this limit can be changed by the [" + contextRateSetting + "] setting",
                CircuitBreaker.Durability.TRANSIENT);
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

    static class TokenBucketState {
        public final long lastInlineCompileTime;
        public final double availableTokens;
        public final boolean tokenSuccessfullyTaken;

        private TokenBucketState(double availableTokens) {
            this(System.nanoTime(), availableTokens, false);
        }

        private TokenBucketState(long lastInlineCompileTime, double availableTokens, boolean tokenSuccessfullyTaken) {
            this.lastInlineCompileTime = lastInlineCompileTime;
            this.availableTokens = availableTokens;
            this.tokenSuccessfullyTaken = tokenSuccessfullyTaken;
        }
    }

    public static class CompilationRate {
        public final int count;
        public final TimeValue time;
        private final String source;

        public CompilationRate(Integer count, TimeValue time) {
            this.count = count;
            this.time = time;
            this.source = null;
        }

        public CompilationRate(Tuple<Integer,TimeValue> rate) {
            this(rate.v1(), rate.v2());
        }

        /**
         * Parses a string as a non-negative int count and a {@code TimeValue} as arguments split by a slash
         */
        public CompilationRate(String value) {
            if (value.contains("/") == false || value.startsWith("/") || value.endsWith("/")) {
                throw new IllegalArgumentException("parameter must contain a positive integer and a timevalue, i.e. 10/1m, but was [" +
                    value + "]");
            }
            int idx = value.indexOf("/");
            String count = value.substring(0, idx);
            String time = value.substring(idx + 1);
            try {
                int rate = Integer.parseInt(count);
                if (rate < 0) {
                    throw new IllegalArgumentException("rate [" + rate + "] must be positive");
                }
                TimeValue timeValue = TimeValue.parseTimeValue(time, "script.max_compilations_rate");
                if (timeValue.nanos() <= 0) {
                    throw new IllegalArgumentException("time value [" + time + "] must be positive");
                }
                // protect against a too hard to check limit, like less than a minute
                if (timeValue.seconds() < 60) {
                    throw new IllegalArgumentException("time value [" + time + "] must be at least on a one minute resolution");
                }
                this.count = rate;
                this.time = timeValue;
                this.source = value;
            } catch (NumberFormatException e) {
                // the number format exception message is so confusing, that it makes more sense to wrap it with a useful one
                throw new IllegalArgumentException("could not parse [" + count + "] as integer in value [" + value + "]", e);
            }
        }

        public Tuple<Integer,TimeValue> asTuple() {
            return new Tuple<>(this.count, this.time);
        }

        @Override
        public String toString() {
            return source != null ? source : count + "/" + time.toHumanReadableString(0);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            CompilationRate that = (CompilationRate) o;
            return count == that.count &&
                Objects.equals(time, that.time);
        }

        @Override
        public int hashCode() {
            return Objects.hash(count, time);
        }
    }
}
