/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.runtimefields;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.common.util.LazyInitializable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.dissect.DissectParser;
import org.elasticsearch.grok.Grok;
import org.elasticsearch.grok.GrokBuiltinPatterns;
import org.elasticsearch.grok.MatcherWatchdog;
import org.elasticsearch.threadpool.ThreadPool;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.regex.Pattern;

/**
 * Extracts named groups from grok and dissect. Think of it kind of like
 * {@link Pattern} but for grok and dissect.
 */
public interface NamedGroupExtractor {
    /**
     * Returns a {@link Map} containing all named capture groups if the
     * string matches or {@code null} if it doesn't.
     */
    Map<String, ?> extract(String in);

    /**
     * Create a {@link NamedGroupExtractor} that runs {@link DissectParser}
     * with the default {@code appendSeparator}.
     */
    static NamedGroupExtractor dissect(String pattern) {
        return dissect(pattern, null);
    }

    /**
     * Create a {@link NamedGroupExtractor} that runs {@link DissectParser}.
     */
    static NamedGroupExtractor dissect(String pattern, String appendSeparator) {
        DissectParser dissect = new DissectParser(pattern, appendSeparator);
        return new NamedGroupExtractor() {
            @Override
            public Map<String, ?> extract(String in) {
                return dissect.parse(in);
            }
        };
    }

    /**
     * Builds {@link NamedGroupExtractor}s from grok patterns.
     */
    class GrokHelper {
        private final SetOnce<ThreadPool> threadPoolContainer = new SetOnce<>();
        private final Supplier<MatcherWatchdog> watchdogSupplier;

        public GrokHelper(TimeValue interval, TimeValue maxExecutionTime) {
            this.watchdogSupplier = new LazyInitializable<MatcherWatchdog, RuntimeException>(() -> {
                ThreadPool threadPool = threadPoolContainer.get();
                if (threadPool == null) {
                    throw new IllegalStateException("missing call to finishInitializing");
                }
                return MatcherWatchdog.newInstance(
                    interval.millis(),
                    maxExecutionTime.millis(),
                    threadPool::relativeTimeInMillis,
                    (delay, command) -> threadPool.schedule(command, TimeValue.timeValueMillis(delay), ThreadPool.Names.GENERIC)
                );
            })::getOrCompute;
        }

        /**
         * Finish initializing. This is split from the ctor because we need an
         * instance of this class to feed into painless before the
         * {@link ThreadPool} is ready.
         */
        public void finishInitializing(ThreadPool threadPool) {
            threadPoolContainer.set(threadPool);
        }

        public NamedGroupExtractor grok(String pattern) {
            MatcherWatchdog watchdog = watchdogSupplier.get();
            /*
             * Build the grok pattern in a PrivilegedAction so it can load
             * things from the classpath.
             */
            Grok grok = AccessController.doPrivileged(new PrivilegedAction<Grok>() {
                @Override
                public Grok run() {
                    try {
                        // Try to collect warnings up front and refuse to compile the expression if there are any
                        List<String> warnings = new ArrayList<>();
                        new Grok(GrokBuiltinPatterns.legacyPatterns(), pattern, watchdog, warnings::add).match("__nomatch__");
                        if (false == warnings.isEmpty()) {
                            throw new IllegalArgumentException("emitted warnings: " + warnings);
                        }

                        return new Grok(GrokBuiltinPatterns.legacyPatterns(), pattern, watchdog, w -> {
                            throw new IllegalArgumentException("grok [" + pattern + "] emitted a warning: " + w);
                        });
                    } catch (RuntimeException e) {
                        throw new IllegalArgumentException("error compiling grok pattern [" + pattern + "]: " + e.getMessage(), e);
                    }
                }
            });
            return new NamedGroupExtractor() {
                @Override
                public Map<String, ?> extract(String in) {
                    return grok.captures(in);
                }
            };
        }
    }
}
