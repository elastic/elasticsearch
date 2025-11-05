/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
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
     * Extracts named groups from the input string using the configured pattern.
     * <p>
     * Returns a map containing all named capture groups if the string matches the pattern,
     * or {@code null} if it doesn't match.
     * </p>
     *
     * @param in the input string to extract groups from
     * @return a map of named groups to their matched values, or {@code null} if no match
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * NamedGroupExtractor extractor = NamedGroupExtractor.dissect("%{name} is %{age}");
     * Map<String, ?> groups = extractor.extract("John is 30");
     * // Returns: {name=John, age=30}
     *
     * Map<String, ?> noMatch = extractor.extract("invalid format");
     * // Returns: null
     * }</pre>
     */
    Map<String, ?> extract(String in);

    /**
     * Creates a {@link NamedGroupExtractor} that runs {@link DissectParser} with the default append separator.
     * <p>
     * Dissect parsing extracts structured fields from text using a pattern-based approach
     * that is simpler and faster than regular expressions or grok.
     * </p>
     *
     * @param pattern the dissect pattern to use for extraction
     * @return a named group extractor using the specified dissect pattern
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * NamedGroupExtractor extractor = NamedGroupExtractor.dissect("%{name} is %{age} years old");
     * Map<String, ?> groups = extractor.extract("Alice is 25 years old");
     * // Returns: {name=Alice, age=25}
     * }</pre>
     */
    static NamedGroupExtractor dissect(String pattern) {
        return dissect(pattern, null);
    }

    /**
     * Creates a {@link NamedGroupExtractor} that runs {@link DissectParser} with a custom append separator.
     * <p>
     * Dissect parsing extracts structured fields from text. The append separator is used when
     * multiple values are captured for the same key and need to be concatenated.
     * </p>
     *
     * @param pattern the dissect pattern to use for extraction
     * @param appendSeparator the separator to use when appending multiple values to the same key, or null for default
     * @return a named group extractor using the specified dissect pattern and append separator
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * NamedGroupExtractor extractor = NamedGroupExtractor.dissect("%{+name} %{+name}", " ");
     * Map<String, ?> groups = extractor.extract("John Doe");
     * // Returns: {name=John Doe}
     * }</pre>
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
     * Helper class for building {@link NamedGroupExtractor}s from grok patterns.
     * <p>
     * This class provides factory methods for creating grok-based extractors with watchdog
     * protection against long-running or infinite loop pattern matching operations.
     * </p>
     */
    class GrokHelper {
        private final SetOnce<ThreadPool> threadPoolContainer = new SetOnce<>();
        private final Supplier<MatcherWatchdog> watchdogSupplier;

        /**
         * Constructs a new GrokHelper with watchdog configuration.
         * <p>
         * The watchdog monitors grok pattern matching operations and interrupts them if they
         * exceed the configured maximum execution time. The interval determines how frequently
         * the watchdog checks for timeouts.
         * </p>
         *
         * @param interval the watchdog check interval
         * @param maxExecutionTime the maximum allowed execution time for a single match operation
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * GrokHelper helper = new GrokHelper(
         *     TimeValue.timeValueSeconds(1),
         *     TimeValue.timeValueSeconds(1)
         * );
         * helper.finishInitializing(threadPool);
         * NamedGroupExtractor extractor = helper.grok("%{WORD:name}");
         * }</pre>
         */
        public GrokHelper(TimeValue interval, TimeValue maxExecutionTime) {
            this.watchdogSupplier = new LazyInitializable<MatcherWatchdog, RuntimeException>(() -> {
                ThreadPool threadPool = threadPoolContainer.get();
                if (threadPool == null) {
                    throw new IllegalStateException("missing call to finishInitializing");
                }
                return MatcherWatchdog.newInstance(
                    interval.millis(),
                    maxExecutionTime.millis(),
                    threadPool.relativeTimeInMillisSupplier(),
                    (delay, command) -> threadPool.schedule(command, TimeValue.timeValueMillis(delay), threadPool.generic())
                );
            })::getOrCompute;
        }

        /**
         * Completes initialization by providing the thread pool for watchdog scheduling.
         * <p>
         * This method is separate from the constructor because an instance of GrokHelper
         * needs to be available to Painless before the {@link ThreadPool} is ready during
         * plugin initialization.
         * </p>
         *
         * @param threadPool the thread pool to use for watchdog scheduling
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * GrokHelper helper = new GrokHelper(interval, maxTime);
         * // ... pass helper to painless extension ...
         * helper.finishInitializing(threadPool);  // Complete initialization later
         * }</pre>
         */
        public void finishInitializing(ThreadPool threadPool) {
            threadPoolContainer.set(threadPool);
        }

        /**
         * Creates a {@link NamedGroupExtractor} from a grok pattern.
         * <p>
         * This method compiles the grok pattern and returns an extractor that can match strings
         * and extract named capture groups. The compilation validates the pattern upfront and
         * will throw an exception if the pattern emits any warnings or errors.
         * </p>
         * <p>
         * Grok patterns support built-in patterns from {@link GrokBuiltinPatterns} and custom
         * named capture groups. The watchdog protects against runaway pattern matching.
         * </p>
         *
         * @param pattern the grok pattern to compile
         * @return a named group extractor using the compiled grok pattern
         * @throws IllegalArgumentException if the pattern is invalid or emits warnings
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * GrokHelper helper = plugin.grokHelper();
         * NamedGroupExtractor extractor = helper.grok("%{WORD:name} %{INT:age}");
         * Map<String, ?> groups = extractor.extract("Alice 30");
         * // Returns: {name=Alice, age=30}
         *
         * // Using built-in patterns:
         * extractor = helper.grok("%{IP:client} %{WORD:method} %{URIPATHPARAM:request}");
         * groups = extractor.extract("127.0.0.1 GET /index.html");
         * }</pre>
         */
        public NamedGroupExtractor grok(String pattern) {
            MatcherWatchdog watchdog = watchdogSupplier.get();
            /*
             * Build the grok pattern in a PrivilegedAction so it can load
             * things from the classpath.
             */
            Grok grok;
            try {
                // Try to collect warnings up front and refuse to compile the expression if there are any
                List<String> warnings = new ArrayList<>();
                new Grok(GrokBuiltinPatterns.legacyPatterns(), pattern, watchdog, warnings::add).match("__nomatch__");
                if (false == warnings.isEmpty()) {
                    throw new IllegalArgumentException("emitted warnings: " + warnings);
                }

                grok = new Grok(GrokBuiltinPatterns.legacyPatterns(), pattern, watchdog, w -> {
                    throw new IllegalArgumentException("grok [" + pattern + "] emitted a warning: " + w);
                });
            } catch (RuntimeException e) {
                throw new IllegalArgumentException("error compiling grok pattern [" + pattern + "]: " + e.getMessage(), e);
            }
            return new NamedGroupExtractor() {
                @Override
                public Map<String, ?> extract(String in) {
                    return grok.captures(in);
                }
            };
        }
    }
}
