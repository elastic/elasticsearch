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

package org.elasticsearch.common.logging;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Build;
import org.elasticsearch.Version;
import org.elasticsearch.common.SuppressLoggerChecks;
import org.elasticsearch.common.util.concurrent.ThreadContext;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Iterator;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A logger that logs deprecation notices.
 */
public class DeprecationLogger {

    private final Logger logger;

    /**
     * This is set once by the {@code Node} constructor, but it uses {@link CopyOnWriteArraySet} to ensure that tests can run in parallel.
     * <p>
     * Integration tests will create separate nodes within the same classloader, thus leading to a shared, {@code static} state.
     * In order for all tests to appropriately be handled, this must be able to remember <em>all</em> {@link ThreadContext}s that it is
     * given in a thread safe manner.
     * <p>
     * For actual usage, multiple nodes do not share the same JVM and therefore this will only be set once in practice.
     */
    private static final CopyOnWriteArraySet<ThreadContext> THREAD_CONTEXT = new CopyOnWriteArraySet<>();

    /**
     * Set the {@link ThreadContext} used to add deprecation headers to network responses.
     * <p>
     * This is expected to <em>only</em> be invoked by the {@code Node}'s constructor (therefore once outside of tests).
     *
     * @param threadContext The thread context owned by the {@code ThreadPool} (and implicitly a {@code Node})
     * @throws IllegalStateException if this {@code threadContext} has already been set
     */
    public static void setThreadContext(ThreadContext threadContext) {
        Objects.requireNonNull(threadContext, "Cannot register a null ThreadContext");

        // add returning false means it _did_ have it already
        if (THREAD_CONTEXT.add(threadContext) == false) {
            throw new IllegalStateException("Double-setting ThreadContext not allowed!");
        }
    }

    /**
     * Remove the {@link ThreadContext} used to add deprecation headers to network responses.
     * <p>
     * This is expected to <em>only</em> be invoked by the {@code Node}'s {@code close} method (therefore once outside of tests).
     *
     * @param threadContext The thread context owned by the {@code ThreadPool} (and implicitly a {@code Node})
     * @throws IllegalStateException if this {@code threadContext} is unknown (and presumably already unset before)
     */
    public static void removeThreadContext(ThreadContext threadContext) {
        assert threadContext != null;

        // remove returning false means it did not have it already
        if (THREAD_CONTEXT.remove(threadContext) == false) {
            throw new IllegalStateException("Removing unknown ThreadContext not allowed!");
        }
    }

    /**
     * Creates a new deprecation logger based on the parent logger. Automatically
     * prefixes the logger name with "deprecation", if it starts with "org.elasticsearch.",
     * it replaces "org.elasticsearch" with "org.elasticsearch.deprecation" to maintain
     * the "org.elasticsearch" namespace.
     */
    public DeprecationLogger(Logger parentLogger) {
        String name = parentLogger.getName();
        if (name.startsWith("org.elasticsearch")) {
            name = name.replace("org.elasticsearch.", "org.elasticsearch.deprecation.");
        } else {
            name = "deprecation." + name;
        }
        this.logger = LogManager.getLogger(name);
    }

    /**
     * Logs a deprecated message.
     */
    public void deprecated(String msg, Object... params) {
        deprecated(THREAD_CONTEXT, msg, params);
    }

    /*
     * RFC7234 specifies the warning format as warn-code <space> warn-agent <space> "warn-text" [<space> "warn-date"]. Here, warn-code is a
     * three-digit number with various standard warn codes specified. The warn code 299 is apt for our purposes as it represents a
     * miscellaneous persistent warning (can be presented to a human, or logged, and must not be removed by a cache). The warn-agent is an
     * arbitrary token; here we use the Elasticsearch version and build hash. The warn text must be quoted. The warn-date is an optional
     * quoted field that can be in a variety of specified date formats; here we use RFC 1123 format.
     */
    private static final String WARNING_FORMAT =
            String.format(
                    Locale.ROOT,
                    "299 Elasticsearch-%s%s-%s ",
                    Version.CURRENT.toString(),
                    Build.CURRENT.isSnapshot() ? "-SNAPSHOT" : "",
                    Build.CURRENT.shortHash()) +
                    "\"%s\" \"%s\"";

    private static final ZoneId GMT = ZoneId.of("GMT");

    public static Pattern WARNING_HEADER_PATTERN = Pattern.compile(
            "299 " + // warn code
                    "Elasticsearch-\\d+\\.\\d+\\.\\d+(?:-(?:alpha|beta|rc)\\d+)?(?:-SNAPSHOT)?-(?:[a-f0-9]{7}|Unknown) " + // warn agent
                    "\"((?:\t| |!|[\\x23-\\x5b]|[\\x5d-\\x7e]|[\\x80-\\xff]|\\\\|\")*)\" " + // quoted warning value, captured
                    // quoted RFC 1123 date format
                    "\"" + // opening quote
                    "(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun), " + // weekday
                    "\\d{2} " + // 2-digit day
                    "(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec) " + // month
                    "\\d{4} " + // 4-digit year
                    "\\d{2}:\\d{2}:\\d{2} " + // (two-digit hour):(two-digit minute):(two-digit second)
                    "GMT" + // GMT
                    "\""); // closing quote

    /**
     * Extracts the warning value from the value of a warning header that is formatted according to RFC 7234. That is, given a string
     * {@code 299 Elasticsearch-6.0.0 "warning value" "Sat, 25 Feb 2017 10:27:43 GMT"}, the return value of this method would be {@code
     * warning value}.
     *
     * @param s the value of a warning header formatted according to RFC 7234.
     * @return the extracted warning value
     */
    public static String extractWarningValueFromWarningHeader(final String s) {
        final Matcher matcher = WARNING_HEADER_PATTERN.matcher(s);
        final boolean matches = matcher.matches();
        assert matches;
        return matcher.group(1);
    }

    /**
     * Logs a deprecated message to the deprecation log, as well as to the local {@link ThreadContext}.
     *
     * @param threadContexts The node's {@link ThreadContext} (outside of concurrent tests, this should only ever have one context).
     * @param message The deprecation message.
     * @param params The parameters used to fill in the message, if any exist.
     */
    @SuppressLoggerChecks(reason = "safely delegates to logger")
    void deprecated(final Set<ThreadContext> threadContexts, final String message, final Object... params) {
        final Iterator<ThreadContext> iterator = threadContexts.iterator();

        if (iterator.hasNext()) {
            final String formattedMessage = LoggerMessageFormat.format(message, params);
            final String warningValue = formatWarning(formattedMessage);
            assert WARNING_HEADER_PATTERN.matcher(warningValue).matches();
            while (iterator.hasNext()) {
                try {
                    final ThreadContext next = iterator.next();
                    next.addResponseHeader("Warning", warningValue, DeprecationLogger::extractWarningValueFromWarningHeader);
                } catch (final IllegalStateException e) {
                    // ignored; it should be removed shortly
                }
            }
            logger.warn(formattedMessage);
        } else {
            logger.warn(message, params);
        }
    }

    /**
     * Format a warning string in the proper warning format by prepending a warn code, warn agent, wrapping the warning string in quotes,
     * and appending the RFC 1123 date.
     *
     * @param s the warning string to format
     * @return a warning value formatted according to RFC 7234
     */
    public static String formatWarning(final String s) {
        return String.format(Locale.ROOT, WARNING_FORMAT, escape(s), DateTimeFormatter.RFC_1123_DATE_TIME.format(ZonedDateTime.now(GMT)));
    }

    /**
     * Escape backslashes and quotes in the specified string.
     *
     * @param s the string to escape
     * @return the escaped string
     */
    public static String escape(String s) {
        return s.replaceAll("(\\\\|\")", "\\\\$1");
    }

}
