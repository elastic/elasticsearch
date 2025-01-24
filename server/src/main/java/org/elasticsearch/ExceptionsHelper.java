/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexFormatTooNewException;
import org.apache.lucene.index.IndexFormatTooOldException;
import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.Index;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.NoSeedNodeLeftException;
import org.elasticsearch.transport.NoSuchRemoteClusterException;
import org.elasticsearch.xcontent.XContentParseException;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public final class ExceptionsHelper {

    private static final Logger logger = LogManager.getLogger(ExceptionsHelper.class);

    public static RuntimeException convertToRuntime(Exception e) {
        if (e instanceof RuntimeException) {
            return (RuntimeException) e;
        }
        return new ElasticsearchException(e);
    }

    public static ElasticsearchException convertToElastic(Exception e) {
        if (e instanceof ElasticsearchException) {
            return (ElasticsearchException) e;
        }
        return new ElasticsearchException(e);
    }

    public static RestStatus status(Throwable t) {
        if (t != null) {
            if (t instanceof ElasticsearchException) {
                return ((ElasticsearchException) t).status();
            } else if (t instanceof IllegalArgumentException) {
                return RestStatus.BAD_REQUEST;
            } else if (t instanceof XContentParseException) {
                return RestStatus.BAD_REQUEST;
            } else if (t instanceof EsRejectedExecutionException) {
                return RestStatus.TOO_MANY_REQUESTS;
            }
        }
        return RestStatus.INTERNAL_SERVER_ERROR;
    }

    public static Throwable unwrapCause(Throwable t) {
        int counter = 0;
        Throwable result = t;
        while (result instanceof ElasticsearchWrapperException) {
            if (result.getCause() == null) {
                return result;
            }
            if (result.getCause() == result) {
                return result;
            }
            if (counter++ > 10) {
                // dear god, if we got more than 10 levels down, WTF? just bail
                logger.warn("Exception cause unwrapping ran for 10 levels...", t);
                return result;
            }
            result = result.getCause();
        }
        return result;
    }

    public static String stackTrace(Throwable e) {
        StringWriter stackTraceStringWriter = new StringWriter();
        PrintWriter printWriter = new PrintWriter(stackTraceStringWriter);
        e.printStackTrace(printWriter);
        return stackTraceStringWriter.toString();
    }

    /**
     * Constructs a limited and compressed stack trace string. Each exception printed as part of the full stack trace will have its printed
     * stack frames capped at the given trace depth. Stack traces that are longer than the given trace depth will summarize the count of the
     * remaining frames at the end of the trace. Each stack frame omits the module information and limits the package names to single
     * characters per part.
     * <br><br>
     * An example result when using a trace depth of 2 and one nested cause:
     * <pre><code>
     * o.e.s.GenericException: some generic exception!
     *   at o.e.s.SomeClass.method(SomeClass.java:100)
     *   at o.e.s.SomeOtherClass.earlierMethod(SomeOtherClass.java:24)
     *   ... 5 more
     * Caused by: o.e.s.GenericException: some other generic exception!
     *   at o.e.s.SomeClass.method(SomeClass.java:115)
     *   at o.e.s.SomeOtherClass.earlierMethod(SomeOtherClass.java:16)
     *   ... 12 more
     * </code></pre>
     *
     * @param e Throwable object to construct a printed stack trace for
     * @param traceDepth The maximum number of stack trace elements to display per exception referenced
     * @return A string containing a limited and compressed stack trace.
     */
    public static String limitedStackTrace(Throwable e, int traceDepth) {
        assert traceDepth >= 0 : "Cannot print stacktraces with negative trace depths";
        StringWriter stackTraceStringWriter = new StringWriter();
        PrintWriter printWriter = new PrintWriter(stackTraceStringWriter);
        printLimitedStackTrace(e, printWriter, traceDepth);
        return stackTraceStringWriter.toString();
    }

    /** Caption for labeling causative exception stack traces */
    private static final String CAUSE_CAPTION = "Caused by: ";
    /** Caption for labeling suppressed exception stack traces */
    private static final String SUPPRESSED_CAPTION = "Suppressed: ";

    private static void printLimitedStackTrace(Throwable e, PrintWriter s, int maxLines) {
        // Guard against malicious overrides of Throwable.equals by
        // using a Set with identity equality semantics.
        Set<Throwable> dejaVu = Collections.newSetFromMap(new IdentityHashMap<>());
        dejaVu.add(e);

        // Print our stack trace
        s.println(compressExceptionMessage(e));
        StackTraceElement[] trace = e.getStackTrace();
        int linesPrinted = 0;
        for (StackTraceElement traceElement : trace) {
            if (linesPrinted >= maxLines) {
                break;
            } else {
                s.println(compressStackTraceElement(new StringBuilder("\tat "), traceElement));
                linesPrinted++;
            }
        }
        if (trace.length > linesPrinted) {
            s.println("\t... " + (trace.length - linesPrinted) + " more");
        }

        // Print suppressed exceptions, if any
        for (Throwable se : e.getSuppressed()) {
            limitAndPrintEnclosedStackTrace(se, s, trace, SUPPRESSED_CAPTION, "\t", maxLines, dejaVu);
        }

        // Print cause, if any
        Throwable ourCause = e.getCause();
        if (ourCause != null) {
            limitAndPrintEnclosedStackTrace(ourCause, s, trace, CAUSE_CAPTION, "", maxLines, dejaVu);
        }
    }

    private static void limitAndPrintEnclosedStackTrace(
        Throwable e,
        PrintWriter s,
        StackTraceElement[] enclosingTrace,
        String caption,
        String prefix,
        int maxLines,
        Set<Throwable> dejaVu
    ) {
        if (dejaVu.contains(e)) {
            s.println(prefix + caption + "[CIRCULAR REFERENCE: " + compressExceptionMessage(e) + "]");
        } else {
            dejaVu.add(e);
            // Compute number of frames in common between this and enclosing trace
            StackTraceElement[] trace = e.getStackTrace();
            int m = trace.length - 1;
            int n = enclosingTrace.length - 1;
            while (m >= 0 && n >= 0 && trace[m].equals(enclosingTrace[n])) {
                m--;
                n--;
            }
            int framesInCommon = trace.length - 1 - m;

            // Instead of breaking out of the print loop below when it reaches the maximum
            // print lines, we simply cap how many frames we plan on printing here.
            int linesToPrint = m + 1;
            if (linesToPrint > maxLines) {
                // The print loop below is "<=" based instead of "<", so subtract
                // one from the max lines to convert a count value to an array index
                // value and avoid an off by one error.
                m = maxLines - 1;
                framesInCommon = trace.length - 1 - m;
            }

            // Print our stack trace
            s.println(prefix + caption + compressExceptionMessage(e));
            for (int i = 0; i <= m; i++) {
                s.println(compressStackTraceElement(new StringBuilder(prefix).append("\tat "), trace[i]));
            }
            if (framesInCommon != 0) {
                s.println(prefix + "\t... " + framesInCommon + " more");
            }

            // Print suppressed exceptions, if any
            for (Throwable se : e.getSuppressed()) {
                limitAndPrintEnclosedStackTrace(se, s, trace, SUPPRESSED_CAPTION, prefix + "\t", maxLines, dejaVu);
            }

            // Print cause, if any
            Throwable ourCause = e.getCause();
            if (ourCause != null) {
                limitAndPrintEnclosedStackTrace(ourCause, s, trace, CAUSE_CAPTION, prefix, maxLines, dejaVu);
            }
        }
    }

    private static String compressExceptionMessage(Throwable e) {
        StringBuilder msg = new StringBuilder();
        compressPackages(msg, e.getClass().getName());
        String message = e.getLocalizedMessage();
        if (message != null) {
            msg.append(": ").append(message);
        }
        return msg.toString();
    }

    private static StringBuilder compressStackTraceElement(StringBuilder s, final StackTraceElement stackTraceElement) {
        String declaringClass = stackTraceElement.getClassName();
        compressPackages(s, declaringClass);

        String methodName = stackTraceElement.getMethodName();
        s.append(".").append(methodName).append("(");

        if (stackTraceElement.isNativeMethod()) {
            s.append("Native Method)");
        } else {
            String fileName = stackTraceElement.getFileName();
            int lineNumber = stackTraceElement.getLineNumber();
            if (fileName != null && lineNumber >= 0) {
                s.append(fileName).append(":").append(lineNumber).append(")");
            } else if (fileName != null) {
                s.append(fileName).append(")");
            } else {
                s.append("Unknown Source)");
            }
        }
        return s;
    }

    // Visible for testing
    static void compressPackages(StringBuilder s, String className) {
        assert s != null : "s cannot be null";
        assert className != null : "className cannot be null";
        int finalDot = className.lastIndexOf('.');
        if (finalDot < 0) {
            s.append(className);
            return;
        }
        int lastPackageName = className.lastIndexOf('.', finalDot - 1);
        if (lastPackageName < 0) {
            if (finalDot >= 1) {
                s.append(className.charAt(0)).append('.');
            }
            s.append(className.substring(finalDot + 1));
            return;
        }
        boolean firstChar = true;
        char[] charArray = className.toCharArray();
        for (int idx = 0; idx <= lastPackageName + 1; idx++) {
            char c = charArray[idx];
            if (firstChar && '.' != c) {
                s.append(c).append('.');
            }
            firstChar = '.' == c;
        }
        s.append(className.substring(finalDot + 1));
    }

    public static String formatStackTrace(final StackTraceElement[] stackTrace) {
        return Arrays.stream(stackTrace).skip(1).map(e -> "\tat " + e).collect(Collectors.joining("\n"));
    }

    /**
     * Rethrows the first exception in the list and adds all remaining to the suppressed list.
     * If the given list is empty no exception is thrown
     *
     */
    public static <T extends Throwable> void rethrowAndSuppress(List<T> exceptions) throws T {
        T main = null;
        for (T ex : exceptions) {
            main = useOrSuppress(main, ex);
        }
        if (main != null) {
            throw main;
        }
    }

    /**
     * Throws a runtime exception with all given exceptions added as suppressed.
     * If the given list is empty no exception is thrown
     */
    public static <T extends Throwable> void maybeThrowRuntimeAndSuppress(List<T> exceptions) {
        T main = null;
        for (T ex : exceptions) {
            main = useOrSuppress(main, ex);
        }
        if (main != null) {
            throw new ElasticsearchException(main);
        }
    }

    public static <T extends Throwable> T useOrSuppress(T first, T second) {
        if (first == null) {
            return second;
        } else {
            first.addSuppressed(second);
        }
        return first;
    }

    private static final List<Class<? extends IOException>> CORRUPTION_EXCEPTIONS = List.of(
        CorruptIndexException.class,
        IndexFormatTooOldException.class,
        IndexFormatTooNewException.class
    );

    /**
     * Looks at the given Throwable's and its cause(s) as well as any suppressed exceptions on the Throwable as well as its causes
     * and returns the first corruption indicating exception (as defined by {@link #CORRUPTION_EXCEPTIONS}) it finds.
     * @param t Throwable
     * @return Corruption indicating exception if one is found, otherwise {@code null}
     */
    public static IOException unwrapCorruption(Throwable t) {
        return t == null ? null : ExceptionsHelper.<IOException>unwrapCausesAndSuppressed(t, cause -> {
            for (Class<?> clazz : CORRUPTION_EXCEPTIONS) {
                if (clazz.isInstance(cause)) {
                    return true;
                }
            }
            return false;
        }).orElse(null);
    }

    /**
     * Looks at the given Throwable and its cause(s) and returns the first Throwable that is of one of the given classes or {@code null}
     * if no matching Throwable is found. Unlike {@link #unwrapCorruption} this method does only check the given Throwable and its causes
     * but does not look at any suppressed exceptions.
     * @param t Throwable
     * @param clazzes Classes to look for
     * @return Matching Throwable if one is found, otherwise {@code null}
     */
    public static Throwable unwrap(Throwable t, Class<?>... clazzes) {
        if (t != null) {
            final Set<Throwable> seen = Collections.newSetFromMap(new IdentityHashMap<>());
            do {
                if (seen.add(t) == false) {
                    return null;
                }
                for (Class<?> clazz : clazzes) {
                    if (clazz.isInstance(t)) {
                        return t;
                    }
                }
            } while ((t = t.getCause()) != null);
        }
        return null;
    }

    /**
     * Throws the specified exception. If null if specified then <code>true</code> is returned.
     */
    public static boolean reThrowIfNotNull(@Nullable Throwable e) {
        if (e != null) {
            if (e instanceof RuntimeException) {
                throw (RuntimeException) e;
            } else {
                throw new RuntimeException(e);
            }
        }
        return true;
    }

    @SuppressWarnings("unchecked")
    public static <T extends Throwable> Optional<T> unwrapCausesAndSuppressed(Throwable cause, Predicate<Throwable> predicate) {
        if (predicate.test(cause)) {
            return Optional.of((T) cause);
        }

        final Queue<Throwable> queue = new LinkedList<>();
        queue.add(cause);
        final Set<Throwable> seen = Collections.newSetFromMap(new IdentityHashMap<>());
        while (queue.isEmpty() == false) {
            final Throwable current = queue.remove();
            if (seen.add(current) == false) {
                continue;
            }
            if (predicate.test(current)) {
                return Optional.of((T) current);
            }
            Collections.addAll(queue, current.getSuppressed());
            if (current.getCause() != null) {
                queue.add(current.getCause());
            }
        }
        return Optional.empty();
    }

    /**
     * Unwrap the specified throwable looking for any suppressed errors or errors as a root cause of the specified throwable.
     *
     * @param cause the root throwable
     * @return an optional error if one is found suppressed or a root cause in the tree rooted at the specified throwable
     */
    public static Optional<Error> maybeError(final Throwable cause) {
        return unwrapCausesAndSuppressed(cause, t -> t instanceof Error);
    }

    /**
     * If the specified cause is an unrecoverable error, this method will rethrow the cause on a separate thread so that it can not be
     * caught and bubbles up to the uncaught exception handler. Note that the cause tree is examined for any {@link Error}. See
     * {@link #maybeError(Throwable)} for the semantics.
     *
     * @param throwable the throwable to possibly throw on another thread
     */
    public static void maybeDieOnAnotherThread(final Throwable throwable) {
        ExceptionsHelper.maybeError(throwable).ifPresent(error -> {
            /*
             * Here be dragons. We want to rethrow this so that it bubbles up to the uncaught exception handler. Yet, sometimes the stack
             * contains statements that catch any throwable (e.g., Netty, and the JDK futures framework). This means that a rethrow here
             * will not bubble up to where we want it to. So, we fork a thread and throw the exception from there where we are sure the
             * stack does not contain statements that catch any throwable. We do not wrap the exception so as to not lose the original cause
             * during exit.
             */
            try {
                // try to log the current stack trace
                final String formatted = ExceptionsHelper.formatStackTrace(Thread.currentThread().getStackTrace());
                logger.error("fatal error {}: {}\n{}", error.getClass().getCanonicalName(), error.getMessage(), formatted);
            } finally {
                new Thread(() -> { throw error; }, "elasticsearch-error-rethrower").start();
            }
        });
    }

    /**
     * Deduplicate the failures by exception message and index.
     * @param failures array to deduplicate
     * @return deduplicated array; if failures is null or empty, it will be returned without modification
     */
    public static ShardOperationFailedException[] groupBy(ShardOperationFailedException[] failures) {
        if (failures == null || failures.length == 0) {
            return failures;
        }
        List<ShardOperationFailedException> uniqueFailures = new ArrayList<>();
        Set<GroupBy> reasons = new HashSet<>();
        for (ShardOperationFailedException failure : failures) {
            GroupBy reason = new GroupBy(failure);
            if (reasons.contains(reason) == false) {
                reasons.add(reason);
                uniqueFailures.add(failure);
            }
        }
        return uniqueFailures.toArray(new ShardOperationFailedException[0]);
    }

    /**
     * Utility method useful for determining whether to log an Exception or perhaps
     * avoid logging a stacktrace if the caller/logger is not interested in these
     * types of node/shard issues.
     *
     * @param t Throwable to inspect
     * @return true if the Throwable is an instance of an Exception that indicates
     *         that either a Node or shard is unavailable/disconnected.
     */
    public static boolean isNodeOrShardUnavailableTypeException(Throwable t) {
        return (t instanceof org.elasticsearch.action.NoShardAvailableActionException
            || t instanceof org.elasticsearch.action.UnavailableShardsException
            || t instanceof org.elasticsearch.node.NodeClosedException
            || t instanceof org.elasticsearch.transport.NodeDisconnectedException
            || t instanceof org.elasticsearch.discovery.MasterNotDiscoveredException
            || t instanceof org.elasticsearch.transport.NodeNotConnectedException
            || t instanceof org.elasticsearch.cluster.block.ClusterBlockException);
    }

    /**
     * Checks the exception against a known list of exceptions that indicate a remote cluster
     * cannot be connected to.
     *
     * @param e Exception to inspect
     * @return true if the Exception is known to indicate that a remote cluster
     *         is unavailable (cannot be connected to by the transport layer)
     */
    public static boolean isRemoteUnavailableException(Exception e) {
        Throwable unwrap = unwrap(e, ConnectTransportException.class, NoSuchRemoteClusterException.class, NoSeedNodeLeftException.class);
        if (unwrap != null) {
            return true;
        }
        Throwable ill = unwrap(e, IllegalStateException.class, IllegalArgumentException.class);
        if (ill != null && (ill.getMessage().contains("Unable to open any connections") || ill.getMessage().contains("unknown host"))) {
            return true;
        }
        // doesn't look like any of the known remote exceptions
        return false;
    }

    private static class GroupBy {
        final String reason;
        final String index;
        final Class<? extends Throwable> causeType;

        GroupBy(ShardOperationFailedException failure) {
            Throwable cause = failure.getCause();
            // the index name from the failure contains the cluster alias when using CCS. Ideally failures should be grouped by
            // index name and cluster alias. That's why the failure index name has the precedence over the one coming from the cause,
            // which does not include the cluster alias.
            String indexName = failure.index();
            if (indexName == null) {
                if (cause instanceof ElasticsearchException) {
                    final Index index = ((ElasticsearchException) cause).getIndex();
                    if (index != null) {
                        indexName = index.getName();
                    }
                }
            }
            this.index = indexName;
            this.reason = cause.getMessage();
            this.causeType = cause.getClass();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            GroupBy groupBy = (GroupBy) o;
            return Objects.equals(reason, groupBy.reason)
                && Objects.equals(index, groupBy.index)
                && Objects.equals(causeType, groupBy.causeType);
        }

        @Override
        public int hashCode() {
            return Objects.hash(reason, index, causeType);
        }
    }
}
