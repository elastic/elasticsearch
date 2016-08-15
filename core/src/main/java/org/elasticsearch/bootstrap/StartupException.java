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

package org.elasticsearch.bootstrap;

import org.elasticsearch.common.inject.CreationException;
import org.elasticsearch.common.inject.spi.Message;

import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.function.Consumer;

/**
 * Wraps an exception in a special way that it gets formatted
 * "reasonably". This means limits on stacktrace frames and
 * cleanup for guice and failing bootstrap checks, and some
 * guidance about consulting full logs for the whole exception.
 */
//TODO: remove this when guice is removed, and exceptions are cleaned up
//this is horrible, but its what we must do
final class StartupException extends RuntimeException {

    /** maximum length of a stacktrace, before we truncate it */
    static final int STACKTRACE_LIMIT = 30;
    /** all lines from this package are RLE-compressed */
    static final String GUICE_PACKAGE = "org.elasticsearch.common.inject";

    /**
     * Create a new StartupException that will format {@code cause}
     * to the console on failure.
     */
    StartupException(Throwable cause) {
        super(cause);
    }

    /*
     * This logic actually prints the exception to the console, its
     * what is invoked by the JVM when we throw the exception from main()
     */
    @Override
    public void printStackTrace(PrintStream s) {
        printStackTrace(s::println);
    }

    @Override
    public void printStackTrace(PrintWriter w) {
        printStackTrace(w::println);
    }

    private void printStackTrace(Consumer<String> s) {
        Throwable originalCause = getCause();
        Throwable cause = originalCause;
        if (cause instanceof CreationException) {
            cause = getFirstGuiceCause((CreationException)cause);
        }

        String message = cause.toString();
        s.accept(message);

        final int linesWrittenLimit = BootstrapCheck.isBootstrapCheckException(cause) ? 1 : STACKTRACE_LIMIT;

        if (cause != null) {
            // walk to the root cause
            while (cause.getCause() != null) {
                cause = cause.getCause();
            }

            // print the root cause message, only if it differs!
            if (cause != originalCause && (message.equals(cause.toString()) == false)) {
                s.accept("Likely root cause: " + cause);
            }

            // print stacktrace of cause
            StackTraceElement stack[] = cause.getStackTrace();
            int linesWritten = 0;
            for (int i = 0; i < stack.length; i++) {
                if (linesWritten == linesWrittenLimit) {
                    s.accept("\t<<<truncated>>>");
                    break;
                }
                String line = stack[i].toString();

                // skip past contiguous runs of this garbage:
                if (line.startsWith(GUICE_PACKAGE)) {
                    while (i + 1 < stack.length && stack[i + 1].toString().startsWith(GUICE_PACKAGE)) {
                        i++;
                    }
                    s.accept("\tat <<<guice>>>");
                    linesWritten++;
                    continue;
                }

                s.accept("\tat " + line.toString());
                linesWritten++;
            }
        }
        // if its a guice exception, the whole thing really will not be in the log, its megabytes.
        // if it is a bootstrap exception, we do not log the whole stacktrace because it's useless
        // refer to the hack in bootstrap, where we don't log it
        if (originalCause instanceof CreationException == false && BootstrapCheck.isBootstrapCheckException(cause) == false) {
            s.accept("Refer to the log for complete error details.");
        }
    }

    /**
     * Returns first cause from a guice error (it can have multiple).
     */
    static Throwable getFirstGuiceCause(CreationException guice) {
        for (Message message : guice.getErrorMessages()) {
            Throwable cause = message.getCause();
            if (cause != null) {
                return cause;
            }
        }
        return guice; // we tried
    }
}
