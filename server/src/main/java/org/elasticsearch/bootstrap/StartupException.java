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
 * cleanup for guice, and some guidance about consulting full
 * logs for the whole exception.
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
    public void printStackTrace(PrintWriter s) {
        printStackTrace(s::println);
    }

    private void printStackTrace(Consumer<String> consumer) {
        Throwable originalCause = getCause();
        Throwable cause = originalCause;
        if (cause instanceof CreationException) {
            cause = getFirstGuiceCause((CreationException)cause);
        }

        String message = cause.toString();
        consumer.accept(message);

        if (cause != null) {
            // walk to the root cause
            while (cause.getCause() != null) {
                cause = cause.getCause();
            }

            // print the root cause message, only if it differs!
            if (cause != originalCause && (message.equals(cause.toString()) == false)) {
                consumer.accept("Likely root cause: " + cause);
            }

            // print stacktrace of cause
            StackTraceElement stack[] = cause.getStackTrace();
            int linesWritten = 0;
            for (int i = 0; i < stack.length; i++) {
                if (linesWritten == STACKTRACE_LIMIT) {
                    consumer.accept("\t<<<truncated>>>");
                    break;
                }
                String line = stack[i].toString();

                // skip past contiguous runs of this garbage:
                if (line.startsWith(GUICE_PACKAGE)) {
                    while (i + 1 < stack.length && stack[i + 1].toString().startsWith(GUICE_PACKAGE)) {
                        i++;
                    }
                    consumer.accept("\tat <<<guice>>>");
                    linesWritten++;
                    continue;
                }

                consumer.accept("\tat " + line);
                linesWritten++;
            }
        }
        // if its a guice exception, the whole thing really will not be in the log, its megabytes.
        // refer to the hack in bootstrap, where we don't log it
        if (originalCause instanceof CreationException == false) {
            final String basePath = System.getProperty("es.logs.base_path");
            // It's possible to fail before logging has been configured, in which case there's no point
            // suggested that the user look in the log file.
            if (basePath != null) {
                final String logPath = System.getProperty("es.logs.base_path")
                    + System.getProperty("file.separator")
                    + System.getProperty("es.logs.cluster_name")
                    + ".log";

                consumer.accept("For complete error details, refer to the log at " + logPath);
            }
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
