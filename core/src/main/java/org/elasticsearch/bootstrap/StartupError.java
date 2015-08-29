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

/**
 * Wraps an exception in a special way that it gets formatted
 * "reasonably". This means limits on stacktrace frames and
 * cleanup for guice, and some guidance about consulting full
 * logs for the whole exception.
 */
//TODO: remove this when guice is removed, and exceptions are cleaned up
//this is horrible, but its what we must do
final class StartupError extends RuntimeException {
    
    /** maximum length of a stacktrace, before we truncate it */
    static final int STACKTRACE_LIMIT = 30;
    /** all lines from this package are RLE-compressed */
    static final String GUICE_PACKAGE = "org.elasticsearch.common.inject";
    
    /** 
     * Create a new StartupError that will format {@code cause}
     * to the console on failure.
     */
    StartupError(Throwable cause) {
        super(cause);
    }

    /*
     * This logic actually prints the exception to the console, its
     * what is invoked by the JVM when we throw the exception from main()
     */
    @Override
    public void printStackTrace(PrintStream s) {
        Throwable originalCause = getCause();
        Throwable cause = originalCause;
        if (cause instanceof CreationException) {
            cause = getFirstGuiceCause((CreationException)cause);
        }
        
        String message = cause.toString();
        s.println(message);
        
        if (cause != null) {
            // walk to the root cause
            while (cause.getCause() != null) {
                cause = cause.getCause();
            }

            // print the root cause message, only if it differs!
            if (cause != originalCause && (message.equals(cause.toString()) == false)) {
                s.println("Likely root cause: " + cause);
            }

            // print stacktrace of cause
            StackTraceElement stack[] = cause.getStackTrace();
            int linesWritten = 0;
            for (int i = 0; i < stack.length; i++) {
                if (linesWritten == STACKTRACE_LIMIT) {
                    s.println("\t<<<truncated>>>");
                    break;
                }
                String line = stack[i].toString();
                
                // skip past contiguous runs of this garbage:
                if (line.startsWith(GUICE_PACKAGE)) {
                    while (i + 1 < stack.length && stack[i + 1].toString().startsWith(GUICE_PACKAGE)) {
                        i++;
                    }
                    s.println("\tat <<<guice>>>");
                    linesWritten++;
                    continue;
                }

                s.println("\tat " + line.toString());
                linesWritten++;
            }
        }
        s.println("Refer to the log for complete error details.");
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
