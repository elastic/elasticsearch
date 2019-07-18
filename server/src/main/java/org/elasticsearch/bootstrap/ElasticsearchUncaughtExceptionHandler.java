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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.SuppressForbidden;

import java.io.IOError;
import java.security.AccessController;
import java.security.PrivilegedAction;

class ElasticsearchUncaughtExceptionHandler implements Thread.UncaughtExceptionHandler {
    private static final Logger logger = LogManager.getLogger(ElasticsearchUncaughtExceptionHandler.class);

    @Override
    public void uncaughtException(Thread t, Throwable e) {
        if (isFatalUncaught(e)) {
            try {
                onFatalUncaught(t.getName(), e);
            } finally {
                // we use specific error codes in case the above notification failed, at least we
                // will have some indication of the error bringing us down
                if (e instanceof InternalError) {
                    halt(128);
                } else if (e instanceof OutOfMemoryError) {
                    halt(127);
                } else if (e instanceof StackOverflowError) {
                    halt(126);
                } else if (e instanceof UnknownError) {
                    halt(125);
                } else if (e instanceof IOError) {
                    halt(124);
                } else {
                    halt(1);
                }
            }
        } else if (e instanceof StartupException) {
            // StartupException means that this server didn't start, and we want to do everything we can to make that
            // error clear to anyone who consults the logs so that they're not simply overwhelmed by a stack trace.
            onStartupException(t.getName(), (StartupException) e);
        } else {
            onNonFatalUncaught(t.getName(), e);
        }
    }

    static boolean isFatalUncaught(Throwable e) {
        return e instanceof Error;
    }

    void onFatalUncaught(final String threadName, final Throwable t) {
        logger.error(() -> new ParameterizedMessage("fatal error in thread [{}], exiting", threadName), t);
    }

    void onNonFatalUncaught(final String threadName, final Throwable t) {
        logger.warn(() -> new ParameterizedMessage("uncaught exception in thread [{}]", threadName), t);
    }

    void onStartupException(final String threadName, final StartupException e) {
        String bannerMessage = describeStartupException(e);
        logger.error(bannerMessage);
        logger.warn(() -> new ParameterizedMessage("uncaught exception in thread [{}]", threadName), e);
        // Log the error message twice (before and after the stack trace) so that it is super-obvious to anyone reading the logs
        logger.error(bannerMessage);
    }

    // accessible for testing
    static String describeStartupException(StartupException e) {
        StringBuilder bannerMessage = new StringBuilder("an exception was thrown that prevented this node from starting (")
            // Append the top message so that it as clear as possible that this message is just a summary of the stacktrace next to it.
            .append(e.getMessage())
            .append(")");
        // Find the first elasticsearch exception, that message is the most likely to provide a helpful explanation
        ElasticsearchException esCause = (ElasticsearchException) ExceptionsHelper.unwrap(e, ElasticsearchException.class);
        if (esCause != null) {
            bannerMessage.append("\nthis was caused by:");
            // Allow the elasticsearch exception to decide on the best root cause(s to report)
            for (ElasticsearchException root : esCause.guessRootCauses()) {
                bannerMessage.append("\n * ").append(root.getMessage())
                    .append(" (").append(ElasticsearchException.getExceptionName(root)).append(")");
                String indent = "   ";
                Throwable cause = root.getCause();
                for (int counter = 0; counter < 3 && cause != null; counter++) {
                    bannerMessage.append('\n').append(indent).append("- caused by: ")
                        .append(cause.getMessage()).append(" (").append(ElasticsearchException.getExceptionName(cause)).append(")");
                    cause = cause.getCause();
                    indent += "  ";
                }
            }
        }
        return bannerMessage.toString();
    }

    void halt(int status) {
        AccessController.doPrivileged(new PrivilegedHaltAction(status));
    }

    static class PrivilegedHaltAction implements PrivilegedAction<Void> {

        private final int status;

        private PrivilegedHaltAction(final int status) {
            this.status = status;
        }

        @SuppressForbidden(reason = "halt")
        @Override
        public Void run() {
            // we halt to prevent shutdown hooks from running
            Runtime.getRuntime().halt(status);
            return null;
        }

    }

}
