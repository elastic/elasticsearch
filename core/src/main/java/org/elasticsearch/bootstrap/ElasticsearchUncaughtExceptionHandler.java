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

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.logging.Loggers;

import java.io.IOError;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Objects;
import java.util.function.Supplier;

class ElasticsearchUncaughtExceptionHandler implements Thread.UncaughtExceptionHandler {

    private final Supplier<String> loggingPrefixSupplier;

    ElasticsearchUncaughtExceptionHandler(final Supplier<String> loggingPrefixSupplier) {
        this.loggingPrefixSupplier = Objects.requireNonNull(loggingPrefixSupplier);
    }

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
        } else {
            onNonFatalUncaught(t.getName(), e);
        }
    }

    // visible for testing
    static boolean isFatalUncaught(Throwable e) {
        return e instanceof Error;
    }

    // visible for testing
    void onFatalUncaught(final String threadName, final Throwable t) {
        final Logger logger = Loggers.getLogger(ElasticsearchUncaughtExceptionHandler.class, loggingPrefixSupplier.get());
        logger.error(
            (org.apache.logging.log4j.util.Supplier<?>)
                () -> new ParameterizedMessage("fatal error in thread [{}], exiting", threadName), t);
    }

    // visible for testing
    void onNonFatalUncaught(final String threadName, final Throwable t) {
        final Logger logger = Loggers.getLogger(ElasticsearchUncaughtExceptionHandler.class, loggingPrefixSupplier.get());
        logger.warn((org.apache.logging.log4j.util.Supplier<?>)
            () -> new ParameterizedMessage("uncaught exception in thread [{}]", threadName), t);
    }

    // visible for testing
    void halt(int status) {
        AccessController.doPrivileged(new PrivilegedAction<Void>() {
            @SuppressForbidden(reason = "halt")
            @Override
            public Void run() {
                // we halt to prevent shutdown hooks from running
                Runtime.getRuntime().halt(status);
                return null;
            }
        });
    }

}
