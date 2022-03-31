/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.ldap.support;

import com.unboundid.ldap.listener.InMemoryDirectoryServerConfig;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.Strings;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.MemoryHandler;

public class LdapServerDebugLogging {
    private final MemoryHandler logHandler;
    private final Logger targetLogger;
    private final AtomicBoolean hasLogMessage = new AtomicBoolean(false);

    public LdapServerDebugLogging(Logger targetLogger) {
        this.logHandler = new MemoryHandler(new InfoLoggingHandler(targetLogger), 1000, Level.WARNING) {
            @Override
            public void publish(LogRecord record) {
                hasLogMessage.set(true);
                super.publish(record);
            }
        };
        this.targetLogger = targetLogger;
    }

    public TestRule getTestWatcher() {
        return new TestWatcher() {
            @Override
            protected void failed(Throwable e, Description description) {
                if (hasLogMessage.get()) {
                    targetLogger.info("Test [{}] failed, printing debug output from LDAP server", description);
                    logHandler.push();
                } else {
                    targetLogger.info("Test [{}] failed, but no debug output was received from LDAP server", description);
                }
            }
        };
    }

    public void configure(InMemoryDirectoryServerConfig config) {
        targetLogger.info("Configuring debug logging for LDAP server [{}]", config);
        config.setLDAPDebugLogHandler(logHandler);
    }

    private static class InfoLoggingHandler extends Handler {

        private final Logger target;

        InfoLoggingHandler(Logger target) {
            this.target = target;
        }

        @Override
        public void publish(LogRecord record) {
            String message = record.getMessage();
            if (message == null) {
                message = "";
            }
            if (Strings.hasText(record.getLoggerName())) {
                message = "(" + record.getLoggerName() + ") " + message;
            }
            target.info("LDAP Server Debugging Info @(" + record.getInstant() + ") : " + message);
        }

        @Override
        public void flush() {
            // no-op
        }

        @Override
        public void close() throws SecurityException {
            // no-op
        }
    }
}
