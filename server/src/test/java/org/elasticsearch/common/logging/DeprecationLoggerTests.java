/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.logging.log4j.simple.SimpleLoggerContext;
import org.apache.logging.log4j.simple.SimpleLoggerContextFactory;
import org.apache.logging.log4j.spi.ExtendedLogger;
import org.apache.logging.log4j.spi.LoggerContext;
import org.apache.logging.log4j.spi.LoggerContextFactory;
import org.elasticsearch.common.SuppressLoggerChecks;
import org.elasticsearch.test.ESTestCase;

import java.net.URI;
import java.security.AccessControlContext;
import java.security.AccessController;
import java.security.Permissions;
import java.security.PrivilegedAction;
import java.security.ProtectionDomain;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.Is.is;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

public class DeprecationLoggerTests extends ESTestCase {
    @SuppressLoggerChecks(reason = "Safe as this is using mockito")
    public void testLogPermissions() {
        AtomicBoolean supplierCalled = new AtomicBoolean(false);

        // mocking the logger used inside DeprecationLogger requires heavy hacking...
        ExtendedLogger mockLogger = mock(ExtendedLogger.class);
        doAnswer(invocationOnMock -> {
            supplierCalled.set(true);
            createTempDir(); // trigger file permission, like rolling logs would
            return null;
        }).when(mockLogger).warn(DeprecatedMessage.of(any(), "foo"));
        final LoggerContext context = new SimpleLoggerContext() {
            @Override
            public ExtendedLogger getLogger(String name) {
                return mockLogger;
            }
        };

        final LoggerContextFactory originalFactory = LogManager.getFactory();
        try {
            LogManager.setFactory(new SimpleLoggerContextFactory() {
                @Override
                public LoggerContext getContext(String fqcn, ClassLoader loader, Object externalContext, boolean currentContext,
                                                URI configLocation, String name) {
                    return context;
                }
            });
            DeprecationLogger deprecationLogger = DeprecationLogger.getLogger("logger");

            AccessControlContext noPermissionsAcc = new AccessControlContext(
                new ProtectionDomain[]{new ProtectionDomain(null, new Permissions())}
            );
            AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
                deprecationLogger.deprecate("testLogPermissions_key", "foo {}", "bar");
                return null;
            }, noPermissionsAcc);
            assertThat("supplier called", supplierCalled.get(), is(true));

            assertWarnings("foo bar");
        } finally {
            LogManager.setFactory(originalFactory);
        }
    }

    public void testMultipleSlowLoggersUseSingleLog4jLogger() {
        org.apache.logging.log4j.core.LoggerContext context = (org.apache.logging.log4j.core.LoggerContext) LogManager.getContext(false);

        DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(DeprecationLoggerTests.class);
        int numberOfLoggersBefore = context.getLoggers().size();

        class LoggerTest{
        }
        DeprecationLogger deprecationLogger2 = DeprecationLogger.getLogger(LoggerTest.class);

        context = (org.apache.logging.log4j.core.LoggerContext) LogManager.getContext(false);
        int numberOfLoggersAfter = context.getLoggers().size();

        assertThat(numberOfLoggersAfter, equalTo(numberOfLoggersBefore+1));
    }
}
