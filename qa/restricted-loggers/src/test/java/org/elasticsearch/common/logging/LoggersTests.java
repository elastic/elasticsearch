/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.logging;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.logging.Loggers.checkRestrictedLoggers;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

public class LoggersTests extends ESTestCase {

    public void testClusterUpdateSettingsRequestValidationForLoggers() {
        assertThat(Loggers.RESTRICTED_LOGGERS, hasSize(greaterThan(0)));

        ClusterUpdateSettingsRequest request = new ClusterUpdateSettingsRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT);
        for (String logger : Loggers.RESTRICTED_LOGGERS) {
            var validation = request.persistentSettings(Map.of("logger." + logger, org.elasticsearch.logging.Level.DEBUG)).validate();
            assertNotNull(validation);
            assertThat(validation.validationErrors(), contains("Level [DEBUG] is not permitted for logger [" + logger + "]"));
            // INFO is permitted
            assertNull(request.persistentSettings(Map.of("logger." + logger, org.elasticsearch.logging.Level.INFO)).validate());
        }
    }

    public void testCheckRestrictedLoggers() {
        assertThat(Loggers.RESTRICTED_LOGGERS, hasSize(greaterThan(0)));

        Settings settings;
        for (String restricted : Loggers.RESTRICTED_LOGGERS) {
            for (String suffix : List.of("", ".xyz")) {
                String logger = restricted + suffix;
                for (Level level : List.of(Level.ALL, Level.TRACE, Level.DEBUG)) {
                    settings = Settings.builder().put("logger." + logger, level).build();
                    List<String> errors = checkRestrictedLoggers(settings);
                    assertThat(errors, contains("Level [" + level + "] is not permitted for logger [" + logger + "]"));
                }
                for (Level level : List.of(Level.ERROR, Level.WARN, Level.INFO)) {
                    settings = Settings.builder().put("logger." + logger, level).build();
                    assertThat(checkRestrictedLoggers(settings), hasSize(0));
                }

                settings = Settings.builder().put("logger." + logger, "INVALID").build();
                assertThat(checkRestrictedLoggers(settings), hasSize(0));

                settings = Settings.builder().put("logger." + logger, (String) null).build();
                assertThat(checkRestrictedLoggers(settings), hasSize(0));
            }
        }
    }

    public void testSetLevelWithRestrictions() {
        assertThat(Loggers.RESTRICTED_LOGGERS, hasSize(greaterThan(0)));

        for (String restricted : Loggers.RESTRICTED_LOGGERS) {
            TestLoggers.runWithLoggersRestored(() -> {
                // 'org.apache.http' is an example of a restricted logger,
                // a restricted component logger would be `org.apache.http.client.HttpClient` for instance,
                // and the parent logger is `org.apache`.
                Logger restrictedLogger = LogManager.getLogger(restricted);
                Logger restrictedComponent = LogManager.getLogger(restricted + ".component");
                Logger parentLogger = LogManager.getLogger(restricted.substring(0, restricted.lastIndexOf('.')));

                Loggers.setLevel(restrictedLogger, Level.INFO);
                assertHasINFO(restrictedLogger, restrictedComponent);

                for (Logger log : List.of(restrictedComponent, restrictedLogger)) {
                    // DEBUG is rejected due to restriction
                    Loggers.setLevel(log, Level.DEBUG);
                    assertHasINFO(restrictedComponent, restrictedLogger);
                }

                // OK for parent `org.apache`, but restriction is enforced for restricted descendants
                Loggers.setLevel(parentLogger, Level.DEBUG);
                assertEquals(Level.DEBUG, parentLogger.getLevel());
                assertHasINFO(restrictedComponent, restrictedLogger);

                // Inheriting DEBUG of parent `org.apache` is rejected
                Loggers.setLevel(restrictedLogger, (Level) null);
                assertHasINFO(restrictedComponent, restrictedLogger);

                // DEBUG of root logger isn't propagated to restricted loggers
                Loggers.setLevel(LogManager.getRootLogger(), Level.DEBUG);
                assertEquals(Level.DEBUG, LogManager.getRootLogger().getLevel());
                assertHasINFO(restrictedComponent, restrictedLogger);
            });
        }
    }

    private static void assertHasINFO(Logger... loggers) {
        for (Logger log : loggers) {
            assertThat("Unexpected log level for [" + log.getName() + "]", log.getLevel(), is(Level.INFO));
        }
    }
}
