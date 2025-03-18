/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.qa.test;

import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.entitlement.qa.entitled.EntitledPlugin;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Locale;
import java.util.TimeZone;

import static org.elasticsearch.entitlement.qa.test.EntitlementTest.ExpectedAccess.ALWAYS_DENIED;
import static org.elasticsearch.entitlement.qa.test.EntitlementTest.ExpectedAccess.PLUGINS;

@SuppressForbidden(reason = "testing entitlements")
@SuppressWarnings({ "unused" /* called via reflection */ })
class JvmActions {

    @EntitlementTest(expectedAccess = PLUGINS)
    static void setSystemProperty() {
        System.setProperty("es.entitlements.checkSetSystemProperty", "true");
        try {
            System.clearProperty("es.entitlements.checkSetSystemProperty");
        } catch (RuntimeException e) {
            // ignore for this test case
        }

    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void clearSystemProperty() {
        EntitledPlugin.selfTest(); // TODO: find a better home
        System.clearProperty("es.entitlements.checkClearSystemProperty");
    }

    @EntitlementTest(expectedAccess = ALWAYS_DENIED)
    static void setSystemProperties() {
        System.setProperties(System.getProperties()); // no side effect in case if allowed (but shouldn't)
    }

    @EntitlementTest(expectedAccess = ALWAYS_DENIED)
    static void setDefaultLocale() {
        Locale.setDefault(Locale.getDefault());
    }

    @EntitlementTest(expectedAccess = ALWAYS_DENIED)
    static void setDefaultLocaleForCategory() {
        Locale.setDefault(Locale.Category.DISPLAY, Locale.getDefault(Locale.Category.DISPLAY));
    }

    @EntitlementTest(expectedAccess = ALWAYS_DENIED)
    static void setDefaultTimeZone() {
        TimeZone.setDefault(TimeZone.getDefault());
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void createClassLoader() throws IOException {
        try (var classLoader = new URLClassLoader("test", new URL[0], RestEntitlementsCheckAction.class.getClassLoader())) {
            // intentionally empty, just let the loader close
        }
    }

    @EntitlementTest(expectedAccess = ALWAYS_DENIED)
    static void createLogManager() {
        new java.util.logging.LogManager() {
        };
    }

    @EntitlementTest(expectedAccess = ALWAYS_DENIED)
    static void thread$$setDefaultUncaughtExceptionHandler() {
        Thread.setDefaultUncaughtExceptionHandler(Thread.getDefaultUncaughtExceptionHandler());
    }

    private JvmActions() {}
}
