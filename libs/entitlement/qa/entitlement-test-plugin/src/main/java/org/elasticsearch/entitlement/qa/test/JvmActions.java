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
import java.util.Properties;
import java.util.TimeZone;

import javax.xml.parsers.SAXParserFactory;

import static org.elasticsearch.entitlement.qa.test.EntitlementTest.ExpectedAccess.ALWAYS_ALLOWED;
import static org.elasticsearch.entitlement.qa.test.EntitlementTest.ExpectedAccess.ALWAYS_DENIED;
import static org.elasticsearch.entitlement.qa.test.EntitlementTest.ExpectedAccess.PLUGINS;

@SuppressForbidden(reason = "testing entitlements")
@SuppressWarnings({ "unused" /* called via reflection */ })
class JvmActions {

    @EntitlementTest(expectedAccess = PLUGINS, expectedDefaultIfDenied = "null")
    static String setSystemProperty() {
        String result = System.setProperty("es.entitlements.checkSetSystemProperty", "true");
        try {
            System.clearProperty("es.entitlements.checkSetSystemProperty");
        } catch (RuntimeException e) {
            // ignore for this test case
        }
        return String.valueOf(result);
    }

    @EntitlementTest(expectedAccess = PLUGINS, expectedDefaultIfDenied = "null")
    static String clearSystemProperty() {
        EntitledPlugin.selfTest(); // TODO: find a better home
        String result = System.clearProperty("es.entitlements.checkClearSystemProperty");
        return String.valueOf(result);
    }

    @EntitlementTest(expectedAccess = ALWAYS_DENIED, expectedDefaultIfDenied = "true")
    static String setSystemProperties() {
        Properties original = System.getProperties();
        System.setProperties(new Properties(original));
        boolean unchanged = System.getProperties() == original;
        if (unchanged == false) {
            System.setProperties(original);
        }
        return String.valueOf(unchanged);
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

    @EntitlementTest(expectedAccess = PLUGINS)
    static void createClassLoaderNewInstance1() throws IOException {
        try (var classLoader = URLClassLoader.newInstance(new URL[0])) {
            // intentionally empty, just let the loader close
        }
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void createClassLoaderNewInstance2() throws IOException {
        try (var classLoader = URLClassLoader.newInstance(new URL[0], RestEntitlementsCheckAction.class.getClassLoader())) {
            // intentionally empty, just let the loader close
        }
    }

    @EntitlementTest(expectedAccess = ALWAYS_DENIED)
    static void createLogManager() {
        new java.util.logging.LogManager() {};
    }

    @EntitlementTest(expectedAccess = ALWAYS_DENIED)
    static void thread$$setDefaultUncaughtExceptionHandler() {
        Thread.setDefaultUncaughtExceptionHandler(Thread.getDefaultUncaughtExceptionHandler());
    }

    @EntitlementTest(expectedAccess = ALWAYS_ALLOWED)
    static void useJavaXmlParser() {
        // java.xml is part of the jdk, but not a system module. this checks it's actually usable
        // as it needs to read classes from the jdk which is not generally allowed
        SAXParserFactory.newInstance();
    }

    private JvmActions() {}
}
