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

import java.io.ByteArrayInputStream;
import java.io.StringReader;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandleProxies;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.StandardCharsets;
import java.util.AbstractMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.entitlement.qa.test.EntitlementTest.ExpectedAccess.ALWAYS_ALLOWED;
import static org.elasticsearch.entitlement.qa.test.EntitlementTest.ExpectedAccess.ALWAYS_DENIED;
import static org.elasticsearch.entitlement.qa.test.EntitlementTest.ExpectedAccess.PLUGINS;

/**
 * Ways to change system properties through the object returned by {@link System#getProperties()}.
 */
@SuppressForbidden(reason = "testing entitlements")
@SuppressWarnings({ "unused" /* called via reflection */ })
class SystemPropertiesActions {

    // a property that the test policies list
    private static final String LISTED = "es.entitlements.checkSetSystemProperty";
    // a property that no policy lists, so writing it must always be denied
    private static final String UNLISTED = "es.entitlements.checkSetOtherSystemProperty";

    private static Exception unwrap(Exception e) {
        return e.getCause() instanceof Exception cause ? cause : e;
    }

    @EntitlementTest(expectedAccess = ALWAYS_ALLOWED)
    static void readViaGetProperties() {
        Properties properties = System.getProperties();
        String expected = System.getProperty("java.version");
        if (expected.equals(properties.getProperty("java.version")) == false
            || expected.equals(properties.get("java.version")) == false
            || properties.stringPropertyNames().contains("java.version") == false
            || properties.containsKey("java.version") == false
            || properties.size() < 10
            || ((Properties) properties.clone()).getProperty("java.version") == null) {
            throw new IllegalStateException("system properties are not readable");
        }
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void putListed() {
        Properties properties = System.getProperties();
        properties.put(LISTED, "true");
        if ("true".equals(System.getProperty(LISTED)) == false) {
            throw new IllegalStateException("write did not reach the system properties");
        }
        properties.remove(LISTED);
    }

    @EntitlementTest(expectedAccess = ALWAYS_DENIED)
    static void putUnlisted() {
        System.getProperties().put(UNLISTED, "true");
    }

    @EntitlementTest(expectedAccess = ALWAYS_DENIED)
    static void setPropertyUnlisted() {
        System.getProperties().setProperty(UNLISTED, "true");
    }

    @EntitlementTest(expectedAccess = ALWAYS_DENIED)
    static void putAllWithUnlisted() {
        System.getProperties().putAll(Map.of(LISTED, "true", UNLISTED, "true"));
    }

    /**
     * The map shows no keys to a check, but has an entry to write.
     */
    @EntitlementTest(expectedAccess = ALWAYS_DENIED)
    static void putAllWithHiddenEntry() {
        System.getProperties().putAll(new AbstractMap<>() {
            @Override
            public Set<Object> keySet() {
                return Set.of();
            }

            @Override
            public Set<Entry<Object, Object>> entrySet() {
                return Set.of(Map.entry(UNLISTED, "true"));
            }
        });
    }

    @EntitlementTest(expectedAccess = ALWAYS_DENIED)
    static void computeUnlisted() {
        System.getProperties().compute(UNLISTED, (k, v) -> "true");
    }

    @EntitlementTest(expectedAccess = ALWAYS_DENIED)
    static void mergeUnlisted() {
        System.getProperties().merge(UNLISTED, "true", (a, b) -> "true");
    }

    @EntitlementTest(expectedAccess = ALWAYS_DENIED)
    static void removeUnlisted() {
        System.getProperties().remove(UNLISTED);
    }

    @EntitlementTest(expectedAccess = ALWAYS_DENIED)
    static void replaceAllWithSameValues() {
        System.getProperties().replaceAll((k, v) -> v);
    }

    @EntitlementTest(expectedAccess = ALWAYS_DENIED)
    static void loadUnlisted() throws Exception {
        System.getProperties().load(new StringReader(UNLISTED + "=true"));
    }

    @EntitlementTest(expectedAccess = ALWAYS_DENIED)
    static void loadStreamUnlisted() throws Exception {
        System.getProperties().load(new ByteArrayInputStream((UNLISTED + "=true").getBytes(StandardCharsets.ISO_8859_1)));
    }

    @EntitlementTest(expectedAccess = ALWAYS_DENIED, expectedExceptionIfDenied = UnsupportedOperationException.class)
    static void removeViaKeySet() {
        System.getProperties().keySet().remove(UNLISTED);
    }

    @EntitlementTest(expectedAccess = ALWAYS_DENIED, expectedExceptionIfDenied = UnsupportedOperationException.class)
    static void removeViaValues() {
        System.getProperties().values().remove(UNLISTED);
    }

    @EntitlementTest(expectedAccess = ALWAYS_DENIED, expectedExceptionIfDenied = UnsupportedOperationException.class)
    static void setValueViaEntrySet() {
        var entry = System.getProperties().entrySet().iterator().next();
        entry.setValue(entry.getValue());
    }

    @EntitlementTest(expectedAccess = ALWAYS_DENIED)
    static void putUnlistedViaReflection() throws Exception {
        try {
            Properties.class.getMethod("put", Object.class, Object.class).invoke(System.getProperties(), UNLISTED, "true");
        } catch (InvocationTargetException e) {
            throw unwrap(e);
        }
    }

    @EntitlementTest(expectedAccess = ALWAYS_DENIED)
    static void putUnlistedViaMethodHandle() throws Throwable {
        MethodHandle put = MethodHandles.lookup()
            .findVirtual(Properties.class, "put", MethodType.methodType(Object.class, Object.class, Object.class));
        put.invoke(System.getProperties(), (Object) UNLISTED, (Object) "true");
    }

    /**
     * The JDK collector does the write, so the JDK is the direct caller.
     */
    @EntitlementTest(expectedAccess = ALWAYS_DENIED)
    static void putUnlistedViaJdkCollector() {
        Properties properties = System.getProperties();
        Stream.of(UNLISTED).collect(Collectors.toMap(k -> (Object) k, k -> (Object) "true", (a, b) -> a, () -> properties));
    }

    /**
     * The write runs on a JDK pool thread, with only a lambda of this class on the stack.
     */
    @EntitlementTest(expectedAccess = ALWAYS_DENIED)
    static void putUnlistedOnPoolThread() throws Exception {
        Properties properties = System.getProperties();
        try {
            CompletableFuture.runAsync(() -> properties.put(UNLISTED, "true")).join();
        } catch (CompletionException e) {
            throw unwrap(e);
        }
    }

    /**
     * The write runs on a new thread where every frame except a generated proxy is JDK code.
     */
    @EntitlementTest(expectedAccess = ALWAYS_DENIED)
    static void putUnlistedViaMethodHandleProxyOnNewThread() throws Exception {
        MethodHandle put = MethodHandles.lookup()
            .findVirtual(Properties.class, "put", MethodType.methodType(Object.class, Object.class, Object.class));
        MethodHandle bound = MethodHandles.insertArguments(put, 0, System.getProperties(), UNLISTED, "true");
        @SuppressWarnings("unchecked")
        Callable<Object> callable = MethodHandleProxies.asInterfaceInstance(Callable.class, bound);
        runOnNewThread(new FutureTask<>(callable));
    }

    private static void runOnNewThread(FutureTask<Object> task) throws Exception {
        Thread thread = new Thread(task);
        thread.start();
        try {
            task.get();
        } catch (ExecutionException e) {
            throw unwrap(e);
        }
    }
}
