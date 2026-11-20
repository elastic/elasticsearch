/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.config;

import org.elasticsearch.entitlement.bridge.NotEntitledException;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.StringReader;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Consumer;

public class SystemPropertiesViewTests extends ESTestCase {

    private static final String ALLOWED = "allowed";
    private static final String DENIED = "denied";

    private Properties properties;
    private Properties view;

    @Before
    public void createView() {
        properties = new Properties();
        properties.setProperty(ALLOWED, "a");
        properties.setProperty(DENIED, "d");
        view = new SystemPropertiesView(properties, key -> {
            if (ALLOWED.equals(key) == false) {
                throw new NotEntitledException("not entitled to write [" + key + "]");
            }
        });
    }

    /**
     * A public method that the view does not override would skip the write check or
     * read the empty state of the view. This fails when the JDK adds such a method.
     */
    public void testOverridesEveryPublicMethod() throws NoSuchMethodException {
        List<String> missing = new ArrayList<>();
        for (Method method : Properties.class.getMethods()) {
            if (Modifier.isStatic(method.getModifiers()) || Modifier.isFinal(method.getModifiers())) {
                continue;
            }
            Method resolved = SystemPropertiesView.class.getMethod(method.getName(), method.getParameterTypes());
            if (resolved.getDeclaringClass() == Object.class) {
                continue;
            }
            if (resolved.getDeclaringClass() != SystemPropertiesView.class) {
                missing.add(resolved.toString());
            }
        }
        assertEquals("methods not overridden by the view", List.of(), missing);
    }

    public void testReadsSeeRealProperties() {
        assertEquals("d", view.getProperty(DENIED));
        assertEquals("d", view.get(DENIED));
        assertEquals(2, view.size());
        assertTrue(view.containsKey(ALLOWED));
        assertEquals(properties.stringPropertyNames(), view.stringPropertyNames());
        assertEquals(properties.keySet(), view.keySet());
        assertEquals(properties.entrySet(), view.entrySet());

        properties.setProperty("later", "l");
        assertEquals("l", view.getProperty("later"));
        assertEquals(3, view.size());
    }

    public void testAllowedWritesReachRealProperties() throws Exception {
        view.setProperty(ALLOWED, "1");
        assertEquals("1", properties.getProperty(ALLOWED));
        view.put(ALLOWED, "2");
        assertEquals("2", properties.getProperty(ALLOWED));
        view.putAll(Map.of(ALLOWED, "3"));
        assertEquals("3", properties.getProperty(ALLOWED));
        view.load(new StringReader(ALLOWED + "=4"));
        assertEquals("4", properties.getProperty(ALLOWED));
        view.compute(ALLOWED, (k, v) -> "5");
        assertEquals("5", properties.getProperty(ALLOWED));
        view.remove(ALLOWED);
        assertNull(properties.getProperty(ALLOWED));
    }

    public void testDeniedWritesDoNotReachRealProperties() throws Exception {
        ByteArrayOutputStream xml = new ByteArrayOutputStream();
        Properties toLoad = new Properties();
        toLoad.setProperty(DENIED, "x");
        toLoad.storeToXML(xml, null);

        List<Consumer<Properties>> writes = List.of(
            p -> p.setProperty(DENIED, "x"),
            p -> p.put(DENIED, "x"),
            p -> p.remove(DENIED),
            p -> p.remove(DENIED, "d"),
            p -> p.putAll(Map.of(ALLOWED, "x", DENIED, "x")),
            p -> p.clear(),
            p -> p.replaceAll((k, v) -> "x"),
            p -> p.putIfAbsent(DENIED, "x"),
            p -> p.replace(DENIED, "d", "x"),
            p -> p.replace(DENIED, "x"),
            p -> p.computeIfAbsent(DENIED, k -> "x"),
            p -> p.computeIfPresent(DENIED, (k, v) -> "x"),
            p -> p.compute(DENIED, (k, v) -> "x"),
            p -> p.merge(DENIED, "x", (a, b) -> "x"),
            p -> {
                try {
                    p.load(new StringReader(DENIED + "=x"));
                } catch (java.io.IOException e) {
                    throw new AssertionError(e);
                }
            },
            p -> {
                try {
                    p.load(new ByteArrayInputStream((DENIED + "=x").getBytes(StandardCharsets.ISO_8859_1)));
                } catch (java.io.IOException e) {
                    throw new AssertionError(e);
                }
            },
            p -> {
                try {
                    p.loadFromXML(new ByteArrayInputStream(xml.toByteArray()));
                } catch (java.io.IOException e) {
                    throw new AssertionError(e);
                }
            }
        );
        for (int i = 0; i < writes.size(); i++) {
            Consumer<Properties> write = writes.get(i);
            expectThrows(NotEntitledException.class, "write " + i, () -> write.accept(view));
            assertEquals("write " + i, "d", properties.getProperty(DENIED));
        }
    }

    public void testNonStringKeysAreRejected() {
        expectThrows(IllegalArgumentException.class, () -> view.put(1, "x"));
        expectThrows(IllegalArgumentException.class, () -> view.remove(1));
        expectThrows(NullPointerException.class, () -> view.put(null, "x"));
        assertEquals(2, properties.size());
    }

    public void testCollectionViewsAreReadOnly() {
        expectThrows(UnsupportedOperationException.class, () -> view.keySet().remove(DENIED));
        expectThrows(UnsupportedOperationException.class, () -> view.keySet().clear());
        expectThrows(UnsupportedOperationException.class, () -> view.values().remove("d"));
        expectThrows(UnsupportedOperationException.class, () -> view.entrySet().clear());
        expectThrows(UnsupportedOperationException.class, () -> view.entrySet().iterator().next().setValue("x"));
        expectThrows(UnsupportedOperationException.class, () -> {
            var iterator = view.keySet().iterator();
            iterator.next();
            iterator.remove();
        });
        assertEquals("d", properties.getProperty(DENIED));
        assertEquals("a", properties.getProperty(ALLOWED));
    }

    public void testCloneIsDetached() {
        Properties clone = (Properties) view.clone();
        assertEquals(Properties.class, clone.getClass());
        assertEquals(properties, clone);
        clone.setProperty(DENIED, "x");
        assertEquals("d", properties.getProperty(DENIED));
    }
}
