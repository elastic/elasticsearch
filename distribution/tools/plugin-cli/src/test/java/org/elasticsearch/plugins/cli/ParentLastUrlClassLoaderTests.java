/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.plugins.cli;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.compiler.InMemoryJavaCompiler;
import org.elasticsearch.test.jar.JarUtils;

import java.net.URL;
import java.nio.file.Path;
import java.util.Map;
import java.util.function.Predicate;

public class ParentLastUrlClassLoaderTests extends ESTestCase {

    public void testLoadClass() throws Exception {
        Path tmpDir = createTempDir();
        Path jar = tmpDir.resolve("lib.jar");
        String src = """
            package widget;
            public class Widget {
              public static int run() {
                return 42;
              }
            }
            """;
        JarUtils.createJarWithEntries(jar, Map.of("widget/Widget.class", InMemoryJavaCompiler.compile("widget.Widget", src)));
        URL[] urls = { jar.toUri().toURL() };
        Predicate<String> filter = name -> name.startsWith("widget.");
        try (var classLoader = new ParentLastUrlClassLoader(urls, getClass().getClassLoader(), filter)) {
            for (int i = 0; i < 2; i++) {
                boolean resolve = i == 0;
                Class<?> widgetClass = classLoader.loadClass("widget.Widget", resolve);
                assertEquals(classLoader, widgetClass.getClassLoader());
                int result = (int) widgetClass.getMethod("run").invoke(null);
                assertEquals(42, result);
                Class<?> strClass = classLoader.loadClass("java.lang.String", resolve);
                assertNotEquals(classLoader, strClass.getClassLoader());
            }
        }
    }

}
