/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless.javadoc;

import org.elasticsearch.test.ESTestCase;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

public class JavadocExtractorTests extends ESTestCase  {
    private static final String clazz = "\n" +
        "package foo;\n"+
        "public class Example {\n" +
        "    /** foo returns zero */\n" +
        "    public int foo(int abc, Map<String, String> map) { return 0; }\n" +
        "}";

    private static class ExampleClassResolver implements JavaClassResolver {
        private final String license;
        public ExampleClassResolver(String license) {
            this.license = license;
        }
        public ExampleClassResolver() {
            this.license = "/*" +
                "\n * This code is free software; you can redistribute it and/or modify it"+
                "\n * under the terms of the GNU General Public License version 2 only, as"+
                "\n * published by the Free Software Foundation." +
                "\n */\n";
        }
        @Override
        public InputStream openClassFile(String className) throws IOException {
            if ("Example".equals(className)) {
                return new ByteArrayInputStream((license + clazz).getBytes());
            }
            return null;
        }
    }

    public void testLicenseEnforcementNoLicense() throws IOException {
        JavadocExtractor extractor = new JavadocExtractor(new ExampleClassResolver(""));
        JavadocExtractor.ParsedJavaClass parsed = extractor.parseClass("Example");
        JavadocExtractor.ParsedMethod method = parsed.getMethod("foo", List.of("int", "Map<String,String>"));
        assertNull(method);
    }

    public void testLicenseEnforcementGPLLicense() throws IOException {
        JavadocExtractor extractor = new JavadocExtractor(new ExampleClassResolver());
        JavadocExtractor.ParsedJavaClass parsed = extractor.parseClass("Example");
        JavadocExtractor.ParsedMethod method = parsed.getMethod("foo", List.of("int", "Map<String,String>"));
        assertNotNull(method);
        assertEquals(List.of("abc", "map"), method.parameterNames);
    }

    public void testLicenseEnforcementProprietaryLicense() throws IOException {
        JavadocExtractor extractor = new JavadocExtractor(new ExampleClassResolver("/* Foocorp all rights reserved */"));
        JavadocExtractor.ParsedJavaClass parsed = extractor.parseClass("Example");
        JavadocExtractor.ParsedMethod method = parsed.getMethod("foo", List.of("int", "Map<String,String>"));
        assertNull(method);
    }
}
