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
