/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.tools.publiccallersfinder;

import org.elasticsearch.entitlement.tools.ExternalAccess;
import org.elasticsearch.entitlement.tools.publiccallersfinder.FindUsagesClassVisitor.MethodDescriptor;
import org.elasticsearch.test.ESTestCase;
import org.objectweb.asm.ClassReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.function.Consumer;

import static org.elasticsearch.entitlement.tools.ExternalAccess.PROTECTED_METHOD;
import static org.elasticsearch.entitlement.tools.ExternalAccess.PUBLIC_CLASS;
import static org.elasticsearch.entitlement.tools.ExternalAccess.PUBLIC_METHOD;
import static org.hamcrest.Matchers.hasItem;

public class FindUsagesClassVisitorTests extends ESTestCase {

    public void testFindUsagesClassVisitor() throws IOException {
        var callers = findCallers(TestClass.class, new MethodDescriptor("java/lang/String", "length", "()I"));

        assertEquals(6, callers.size());
        assertThat(
            "Expected publicMethod not found",
            callers,
            hasItem(new FoundCaller("publicMethod", EnumSet.of(PUBLIC_CLASS, PUBLIC_METHOD)))
        );
        assertThat("Expected privateMethod not found", callers, hasItem(new FoundCaller("privateMethod", EnumSet.of(PUBLIC_CLASS))));
        assertThat(
            "Expected protectedMethod not found",
            callers,
            hasItem(new FoundCaller("protectedMethod", EnumSet.of(PROTECTED_METHOD, PUBLIC_CLASS)))
        );
        assertThat(
            "Expected staticMethod not found",
            callers,
            hasItem(new FoundCaller("staticMethod", EnumSet.of(PUBLIC_CLASS, PUBLIC_METHOD)))
        );
        assertThat("Expected <init> not found", callers, hasItem(new FoundCaller("<init>", EnumSet.of(PUBLIC_CLASS, PUBLIC_METHOD))));
        assertThat("Expected lambda not found", callers, hasItem(new FoundCaller("lambda$lambdaMethod$0", EnumSet.of(PUBLIC_CLASS))));
    }

    public void testMultipleCallsInSameMethod() throws IOException {
        var callers = findCallers(MultipleCallsTestClass.class, new MethodDescriptor("java/lang/String", "length", "()I"));
        assertEquals(2, callers.size());
        assertTrue(callers.stream().allMatch(c -> c.methodName().equals("lengthTwice")));
    }

    public void testMethodDescriptorMatching() throws IOException {
        var exactResult = findCallers(
            OverloadedMethodsTestClass.class,
            new MethodDescriptor("java/lang/String", "substring", "(I)Ljava/lang/String;")
        );
        assertEquals(1, exactResult.size());
        assertThat(exactResult, hasItem(new FoundCaller("substringFrom", EnumSet.of(PUBLIC_CLASS, PUBLIC_METHOD))));

        // null descriptor matches all usages
        var wideResult = findCallers(OverloadedMethodsTestClass.class, new MethodDescriptor("java/lang/String", "substring", null));
        assertEquals(2, wideResult.size());
        assertThat(wideResult, hasItem(new FoundCaller("substringFrom", EnumSet.of(PUBLIC_CLASS, PUBLIC_METHOD))));
        assertThat(wideResult, hasItem(new FoundCaller("substringRange", EnumSet.of(PUBLIC_CLASS, PUBLIC_METHOD))));
    }

    record FoundCaller(String methodName, EnumSet<ExternalAccess> access) {}

    private List<FoundCaller> findCallers(Class<?> classToScan, MethodDescriptor methodToFind) throws IOException {
        List<FoundCaller> callers = new ArrayList<>();
        var visitor = new FindUsagesClassVisitor(
            methodToFind,
            m -> true,
            (source, line, method, access) -> callers.add(new FoundCaller(method.methodName(), access))
        );
        new ClassReader(classToScan.getName()).accept(visitor, 0);
        return callers;
    }

    public static class TestClass {
        public TestClass(String s) {
            s.length();
        }

        public void publicMethod(String s) {
            s.length();
        }

        private void privateMethod(String s) {
            s.length();
        }

        protected void protectedMethod(String s) {
            s.length();
        }

        public static void staticMethod(String s) {
            s.length();
        }

        public Consumer<String> lambdaMethod() {
            return s -> s.length();
        }
    }

    public static class MultipleCallsTestClass {
        public void lengthTwice(String s1, String s2) {
            s1.length();
            s2.length();
        }
    }

    public static class OverloadedMethodsTestClass {
        public String substringFrom(String s, int start) {
            return s.substring(start);
        }

        public String substringRange(String s, int start, int end) {
            return s.substring(start, end);
        }
    }
}
