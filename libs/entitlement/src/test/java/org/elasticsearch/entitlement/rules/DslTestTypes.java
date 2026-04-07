/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.rules;

/**
 * Dummy types for testing the EntitlementRulesBuilder DSL. Do not use real JDK types
 * in DSL tests so behaviour is stable across implementations and JDK versions.
 */
public final class DslTestTypes {

    private DslTestTypes() {}

    /** Concrete class: instance/static/void methods and constructors. */
    public static class Concrete {
        public Concrete() {}

        public Concrete(String arg) {}

        public boolean noArg() {
            return true;
        }

        public String withArg(String a) {
            return a;
        }

        public int withInt(int x) {
            return x;
        }

        public void voidNoArg() {}

        public void voidWithArg(String a) {}

        public static boolean staticNoArg() {
            return true;
        }

        public static String staticWithArg(int i) {
            return String.valueOf(i);
        }

        public static void staticVoidNoArg() {}

        public static void staticVoidWithArg(String a) {}

        public void overloaded(int i) {}

        public void overloaded(String s) {}

        public String withArray(byte[] bytes) {
            return bytes == null ? null : String.valueOf(bytes.length);
        }
    }

    /** Abstract class for testing SerializedLambda resolution on abstract types. */
    public abstract static class Abstract {
        public abstract boolean abstractMethod();

        public abstract String abstractWithArg(String a);
    }

    /** Concrete subclass of Abstract so we can use method references (e.g. AbstractSub::abstractMethod). */
    public static class AbstractSub extends Abstract {
        @Override
        public boolean abstractMethod() {
            return true;
        }

        @Override
        public String abstractWithArg(String a) {
            return a;
        }
    }

    /** Interface for testing proxy-based method reference resolution. */
    public interface TargetInterface {
        int noArg();

        String withArg(String a);
    }

    /** Second dummy type for "multiple classes" tests. */
    public static class OtherDummy {
        public String noArg() {
            return "other";
        }
    }

    /** Dummy with generic-style method for TypeToken tests. */
    public static class DummyWithGeneric {
        public static Object takeOne(int i) {
            return i;
        }

        public static String takeOneStatic(int i) {
            return String.valueOf(i);
        }
    }
}
