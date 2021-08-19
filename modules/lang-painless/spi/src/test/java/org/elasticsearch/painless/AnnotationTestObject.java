/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless;

import org.elasticsearch.painless.spi.annotation.WhitelistAnnotationParser;

import java.util.Map;

public class AnnotationTestObject {

    public static class TestAnnotation {

        public static final String NAME = "test_annotation";

        private final String one;
        private final String two;
        private final String three;

        public TestAnnotation(String one, String two, String three) {
            this.one = one;
            this.two = two;
            this.three = three;
        }

        public String getOne() {
            return one;
        }

        public String getTwo() {
            return two;
        }

        public String getThree() {
            return three;
        }
    }

    public static class TestAnnotationParser implements WhitelistAnnotationParser {

        public static final TestAnnotationParser INSTANCE = new TestAnnotationParser();

        private TestAnnotationParser() {

        }

        @Override
        public Object parse(Map<String, String> arguments) {
            if (arguments.size() != 3) {
                throw new IllegalArgumentException("expected three arguments");
            }

            String one = arguments.get("one");

            if (one == null) {
                throw new IllegalArgumentException("missing one");
            }

            String two = arguments.get("two");

            if (two == null) {
                throw new IllegalArgumentException("missing two");
            }

            String three = arguments.get("three");

            if (three == null) {
                throw new IllegalArgumentException("missing three");
            }

            return new TestAnnotation(one, two, three);
        }
    }

    public void deprecatedMethod() {

    }

    public void annotatedTestMethod() {

    }

    public void annotatedMultipleMethod() {

    }
}
