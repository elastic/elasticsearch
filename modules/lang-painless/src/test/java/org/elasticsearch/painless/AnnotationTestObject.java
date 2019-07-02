/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
