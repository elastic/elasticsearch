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

package org.elasticsearch.test;

import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.RestTestCandidate;

import junit.framework.TestCase;

/**
 * Tests for {@linkplain NamingConventionsCheck}. It has a self test mode that covers most cases but this is quicker to run and easier to
 * debug.
 */
public class NamingConventionsCheckTests extends ESTestCase {
    private NamingConventionsCheck check(Class<?> clazz) {
        return check(getRandom().nextBoolean(), clazz);
    }

    private NamingConventionsCheck check(boolean esIntegTestsCanBeUnitTests, Class<?> clazz) {
        NamingConventionsCheck check = new NamingConventionsCheck(esIntegTestsCanBeUnitTests);
        check.check(clazz);
        return check;
    }

    public void testNotRunnableTestsIsNotRunnable() {
        assertTrue(check(NonRunnableTests.class).notRunnable.contains(NonRunnableTests.class));
    }

    public static abstract class NonRunnableTests extends ESTestCase {
    }

    public void testNotRunnableITIsNotRunnable() {
        assertTrue(check(NonRunnableIT.class).notRunnable.contains(NonRunnableIT.class));
    }

    public static abstract class NonRunnableIT extends ESTestCase {
    }

    public void testInterfaceTestsIsNotRunnable() {
        assertTrue(check(InterfaceTests.class).notRunnable.contains(InterfaceTests.class));
    }

    public interface InterfaceTests {
    }

    public void testInterfaceITIsNotRunnable() {
        assertTrue(check(InterfaceIT.class).notRunnable.contains(InterfaceIT.class));
    }

    public interface InterfaceIT {
    }

    public void testInnerTestsIsInnerClass() {
        assertTrue(check(InnerTests.class).innerClasses.contains(InnerTests.class));
    }

    public static final class InnerTests extends ESTestCase {
    }

    public void testInnerTIIsInnerClass() {
        assertTrue(check(InnerIT.class).innerClasses.contains(InnerIT.class));
    }

    public static final class InnerIT extends ESExternalDepsTestCase {
    }

    public void testNothingNamedLikeTestsNamedLikeUnitButNotUnit() {
        assertTrue(check(NothingNamedLikeTests.class).namedLikeUnitButNotUnit.contains(NothingNamedLikeTests.class));
    }

    public static final class NothingNamedLikeTests {
    }

    public void testNothingNamedLikeITNamedLikeIntegButNotInteg() {
        assertTrue(check(NothingNamedLikeIT.class).namedLikeIntegButNotInteg.contains(NothingNamedLikeIT.class));
    }

    public static final class NothingNamedLikeIT {
    }

    public void testPlainUnitsArePlainUnit() {
        assertTrue(check(PlainUnit.class).plainUnit.contains(PlainUnit.class));
        assertTrue(check(PlainUnitTests.class).plainUnit.contains(PlainUnitTests.class));
        assertTrue(check(PlainUnitTests.class).plainUnit.contains(PlainUnitTests.class));
    }

    public static final class PlainUnit extends TestCase {
    }

    public static final class PlainUnitTests extends TestCase {
    }

    public static final class PlainUnitIT extends TestCase {
    }

    public void testMissingUnitTestSuffixIsMissingUnitTestSuffix() {
        assertTrue(check(MissingUnitTestSuffix.class).missingUnitTestSuffix.contains(MissingUnitTestSuffix.class));
    }

    public static final class MissingUnitTestSuffix extends ESTestCase {
    }

    public void testNamedLikeIntegButUnitITIsNamedLikeIntegButNotInteg() {
        assertTrue(check(NamedLikeIntegButUnitIT.class).namedLikeIntegButNotInteg.contains(NamedLikeIntegButUnitIT.class));
    }

    public static final class NamedLikeIntegButUnitIT extends ESTestCase {
    }

    public void testMissingIntegTestSuffixIsMissingIntegTestSuffix() {
        assertTrue(check(MissingIntegTestSuffix.class).missingIntegTestSuffix.contains(MissingIntegTestSuffix.class));
    }

    public static final class MissingIntegTestSuffix extends ESIntegTestCase {
    }

    public void testNamedLikeUnitButIntegTestsIsNamedLikeUnitButNotUnit() {
        assertTrue(check(false, NamedLikeUnitButIntegTests.class).namedLikeUnitButNotUnit.contains(NamedLikeUnitButIntegTests.class));
        assertFalse(check(true, NamedLikeUnitButIntegTests.class).namedLikeUnitButNotUnit.contains(NamedLikeUnitButIntegTests.class));
    }

    public static final class NamedLikeUnitButIntegTests extends ESIntegTestCase {
    }

    public void testMissingRestTestSuffixIsMissingIntegTestSuffix() {
        assertTrue(check(MissingRestTestSuffix.class).missingIntegTestSuffix.contains(MissingRestTestSuffix.class));
    }

    public static final class MissingRestTestSuffix extends ESRestTestCase {
        public MissingRestTestSuffix(RestTestCandidate testCandidate) {
            super(testCandidate);
        }
    }

    public void testMissingClientTestSuffixIsMissingIntegTestSuffix() {
        assertTrue(check(MissingClientTestSuffix.class).missingIntegTestSuffix.contains(MissingClientTestSuffix.class));
    }

    public static final class MissingClientTestSuffix extends ESExternalDepsTestCase {
    }

    public void testNamedLikeUnitButRestTestsIsNamedLikeUnitButNotUnit() {
        assertTrue(check(NamedLikeUnitButRestTests.class).namedLikeUnitButNotUnit.contains(NamedLikeUnitButRestTests.class));
    }

    public static final class NamedLikeUnitButRestTests extends ESRestTestCase {
        public NamedLikeUnitButRestTests(RestTestCandidate testCandidate) {
            super(testCandidate);
        }
    }

    public void testNamedLikeUnitButClientTestsIsNamedLikeUnitButNotUnit() {
        assertTrue(
                check(NamedLikeUnitButExternalDepsTests.class).namedLikeUnitButNotUnit.contains(NamedLikeUnitButExternalDepsTests.class));
    }

    public static final class NamedLikeUnitButExternalDepsTests extends ESExternalDepsTestCase {
    }
}
