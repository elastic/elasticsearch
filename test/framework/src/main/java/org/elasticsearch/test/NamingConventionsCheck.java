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

import java.io.IOException;
import java.lang.reflect.Modifier;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.HashSet;
import java.util.Set;

import org.apache.lucene.util.LuceneTestCase;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.test.rest.ESRestTestCase;

import junit.framework.TestCase;

/**
 * Checks that all tests in a directory are named according to our naming conventions. This is important because tests that do not follow
 * our conventions aren't run by gradle. This was once a glorious unit test but now that Elasticsearch is a multi-module project it must be
 * a class with a main method so gradle can call it for each project. This has the advantage of allowing gradle to calculate when it is
 * {@code UP-TO-DATE} so it can be skipped if the compiled classes haven't changed. This is useful on large modules for which checking all
 * the modules can be slow.
 *
 * Annoyingly, this cannot be tested using standard unit tests because to do so you'd have to declare classes that violate the rules. That
 * would cause the test fail which would prevent the build from passing. So we have to make a mechanism for removing those test classes. Now
 * that we have such a mechanism it isn't much work to fail the process if we don't detect the offending classes. Thus, the funky
 * {@code --self-test} that is only run in the test:framework project.
 */
public class NamingConventionsCheck {
    public static void main(String[] args) throws IOException, ClassNotFoundException {
        boolean esIntegTestsCanBeUnitTests = false;
        boolean selfTest = false;
        int i = 0;
        while (true) {
            switch (args[i]) {
            case "--es-integ-tests-can-be-tests":
                esIntegTestsCanBeUnitTests = true;
                i++;
                continue;
            case "--self-test":
                selfTest = true;
                i++;
                continue;
            case "--":
                i++;
                break;
            default:
                throw new RuntimeException("Expected -- before a path.");
            }
            break;
        }
        NamingConventionsCheck check = new NamingConventionsCheck(esIntegTestsCanBeUnitTests);
        check.check(PathUtils.get(args[i]));

        // If we're in self test mode we need to have some violations to prove that it worked
        if (selfTest) {
            assertViolation("NonRunnableTests", check.notRunnable);
            assertViolation("NonRunnableIT", check.notRunnable);
            assertViolation("InterfaceTests", check.notRunnable);
            assertViolation("InterfaceIT", check.notRunnable);
            assertViolation("InnerTests", check.innerClasses);
            assertViolation("InnerIT", check.innerClasses);
            assertViolation("NothingNamedLikeTests", check.namedLikeUnitButNotUnit);
            assertViolation("NothingNamedLikeIT", check.namedLikeIntegButNotInteg);
            assertViolation("PlainUnit", check.plainUnit);
            assertViolation("PlainUnitTests", check.plainUnit);
            assertViolation("PlainUnitIT", check.plainUnit);
            assertViolation("MissingUnitTestSuffix", check.missingUnitTestSuffix);
            assertViolation("NamedLikeIntegButUnitIT", check.namedLikeIntegButNotInteg);
            assertViolation("MissingIntegTestSuffix", check.missingIntegTestSuffix);
            assertViolation("NamedLikeUnitButIntegTests", check.namedLikeUnitButNotUnit);
            assertViolation("MissingRestTestSuffix", check.missingIntegTestSuffix);
            assertViolation("MissingClientTestSuffix", check.missingIntegTestSuffix);
            assertViolation("NamedLikeUnitButRestTests", check.namedLikeUnitButNotUnit);
            assertViolation("NamedLikeUnitButClientTests", check.namedLikeUnitButNotUnit);
        }

        // Now we should have no violations
        assertNoViolations("Classes ending with [Tests] or [IT] are abstract or interfaces", check.notRunnable);
        assertNoViolations("Found inner classes that are tests, which are excluded from the test runner", check.innerClasses);
        assertNoViolations("Classes ending with [Tests] that aren't unit tests", check.namedLikeUnitButNotUnit);
        assertNoViolations("Classes ending with [IT] that aren't integration tests", check.namedLikeIntegButNotInteg);
        assertNoViolations("Unit tests that don't ends with [Tests]", check.missingUnitTestSuffix);
        assertNoViolations("Integration tests that don't ends with [IT]", check.missingIntegTestSuffix);

        String classesToSubclass = String.join(",", ESTestCase.class.getSimpleName(), ESIntegTestCase.class.getSimpleName(),
                ESRestTestCase.class.getSimpleName(), ESTokenStreamTestCase.class.getSimpleName(), LuceneTestCase.class.getSimpleName());
        assertNoViolations("Plain Unit-Test found must subclass one of [" + classesToSubclass + "]", check.plainUnit);
    }

    final Set<Class<?>> notRunnable = new HashSet<>();
    final Set<Class<?>> innerClasses = new HashSet<>();
    final Set<Class<?>> plainUnit = new HashSet<>();
    final Set<Class<?>> missingUnitTestSuffix = new HashSet<>();
    final Set<Class<?>> missingIntegTestSuffix = new HashSet<>();
    final Set<Class<?>> namedLikeUnitButNotUnit = new HashSet<>();
    final Set<Class<?>> namedLikeIntegButNotInteg = new HashSet<>();

    private final boolean esIntegTestsCanBeUnitTests;
    
    public NamingConventionsCheck(boolean esIntegTestsCanBeUnitTests) {
        this.esIntegTestsCanBeUnitTests = esIntegTestsCanBeUnitTests;
    }

    public void check(Path rootPath) throws IOException {
        Files.walkFileTree(rootPath, new FileVisitor<Path>() {
            /**
             * The package name of the directory we are currently visiting. Kept as a string rather than something fancy because we load
             * just about every class and doing so requires building a string out of it anyway. At least this way we don't need to build the
             * first part of the string over and over and over again.
             */
            private String packageName;

            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                // First we visit the root directory
                if (packageName == null) {
                    // And it package is empty string regardless of the directory name
                    packageName = "";
                } else {
                    packageName += dir.getFileName() + ".";
                }
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                // Go up one package by jumping back to the second to last '.'
                packageName = packageName.substring(0, 1 + packageName.lastIndexOf('.', packageName.length() - 2));
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                String filename = file.getFileName().toString();
                if (filename.endsWith(".class")) {
                    String className = filename.substring(0, filename.length() - ".class".length());
                    check(loadClass(className));
                }
                return FileVisitResult.CONTINUE;
            }

            private Class<?> loadClass(String className) {
                try {
                    return Thread.currentThread().getContextClassLoader().loadClass(packageName + className);
                } catch (ClassNotFoundException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
                throw exc;
            }
        });
    }

    void check(Class<?> clazz) {
        TestNameType nameType = TestNameType.fromName(clazz.getName());

        if (false == isRunnable(clazz)) {
            if (nameType == TestNameType.NOT_A_TEST) {
                // Non-runnable stuff that isn't named like a test can extend whatever it likes
                return;
            } else {
                // All things named like a test must be runnable, non-inner classes
                notRunnable.add(clazz);
                return;
            }
        }

        TestClassType classType = TestClassType.fromClass(clazz);
        Set<Class<?>> failureGroup = failureGroup(nameType, classType);
        if (failureGroup != null) {
            failureGroup.add(clazz);
            return;
        }
        if (classType != TestClassType.NOT_A_TEST && Modifier.isStatic(clazz.getModifiers())) {
            // Tests that are inner classes won't be picked upl
            innerClasses.add(clazz);
            return;
        }
    }

    private boolean isRunnable(Class<?> clazz) {
        return Modifier.isAbstract(clazz.getModifiers()) == false && Modifier.isInterface(clazz.getModifiers()) == false;
    }

    private Set<Class<?>> failureGroup(TestNameType nameType, TestClassType classType) {
        switch (classType) {
        case NOT_A_TEST:
            switch (nameType) {
            case NOT_A_TEST: return null;
            case UNIT:       return namedLikeUnitButNotUnit;
            case INTEG:      return namedLikeIntegButNotInteg;
            }
            throw new IllegalArgumentException("Unknown nameType for test:  [" + nameType + "]");
        case PLAIN_UNIT:
            return plainUnit;
        case UNIT:
            switch (nameType) {
            case NOT_A_TEST: return missingUnitTestSuffix;
            case UNIT:       return null;
            case INTEG:      return namedLikeIntegButNotInteg;
            }
            throw new IllegalArgumentException("Unknown nameType for test:  [" + nameType + "]");
        case ES_INTEG:
            switch (nameType) {
            case NOT_A_TEST: return missingIntegTestSuffix;
            case UNIT:       return esIntegTestsCanBeUnitTests ? null : namedLikeUnitButNotUnit; 
            case INTEG:      return null;
            }
            throw new IllegalArgumentException("Unknown nameType for test:  [" + nameType + "]");
        case INTEG:
            switch (nameType) {
            case NOT_A_TEST: return missingIntegTestSuffix;
            case UNIT:       return namedLikeUnitButNotUnit;
            case INTEG:      return null;
            }
            throw new IllegalArgumentException("Unknown nameType for test:  [" + nameType + "]");
        }
        throw new IllegalArgumentException("Unknown classType for test:  [" + classType + "]");
    }

    /**
     * Fail the process if there are any violations in the set. Named to look like a junit assertion even though it isn't because it is
     * similar enough.
     */
    private static void assertNoViolations(String message, Set<Class<?>> set) {
        if (false == set.isEmpty()) {
            StringBuilder messageBuilder = new StringBuilder().append(message).append(':');
            for (Class<?> bad : set) {
                messageBuilder.append('\n').append(" * ").append(bad.getName());
            }
            throw new RuntimeException(messageBuilder.toString());
        }
    }

    /**
     * Fail the process if we didn't detect a particular violation. Named to look like a junit assertion even though it isn't because it is
     * similar enough.
     */
    private static void assertViolation(String className, Set<Class<?>> set) throws ClassNotFoundException {
        className = "org.elasticsearch.test.NamingConventionsCheckTests$" + className;
        if (false == set.remove(Class.forName(className))) {
            throw new RuntimeException("Error in NamingConventionsCheck! Expected [" + className + "] to be a violation but wasn't.");
        }
    }

    private enum TestClassType {
        NOT_A_TEST, UNIT, ES_INTEG, INTEG, PLAIN_UNIT;

        public static TestClassType fromClass(Class<?> clazz) {
            if (ESRestTestCase.class.isAssignableFrom(clazz)) {
                return INTEG;
            }
            if (ESClientTestCase.class.isAssignableFrom(clazz)) {
                return INTEG;
            }
            if (ESIntegTestCase.class.isAssignableFrom(clazz)) {
                return ES_INTEG;
            }
            if (LuceneTestCase.class.isAssignableFrom(clazz)) {
                return UNIT;
            }
            if (TestCase.class.isAssignableFrom(clazz)) {
                return PLAIN_UNIT;
            }
            return NOT_A_TEST;
        }
    }
    
    private enum TestNameType {
        NOT_A_TEST, UNIT, INTEG;

        public static TestNameType fromName(String name) {
            if (name.endsWith("Tests")) {
                return UNIT;
            }
            if (name.endsWith("IT")) {
                return INTEG;
            }
            return NOT_A_TEST;
        }
    }
}
