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
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.HashSet;
import java.util.Set;

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
    public static void main(String[] args) throws IOException {
        int i = 0;
        NamingConventionsCheck check = new NamingConventionsCheck(
                loadClassWithoutInitializing(args[i++]),
                loadClassWithoutInitializing(args[i++]));
        boolean skipIntegTestsInDisguise = false;
        boolean selfTest = false;
        while (true) {
            switch (args[i]) {
            case "--skip-integ-tests-in-disguise":
                skipIntegTestsInDisguise = true;
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
                fail("Expected -- before a path.");
            }
            break;
        }
        check.check(Paths.get(args[i]));

        if (selfTest) {
            assertViolation("WrongName", check.missingSuffix);
            assertViolation("WrongNameTheSecond", check.missingSuffix);
            assertViolation("DummyAbstractTests", check.notRunnable);
            assertViolation("DummyInterfaceTests", check.notRunnable);
            assertViolation("InnerTests", check.innerClasses);
            assertViolation("NotImplementingTests", check.notImplementing);
            assertViolation("PlainUnit", check.pureUnitTest);
        }

        // Now we should have no violations
        assertNoViolations("Not all subclasses of " + check.testClass.getSimpleName()
                + " match the naming convention. Concrete classes must end with [Tests]", check.missingSuffix);
        assertNoViolations("Classes ending with [Tests] are abstract or interfaces", check.notRunnable);
        assertNoViolations("Found inner classes that are tests, which are excluded from the test runner", check.innerClasses);
        assertNoViolations("Pure Unit-Test found must subclass [" + check.testClass.getSimpleName() + "]", check.pureUnitTest);
        assertNoViolations("Classes ending with [Tests] must subclass [" + check.testClass.getSimpleName() + "]", check.notImplementing);
        if (!skipIntegTestsInDisguise) {
            assertNoViolations("Subclasses of ESIntegTestCase should end with IT as they are integration tests",
                    check.integTestsInDisguise);
        }
    }

    private final Set<Class<?>> notImplementing = new HashSet<>();
    private final Set<Class<?>> pureUnitTest = new HashSet<>();
    private final Set<Class<?>> missingSuffix = new HashSet<>();
    private final Set<Class<?>> integTestsInDisguise = new HashSet<>();
    private final Set<Class<?>> notRunnable = new HashSet<>();
    private final Set<Class<?>> innerClasses = new HashSet<>();

    private final Class<?> testClass;
    private final Class<?> integTestClass;

    public NamingConventionsCheck(Class<?> testClass, Class<?> integTestClass) {
        this.testClass = testClass;
        this.integTestClass = integTestClass;
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
                    Class<?> clazz = loadClassWithoutInitializing(packageName + className);
                    if (clazz.getName().endsWith("Tests")) {
                        if (integTestClass.isAssignableFrom(clazz)) {
                            integTestsInDisguise.add(clazz);
                        }
                        if (Modifier.isAbstract(clazz.getModifiers()) || Modifier.isInterface(clazz.getModifiers())) {
                            notRunnable.add(clazz);
                        } else if (isTestCase(clazz) == false) {
                            notImplementing.add(clazz);
                        } else if (Modifier.isStatic(clazz.getModifiers())) {
                            innerClasses.add(clazz);
                        }
                    } else if (clazz.getName().endsWith("IT")) {
                        if (isTestCase(clazz) == false) {
                            notImplementing.add(clazz);
                        }
                    } else if (Modifier.isAbstract(clazz.getModifiers()) == false && Modifier.isInterface(clazz.getModifiers()) == false) {
                        if (isTestCase(clazz)) {
                            missingSuffix.add(clazz);
                        } else if (junit.framework.Test.class.isAssignableFrom(clazz)) {
                            pureUnitTest.add(clazz);
                        }
                    }
                }
                return FileVisitResult.CONTINUE;
            }

            private boolean isTestCase(Class<?> clazz) {
                return testClass.isAssignableFrom(clazz);
            }

            @Override
            public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
                throw exc;
            }
        });
    }

    /**
     * Fail the process if there are any violations in the set. Named to look like a junit assertion even though it isn't because it is
     * similar enough.
     */
    private static void assertNoViolations(String message, Set<Class<?>> set) {
        if (false == set.isEmpty()) {
            System.err.println(message + ":");
            for (Class<?> bad : set) {
                System.err.println(" * " + bad.getName());
            }
            System.exit(1);
        }
    }

    /**
     * Fail the process if we didn't detect a particular violation. Named to look like a junit assertion even though it isn't because it is
     * similar enough.
     */
    private static void assertViolation(String className, Set<Class<?>> set) {
        className = "org.elasticsearch.test.NamingConventionsCheckBadClasses$" + className;
        if (false == set.remove(loadClassWithoutInitializing(className))) {
            System.err.println("Error in NamingConventionsCheck! Expected [" + className + "] to be a violation but wasn't.");
            System.exit(1);
        }
    }

    /**
     * Fail the process with the provided message.
     */
    private static void fail(String reason) {
        System.err.println(reason);
        System.exit(1);
    }

    static Class<?> loadClassWithoutInitializing(String name) {
        try {
            return Class.forName(name,
                    // Don't initialize the class to save time. Not needed for this test and this doesn't share a VM with any other tests.
                    false,
                    // Use our classloader rather than the bootstrap class loader.
                    NamingConventionsCheck.class.getClassLoader());
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }
}
