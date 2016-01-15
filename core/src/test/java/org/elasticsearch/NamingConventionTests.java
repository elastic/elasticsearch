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
package org.elasticsearch;

import junit.framework.TestCase;
import org.apache.lucene.util.LuceneTestCase;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.ESTokenStreamTestCase;

import java.io.IOException;
import java.lang.reflect.Modifier;
import java.net.URISyntaxException;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Simple class that ensures that all subclasses concrete of ESTestCase end with either Test | Tests
 */
public class NamingConventionTests extends ESTestCase {

    // see https://github.com/elasticsearch/elasticsearch/issues/9945
    public void testNamingConventions()
            throws ClassNotFoundException, IOException, URISyntaxException {
        final Set<Class> notImplementing = new HashSet<>();
        final Set<Class> pureUnitTest = new HashSet<>();
        final Set<Class> missingSuffix = new HashSet<>();
        final Set<Class> integTestsInDisguise = new HashSet<>();
        final Set<Class> notRunnable = new HashSet<>();
        final Set<Class> innerClasses = new HashSet<>();
        String[] packages = {"org.elasticsearch", "org.apache.lucene"};
        for (final String packageName : packages) {
            final String path = "/" + packageName.replace('.', '/');
            final Path startPath = getDataPath(path);
            Files.walkFileTree(startPath, new FileVisitor<Path>() {
                private Path pkgPrefix = PathUtils.get(path).getParent();
                @Override
                public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                    pkgPrefix = pkgPrefix.resolve(dir.getFileName());
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    try {
                        String filename = file.getFileName().toString();
                        if (filename.endsWith(".class")) {
                            Class<?> clazz = loadClass(filename);
                            if (clazz.getName().endsWith("Tests")) { // don't worry about the ones that match the pattern

                                if (ESIntegTestCase.class.isAssignableFrom(clazz)) {
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
                                // otherwise fine
                            } else if (Modifier.isAbstract(clazz.getModifiers()) == false && Modifier.isInterface(clazz.getModifiers()) == false) {
                                if (isTestCase(clazz)) {
                                    missingSuffix.add(clazz);
                                } else if (junit.framework.Test.class.isAssignableFrom(clazz)) {
                                    pureUnitTest.add(clazz);
                                }
                            }
                        }
                    } catch (ClassNotFoundException e) {
                        throw new RuntimeException(e);
                    }
                    return FileVisitResult.CONTINUE;
                }

                private boolean isTestCase(Class<?> clazz) {
                    return LuceneTestCase.class.isAssignableFrom(clazz);
                }

                private Class<?> loadClass(String filename) throws ClassNotFoundException {
                    StringBuilder pkg = new StringBuilder();
                    for (Path p : pkgPrefix) {
                        pkg.append(p.getFileName().toString()).append(".");
                    }
                    pkg.append(filename.substring(0, filename.length() - 6));
                    return Thread.currentThread().getContextClassLoader().loadClass(pkg.toString());
                }

                @Override
                public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
                    throw exc;
                }

                @Override
                public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                    pkgPrefix = pkgPrefix.getParent();
                    return FileVisitResult.CONTINUE;
                }
            });

        }
        assertTrue(missingSuffix.remove(WrongName.class));
        assertTrue(missingSuffix.remove(WrongNameTheSecond.class));
        assertTrue(notRunnable.remove(DummyAbstractTests.class));
        assertTrue(notRunnable.remove(DummyInterfaceTests.class));
        assertTrue(innerClasses.remove(InnerTests.class));
        assertTrue(notImplementing.remove(NotImplementingTests.class));
        assertTrue(pureUnitTest.remove(PlainUnit.class));

        String classesToSubclass = String.join(
                ",",
                ESTestCase.class.getSimpleName(),
                ESTestCase.class.getSimpleName(),
                ESTokenStreamTestCase.class.getSimpleName(),
                LuceneTestCase.class.getSimpleName()
        );
        assertNoViolations("Not all subclasses of " + ESTestCase.class.getSimpleName() + " match the naming convention. Concrete classes must end with [Tests]:\n", missingSuffix);
        assertNoViolations("Classes ending with [Tests] are abstract or interfaces:\n", notRunnable);
        assertNoViolations("Found inner classes that are tests, which are excluded from the test runner:\n", innerClasses);
        assertNoViolations("Pure Unit-Test found must subclass one of [" + classesToSubclass + "]:\n", pureUnitTest);
        assertNoViolations("Classes ending with [Tests] must subclass [" + classesToSubclass + "]:\n", notImplementing);
        assertNoViolations("Subclasses of ESIntegTestCase should end with IT as they are integration tests:\n", integTestsInDisguise);
    }

    private String join(Set<Class> set) {
        return set.stream().map(Object::toString).collect(Collectors.joining("\n"));
    }

    private void assertNoViolations(String message, Set<Class> set) {
        assertTrue(message + join(set), set.isEmpty());
    }

    /*
     * Some test the test classes
     */

    public static final class NotImplementingTests {}

    public static final class WrongName extends ESTestCase {}

    public static abstract class DummyAbstractTests extends ESTestCase {}

    public interface DummyInterfaceTests {}

    public static final class InnerTests extends ESTestCase {}

    public static final class WrongNameTheSecond extends ESTestCase {}

    public static final class PlainUnit extends TestCase {}
}
