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

import com.google.common.base.Joiner;
import com.google.common.collect.Sets;

import junit.framework.TestCase;

import org.apache.lucene.util.LuceneTestCase;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.elasticsearch.test.ElasticsearchTokenStreamTestCase;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.URISyntaxException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.HashSet;
import java.util.Set;

/**
 * Simple class that ensures that all subclasses concrete of ElasticsearchTestCase end with either Test | Tests
 */
public class NamingConventionTests extends ElasticsearchTestCase {

    // see https://github.com/elasticsearch/elasticsearch/issues/9945
    public void testNamingConventions()
            throws ClassNotFoundException, IOException, URISyntaxException {
        final Set<Class> notImplementing = new HashSet<>();
        final Set<Class> pureUnitTest = new HashSet<>();
        final Set<Class> missingSuffix = new HashSet<>();
        String[] packages = {"org.elasticsearch", "org.apache.lucene"};
        for (final String packageName : packages) {
            final String path = "/" + packageName.replace('.', '/');
            final Path startPath = getDataPath(path);
            final Set<Path> ignore = Sets.newHashSet(PathUtils.get("/org/elasticsearch/stresstest"), PathUtils.get("/org/elasticsearch/benchmark/stress"));
            Files.walkFileTree(startPath, new FileVisitor<Path>() {
                private Path pkgPrefix = PathUtils.get(path).getParent();
                @Override
                public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                    Path next =  pkgPrefix.resolve(dir.getFileName());
                    if (ignore.contains(next)) {
                        return FileVisitResult.SKIP_SUBTREE;
                    }
                    pkgPrefix = next;
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    try {
                        String filename = file.getFileName().toString();
                        if (filename.endsWith(".class")) {
                            Class<?> clazz = loadClass(filename);
                            if (Modifier.isAbstract(clazz.getModifiers()) == false && Modifier.isInterface(clazz.getModifiers()) == false) {
                                if ((clazz.getName().endsWith("Tests") || clazz.getName().endsWith("Test"))) { // don't worry about the ones that match the pattern
                                    if (isTestCase(clazz) == false) {
                                        notImplementing.add(clazz);
                                    }
                                } else if (isTestCase(clazz)) {
                                    missingSuffix.add(clazz);
                                } else if (junit.framework.Test.class.isAssignableFrom(clazz) || hasTestAnnotation(clazz)) {
                                    pureUnitTest.add(clazz);
                                }
                            }

                        }
                    } catch (ClassNotFoundException e) {
                        throw new RuntimeException(e);
                    }
                    return FileVisitResult.CONTINUE;
                }

                private boolean hasTestAnnotation(Class<?> clazz) {
                    for (Method method : clazz.getDeclaredMethods()) {
                        if (method.getAnnotation(Test.class) != null) {
                            return true;
                        }
                    }
                    return false;

                }

                private boolean isTestCase(Class<?> clazz) {
                    return ElasticsearchTestCase.class.isAssignableFrom(clazz) || ElasticsearchTestCase.class.isAssignableFrom(clazz) || ElasticsearchTokenStreamTestCase.class.isAssignableFrom(clazz) || LuceneTestCase.class.isAssignableFrom(clazz);
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
        assertTrue(notImplementing.remove(NotImplementingTests.class));
        assertTrue(notImplementing.remove(NotImplementingTest.class));
        assertTrue(pureUnitTest.remove(PlainUnit.class));
        assertTrue(pureUnitTest.remove(PlainUnitTheSecond.class));

        String classesToSubclass = Joiner.on(',').join(
                ElasticsearchTestCase.class.getSimpleName(),
                ElasticsearchTestCase.class.getSimpleName(),
                ElasticsearchTokenStreamTestCase.class.getSimpleName(),
                LuceneTestCase.class.getSimpleName());
        assertTrue("Not all subclasses of " + ElasticsearchTestCase.class.getSimpleName() +
                        " match the naming convention. Concrete classes must end with [Test|Tests]: " + missingSuffix.toString(),
                missingSuffix.isEmpty());
        assertTrue("Pure Unit-Test found must subclass one of [" + classesToSubclass +"] " + pureUnitTest.toString(),
                pureUnitTest.isEmpty());
        assertTrue("Classes ending with Test|Tests] must subclass [" + classesToSubclass +"] " + notImplementing.toString(),
                notImplementing.isEmpty());
    }

    /*
     * Some test the test classes
     */

    @Ignore
    public static final class NotImplementingTests {}
    @Ignore
    public static final class NotImplementingTest {}

    public static final class WrongName extends ElasticsearchTestCase {}

    public static final class WrongNameTheSecond extends ElasticsearchTestCase {}

    public static final class PlainUnit extends TestCase {}

    public static final class PlainUnitTheSecond {
        @Test
        public void foo() {
        }
    }

}
