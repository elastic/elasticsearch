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

import com.google.common.collect.Sets;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ElasticsearchTestCase;

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

public class ExceptionRegistrationTests extends ElasticsearchTestCase {

    public void testExceptionRegistration()
            throws ClassNotFoundException, IOException, URISyntaxException {
        final Set<Class> notRegistered = new HashSet<>();
        final Set<String> registered = new HashSet<>();
        final String path = "/org/elasticsearch";
        final Path startPath = PathUtils.get(ElasticsearchException.class.getProtectionDomain().getCodeSource().getLocation().toURI()).resolve("org").resolve("elasticsearch");
        final Set<? extends Class> ignore = Sets.newHashSet(
                org.elasticsearch.test.rest.parser.RestTestParseException.class,
                org.elasticsearch.index.query.TestQueryParsingException.class,
                org.elasticsearch.test.rest.client.RestException.class,
                org.elasticsearch.common.util.CancellableThreadsTest.CustomException.class,
                org.elasticsearch.rest.BytesRestResponseTests.WithHeadersException.class,
                org.elasticsearch.client.AbstractClientHeadersTests.InternalException.class);
        FileVisitor<Path> visitor = new FileVisitor<Path>() {
            private Path pkgPrefix = PathUtils.get(path).getParent();

            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                Path next = pkgPrefix.resolve(dir.getFileName());
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
                        if (ignore.contains(clazz) == false) {
                            if (Modifier.isAbstract(clazz.getModifiers()) == false && Modifier.isInterface(clazz.getModifiers()) == false && isEsException(clazz)) {
                                if (ElasticsearchException.MAPPING.containsKey(clazz.getName()) == false && ElasticsearchException.class.equals(clazz.getEnclosingClass()) == false) {
                                    notRegistered.add(clazz);
                                } else if (ElasticsearchException.MAPPING.containsKey(clazz.getName())) {
                                    registered.add(clazz.getName());
                                }
                            }
                        }

                    }
                } catch (ClassNotFoundException e) {
                    throw new RuntimeException(e);
                }
                return FileVisitResult.CONTINUE;
            }

            private boolean isEsException(Class<?> clazz) {
                return ElasticsearchException.class.isAssignableFrom(clazz);
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
        };
        Files.walkFileTree(startPath, visitor);
        final Path testStartPath = PathUtils.get(ExceptionRegistrationTests.class.getResource(path).toURI());
        Files.walkFileTree(testStartPath, visitor);
        assertTrue(notRegistered.remove(TestException.class));
        assertTrue("Classes subclassing ElasticsearchException must be registered \n" + notRegistered.toString(),
                notRegistered.isEmpty());
        assertTrue(registered.removeAll(ElasticsearchException.MAPPING.keySet())); // check
        assertEquals(registered.toString(), 0 ,registered.size());
    }

    public static final class TestException extends ElasticsearchException {
        public TestException(StreamInput in) throws IOException{
            super(in);
        }
    }
}
