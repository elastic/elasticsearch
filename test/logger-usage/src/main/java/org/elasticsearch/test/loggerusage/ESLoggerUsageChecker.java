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

package org.elasticsearch.test.loggerusage;

import org.apache.logging.log4j.Logger;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.Type;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Predicate;

public class ESLoggerUsageChecker {

    public static final List<String> LOGGER_METHODS = Arrays.asList("trace", "debug", "info", "warn", "error", "fatal");
    public static final String LOGGER_CLASS = Logger.class.getName();
    public static final String IGNORE_CHECKS_ANNOTATION = "org.elasticsearch.common.SuppressLoggerChecks";

    @SuppressForbidden(reason = "command line tool")
    public static void main(String... args) throws Exception {
        System.out.println("Checking for wrong usages of ESLogger...");
        AtomicInteger wrongUsagesCount = new AtomicInteger();
        checkDirectories(wrongLoggerUsage -> {
            System.err.println(wrongLoggerUsage.getErrorLines());
            wrongUsagesCount.incrementAndGet();
        }, args);
        if (wrongUsagesCount.get() > 0) {
            System.out.printf("Found %d wrong logger usages%n", wrongUsagesCount.get());
        } else {
            System.out.println("No wrong usages found");
        }
    }

    private static void checkDirectories(Consumer<WrongLoggerUsage> wrongUsageCallback, String... classDirectories) throws IOException {
        for (String classDirectory : classDirectories) {
            Path root = Paths.get(classDirectory);
            if (!Files.isDirectory(root)) {
                throw new IllegalArgumentException(root + " should be an existing directory");
            }
            Files.walkFileTree(root, new SimpleFileVisitor<>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    if (Files.isRegularFile(file) && file.getFileName().toString().endsWith(".class")) {
                        try (InputStream in = Files.newInputStream(file)) {
                            ESLoggerUsageChecker.check(wrongUsageCallback, in);
                        }
                    }
                    return super.visitFile(file, attrs);
                }
            });
        }
    }

    public static void check(Consumer<WrongLoggerUsage> wrongUsageCallback, InputStream inputStream) throws IOException {
        check(wrongUsageCallback, inputStream, s -> true);
    }

    // used by tests
    static void check(Consumer<WrongLoggerUsage> wrongUsageCallback, InputStream inputStream,
                      Predicate<String> methodsToCheck) throws IOException {
        ClassReader cr = new ClassReader(inputStream);
        cr.accept(new ClassChecker(wrongUsageCallback, methodsToCheck), 0);
    }
}
