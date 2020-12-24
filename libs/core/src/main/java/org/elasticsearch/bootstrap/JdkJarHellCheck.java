/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.bootstrap;

import org.elasticsearch.common.SuppressForbidden;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class JdkJarHellCheck {

    private Set<String> detected = new HashSet<>();

    private void scanForJDKJarHell(Path root) throws IOException {
        // system.parent = extensions loader.
        // note: for jigsaw, this evilness will need modifications (e.g. use jrt filesystem!)
        ClassLoader ext = ClassLoader.getSystemClassLoader().getParent();
        assert ext != null;

        Files.walkFileTree(root, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                String entry = root.relativize(file).toString().replace('\\', '/');
                if (entry.endsWith(".class") && entry.endsWith("module-info.class") == false) {
                    if (ext.getResource(entry) != null) {
                        detected.add(entry.replace("/", ".").replace(".class", ""));
                    }
                }
                return FileVisitResult.CONTINUE;
            }
        });
    }

    public Set<String> getDetected() {
        return Collections.unmodifiableSet(detected);
    }

    @SuppressForbidden(reason = "command line tool")
    public static void main(String[] argv) throws IOException {
        JdkJarHellCheck checker = new JdkJarHellCheck();
        for (String location : argv) {
            Path path = Paths.get(location);
            if (Files.exists(path) == false) {
                throw new IllegalArgumentException("Path does not exist: " + path);
            }
            checker.scanForJDKJarHell(path);
        }
        if (checker.getDetected().isEmpty()) {
            System.exit(0);
        } else {
            checker.getDetected().forEach(System.out::println);
            System.exit(1);
        }
    }

}
