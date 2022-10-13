/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.jdk;

import org.elasticsearch.core.SuppressForbidden;

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

    private JdkJarHellCheck() {}

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
