/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.precommit.transport;

import org.objectweb.asm.ClassReader;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ClassReaders {
    private static final String MODULE_INFO = "module-info.class";

    public static void forEachClassesInPath(String path, Consumer<ClassReader> consumer) {
        try (var stream = Files.walk(Paths.get(path))) {
             stream.filter(p -> p.toString().endsWith(".class"))
                .filter(p -> p.toString().endsWith(MODULE_INFO) == false)
                .filter(p -> p.toString().startsWith("/META-INF") == false)// skip multi-release files
                .forEach(p -> {

                    try (InputStream is = Files.newInputStream(p)) {
                        byte[] classBytes = is.readAllBytes();
                        consumer.accept(new ClassReader(classBytes));
                    } catch (IOException ex) {
                        throw new UncheckedIOException(ex);
                    }
                });
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

}
