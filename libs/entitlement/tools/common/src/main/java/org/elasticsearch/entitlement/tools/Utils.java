/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.tools;

import java.io.IOException;
import java.lang.module.ModuleDescriptor;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class Utils {

    private static final Set<String> EXCLUDED_MODULES = Set.of(
        "java.desktop",
        "jdk.jartool",
        "jdk.jdi",
        "java.security.jgss",
        "jdk.jshell"
    );

    private static Map<String, Set<String>> findModuleExports(FileSystem fs) throws IOException {
        var modulesExports = new HashMap<String, Set<String>>();
        try (var stream = Files.walk(fs.getPath("modules"))) {
            stream.filter(p -> p.getFileName().toString().equals("module-info.class")).forEach(x -> {
                try (var is = Files.newInputStream(x)) {
                    var md = ModuleDescriptor.read(is);
                    modulesExports.put(
                        md.name(),
                        md.exports()
                            .stream()
                            .filter(e -> e.isQualified() == false)
                            .map(ModuleDescriptor.Exports::source)
                            .collect(Collectors.toSet())
                    );
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        }
        return modulesExports;
    }

    public interface JdkModuleConsumer {
        void accept(String moduleName, List<Path> moduleClasses, Set<String> moduleExports);
    }

    public static void walkJdkModules(JdkModuleConsumer c) throws IOException {

        FileSystem fs = FileSystems.getFileSystem(URI.create("jrt:/"));

        var moduleExports = Utils.findModuleExports(fs);

        try (var stream = Files.walk(fs.getPath("modules"))) {
            var modules = stream.filter(x -> x.toString().endsWith(".class"))
                .collect(Collectors.groupingBy(x -> x.subpath(1, 2).toString()));

            for (var kv : modules.entrySet()) {
                var moduleName = kv.getKey();
                if (Utils.EXCLUDED_MODULES.contains(moduleName) == false) {
                    var thisModuleExports = moduleExports.get(moduleName);
                    c.accept(moduleName, kv.getValue(), thisModuleExports);
                }
            }
        }
    }
}
