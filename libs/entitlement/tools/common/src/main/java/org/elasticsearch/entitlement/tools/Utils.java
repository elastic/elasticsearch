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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class Utils {
    private static final FileSystem JRT_FS = FileSystems.getFileSystem(URI.create("jrt:/"));

    // TODO Currently ServerProcessBuilder is using --add-modules=ALL-MODULE-PATH, should this rather
    // reflect below excludes (except for java.desktop which requires a special handling)?
    // internal and incubator modules are also excluded
    private static final Set<String> EXCLUDED_MODULES = Set.of(
        "java.desktop",
        "jdk.jartool",
        "jdk.jdi",
        "java.security.jgss",
        "jdk.jshell",
        "jdk.jcmd",
        "jdk.hotspot.agent",
        "jdk.jfr",
        "jdk.javadoc",
        // "jdk.jpackage", // Do we want to include this?
        // "jdk.jlink", // Do we want to include this?
        "jdk.localedata" // noise, change here are not interesting
    );

    public static final Predicate<String> DEFAULT_MODULE_PREDICATE = m -> EXCLUDED_MODULES.contains(m) == false
        && m.contains(".internal.") == false
        && m.contains(".incubator.") == false;

    public static final Predicate<String> modulePredicate(boolean includeIncubator) {
        return includeIncubator == false ? DEFAULT_MODULE_PREDICATE : DEFAULT_MODULE_PREDICATE.or(m -> m.contains(".incubator."));
    }

    public static Map<String, Set<String>> loadExportsByModule() throws IOException {
        var modulesExports = new HashMap<String, Set<String>>();
        try (var stream = Files.walk(JRT_FS.getPath("modules"))) {
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
        return Collections.unmodifiableMap(modulesExports);
    }

    public static Map<String, String> loadClassToModuleMapping() throws IOException {
        Map<String, String> moduleNameByClass = new HashMap<>();
        Utils.walkJdkModules(m -> true, Collections.emptyMap(), (moduleName, moduleClasses, moduleExports) -> {
            for (var classFile : moduleClasses) {
                String prev = moduleNameByClass.put(internalClassName(classFile, moduleName), moduleName);
                if (prev != null) {
                    throw new IllegalStateException("Class " + classFile + " is in both modules " + prev + " and " + moduleName);
                }
            }
        });
        return Collections.unmodifiableMap(moduleNameByClass);
    }

    public interface JdkModuleConsumer {
        void accept(String moduleName, List<Path> moduleClasses, Set<String> moduleExports);
    }

    public static void walkJdkModules(JdkModuleConsumer c) throws IOException {
        walkJdkModules(DEFAULT_MODULE_PREDICATE, Utils.loadExportsByModule(), c);
    }

    public static void walkJdkModules(Predicate<String> modulePredicate, Map<String, Set<String>> exportsByModule, JdkModuleConsumer c)
        throws IOException {
        try (var stream = Files.walk(JRT_FS.getPath("modules"))) {
            var modules = stream.filter(
                x -> x.toString().endsWith(".class") && x.getFileName().toString().equals("module-info.class") == false
            ).collect(Collectors.groupingBy(x -> x.subpath(1, 2).toString()));

            for (var kv : modules.entrySet()) {
                var moduleName = kv.getKey();
                if (modulePredicate.test(moduleName)) {
                    var thisModuleExports = exportsByModule.getOrDefault(moduleName, Collections.emptySet());
                    c.accept(moduleName, kv.getValue(), thisModuleExports);
                }
            }
        }
    }

    public static String internalClassName(Path clazz, String moduleName) {
        Path modulePath = clazz.getFileSystem().getPath("modules", moduleName);
        String relativePath = modulePath.relativize(clazz).toString();
        return relativePath.substring(0, relativePath.length() - ".class".length());
    }
}
