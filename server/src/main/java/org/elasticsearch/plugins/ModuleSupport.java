/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins;

import org.elasticsearch.core.SuppressForbidden;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.module.InvalidModuleDescriptorException;
import java.lang.module.ModuleDescriptor;
import java.lang.module.ModuleFinder;
import java.lang.module.ModuleReader;
import java.lang.module.ModuleReference;
import java.net.URI;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.stream.Collectors;
import java.util.zip.ZipFile;

public class ModuleSupport {

    static ModuleFinder ofSyntheticPluginModule(String name, Path[] jarPaths, Set<String> requires) {
        try {
            return new InMemoryModuleFinder(
                new InMemoryModuleReference(createModuleDescriptor(name, jarPaths, requires), URI.create("module:/" + name))
            );
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @SuppressForbidden(reason = "need access to the jar file")
    static ModuleDescriptor createModuleDescriptor(String name, Path[] jarPaths, Set<String> requires) throws IOException {
        var builder = ModuleDescriptor.newOpenModule(name); // open module, for now
        requires.stream().forEach(builder::requires);

        // scan the names of the entries in the JARs
        Set<String> pkgs = new HashSet<>();
        for (Path path : jarPaths) {
            assert path.getFileName().toString().endsWith(".jar") : "expected jars suffix, in path: " + path;
            try (JarFile jf = new JarFile(path.toFile(), true, ZipFile.OPEN_READ, Runtime.version())) {
                // separator = path.getFileSystem().getSeparator();
                var scan = scan(jf);
                scan.classFiles().stream().map(cf -> toPackageName(cf, "/")).flatMap(Optional::stream).forEach(pkgs::add);
            }
        }
        builder.packages(pkgs);
        // TODO: provides and uses
        return builder.build();
    }

    static class InMemoryModuleFinder implements ModuleFinder {
        private final ModuleReference mref;
        private final String mn;

        private InMemoryModuleFinder(ModuleReference mref) {
            this.mref = mref;
            this.mn = mref.descriptor().name();
        }

        @Override
        public Optional<ModuleReference> find(String name) {
            Objects.requireNonNull(name);
            return Optional.ofNullable(mn.equals(name) ? mref : null);
        }

        @Override
        public Set<ModuleReference> findAll() {
            return Set.of(mref);
        }
    }

    static class InMemoryModuleReference extends ModuleReference {
        InMemoryModuleReference(ModuleDescriptor descriptor, URI location) {
            super(descriptor, location);
        }

        @Override
        public ModuleReader open() {
            throw new UnsupportedOperationException();
        }
    }

    private static final String SERVICES_PREFIX = "META-INF/services/";

    private static final String MODULE_INFO = "module-info.class";

    // A scan result, which aggregates class and services files.
    record ScanResult(Set<String> classFiles, Set<String> serviceFiles) {}

    @SuppressForbidden(reason = "need access to the jar file")
    static ScanResult scan(JarFile jarFile) {
        Map<Boolean, Set<String>> map = jarFile.versionedStream()
            .filter(e -> e.isDirectory() == false)
            .map(JarEntry::getName)
            .filter(e -> (e.endsWith(".class") ^ e.startsWith(SERVICES_PREFIX)))
            .collect(Collectors.partitioningBy(e -> e.startsWith(SERVICES_PREFIX), Collectors.toSet()));

        return new ScanResult(map.get(Boolean.FALSE), map.get(Boolean.TRUE));
    }

    // Returns an optional containing the package name from a given binary class path name, or an
    // empty optional if none.
    static Optional<String> toPackageName(String name, String separator) {
        assert name.endsWith(separator) == false;
        int index = name.lastIndexOf(separator);
        if (index == -1) {
            if (name.endsWith(".class") && name.equals(MODULE_INFO) == false) {
                String msg = name + " found in top-level directory (unnamed package not allowed in module)";
                throw new InvalidModuleDescriptorException(msg);
            }
            return Optional.empty();
        }

        String pn = name.substring(0, index).replace(separator, ".");
        if (isPackageName(pn)) {
            return Optional.of(pn);
        } else {
            // not a valid package name
            return Optional.empty();
        }
    }

    static boolean isPackageName(String name) {
        return isTypeName(name);
    }

    static boolean isClassName(String name) {
        return isTypeName(name);
    }

    static boolean isTypeName(String name) {
        int next;
        int off = 0;
        while ((next = name.indexOf('.', off)) != -1) {
            String id = name.substring(off, next);
            if (isJavaIdentifier(id) == false) {
                return false;
            }
            off = next + 1;
        }
        String last = name.substring(off);
        return isJavaIdentifier(last);
    }

    static boolean isJavaIdentifier(String str) {
        if (str.isEmpty()) {
            return false;
        }

        int first = Character.codePointAt(str, 0);
        if (Character.isJavaIdentifierStart(first) == false) {
            return false;
        }

        int i = Character.charCount(first);
        while (i < str.length()) {
            int cp = Character.codePointAt(str, i);
            if (Character.isJavaIdentifierPart(cp) == false) {
                return false;
            }
            i += Character.charCount(cp);
        }
        return true;
    }
}
