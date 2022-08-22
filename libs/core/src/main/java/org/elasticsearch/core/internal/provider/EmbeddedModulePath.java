/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.core.internal.provider;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.lang.module.FindException;
import java.lang.module.InvalidModuleDescriptorException;
import java.lang.module.ModuleDescriptor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.jar.Attributes;
import java.util.jar.Manifest;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Utility methods that support module path functionality in embedded jars.
 */
final class EmbeddedModulePath {

    /**
     * Returns a module descriptor for the module at the given path.
     *
     * @throws FindException if not exactly one module is found
     */
    static ModuleDescriptor descriptorFor(Path path) {
        try {
            Optional<ModuleDescriptor> vmd = getModuleInfoVersioned(path);
            if (vmd.isPresent()) {
                return vmd.get();
            } else if (hasRootModuleInfo(path)) {
                return readModuleInfo(path.resolve(MODULE_INFO), path);
            } else {
                return descriptorForAutomatic(path);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    // Generates and returns a module descriptor for an automatic module at the given path.
    // Currently, only automatic modules with a manifest name are supported.
    private static ModuleDescriptor descriptorForAutomatic(Path path) throws IOException {
        String moduleName = moduleNameFromManifestOrNull(path);
        if (moduleName == null) {
            throw new FindException("automatic module without a manifest name is not supported, for: " + path);
        }
        ModuleDescriptor.Builder builder;
        try {
            builder = ModuleDescriptor.newAutomaticModule(moduleName);
        } catch (IllegalArgumentException e) {
            throw new FindException(AUTOMATIC_MODULE_NAME + ": " + e.getMessage());
        }

        version(path.getFileName().toString()).ifPresent(builder::version);

        // scan the names of the entries in the exploded JAR
        var scan = scan(path);

        // all packages are exported and open, since the auto-module bit is set
        String separator = path.getFileSystem().getSeparator();
        builder.packages(
            scan.classFiles().stream().map(cf -> toPackageName(cf, separator)).flatMap(Optional::stream).collect(Collectors.toSet())
        );

        services(scan.serviceFiles(), path).entrySet().forEach(e -> builder.provides(e.getKey(), e.getValue()));
        return builder.build();
    }

    private EmbeddedModulePath() {}

    private static final String MODULE_INFO = "module-info.class";

    private static final String SERVICES_PREFIX = "META-INF/services/";

    private static final Attributes.Name AUTOMATIC_MODULE_NAME = new Attributes.Name("Automatic-Module-Name");

    private static final int BASE_VERSION_FEATURE = 8; // lowest supported release version
    private static final int RUNTIME_VERSION_FEATURE = Runtime.version().feature();

    static boolean hasRootModuleInfo(Path path) {
        return Files.exists(path.resolve(MODULE_INFO));
    }

    static final String MRJAR_PREFIX_PATH = "META-INF/versions";

    static Path maybeRemoveMRJARPrefix(Path path) {
        if (path.startsWith(MRJAR_PREFIX_PATH)) {
            assert path.getNameCount() >= 3;
            return path.subpath(3, path.getNameCount());
        }
        return path;
    }

    static Set<String> explodedPackages(Path dir) {
        String separator = dir.getFileSystem().getSeparator();
        try (var stream = Files.find(dir, Integer.MAX_VALUE, ((path, attrs) -> attrs.isRegularFile()))) {
            return stream.map(path -> dir.relativize(path))
                .map(EmbeddedModulePath::maybeRemoveMRJARPrefix)
                .map(path -> toPackageName(path, separator))
                .flatMap(Optional::stream)
                .collect(Collectors.toSet());
        } catch (IOException x) {
            throw new UncheckedIOException(x);
        }
    }

    private static ModuleDescriptor readModuleInfo(Path miPath, Path pkgsRoot) throws IOException {
        try (var is = Files.newInputStream(miPath)) {
            return ModuleDescriptor.read(is, () -> explodedPackages(pkgsRoot));
        }
    }

    private static Optional<ModuleDescriptor> getModuleInfoVersioned(Path rootPath) throws IOException {
        for (int v = RUNTIME_VERSION_FEATURE; v >= BASE_VERSION_FEATURE; v--) {
            Path mi = rootPath.resolve("META-INF").resolve("versions").resolve(Integer.toString(v)).resolve(MODULE_INFO);
            if (Files.exists(mi)) {
                return Optional.of(readModuleInfo(mi, rootPath));
            }
        }
        return Optional.empty();
    }

    // A scan result, which aggregates class and services files.
    record ScanResult(Set<String> classFiles, Set<String> serviceFiles) {}

    // Scans a given path for class files and services.
    static ScanResult scan(Path path) throws IOException {
        try (var stream = Files.walk(path)) {
            Map<Boolean, Set<String>> map = stream.filter(p -> Files.isDirectory(p) == false)
                .map(p -> path.relativize(p).toString())
                .filter(p -> (p.endsWith(".class") ^ p.startsWith(SERVICES_PREFIX)))
                .collect(Collectors.partitioningBy(e -> e.startsWith(SERVICES_PREFIX), Collectors.toSet()));
            return new ScanResult(map.get(Boolean.FALSE), map.get(Boolean.TRUE));
        }
    }

    private static final Pattern DASH_VERSION = Pattern.compile("-(\\d+(\\.|$))");

    // Determines the module version (of an automatic module), given the jar file name. As per,
    // https://docs.oracle.com/en/java/javase/18/docs/api/java.base/java/lang/module/ModuleFinder.html#of(java.nio.file.Path...)
    static Optional<ModuleDescriptor.Version> version(String jarName) {
        if (jarName.endsWith(".jar") == false) {
            throw new IllegalArgumentException("unexpected jar name: " + jarName);
        }
        // drop ".jar"
        String name = jarName.substring(0, jarName.length() - 4);
        // find first occurrence of -${NUMBER}. or -${NUMBER}$
        Matcher matcher = DASH_VERSION.matcher(name);
        if (matcher.find()) {
            int start = matcher.start();
            try {
                String tail = name.substring(start + 1);
                return Optional.of(ModuleDescriptor.Version.parse(tail));
            } catch (IllegalArgumentException ignore) {}
        }
        return Optional.empty();
    }

    // Parses a set of given service files, and returns a map of service name to list of provider
    // classes.
    static Map<String, List<String>> services(Set<String> serviceFiles, Path path) throws IOException {
        // map names of service configuration files to service names
        Set<String> serviceNames = serviceFiles.stream()
            .map(EmbeddedModulePath::toServiceName)
            .flatMap(Optional::stream)
            .collect(Collectors.toSet());

        Map<String, List<String>> map = new HashMap<>();
        // parse each service configuration file
        for (String sn : serviceNames) {
            Path se = path.resolve(SERVICES_PREFIX + sn);
            List<String> providerClasses = Files.readAllLines(se)
                .stream()
                .map(EmbeddedModulePath::dropCommentAndTrim)
                .filter(Predicate.not(String::isEmpty))
                .toList();
            if (providerClasses.isEmpty() == false) {
                map.put(sn, providerClasses);
            }
        }
        return map;
    }

    // Drops comments and trims the given string.
    private static String dropCommentAndTrim(String line) {
        int ci = line.indexOf('#');
        if (ci >= 0) {
            line = line.substring(0, ci);
        }
        return line.trim();
    }

    // Returns an optional containing the package name from a given path and separator, or an
    // empty optional if none.
    static Optional<String> toPackageName(Path file, String separator) {
        Path parent = file.getParent();
        if (parent == null) {
            String name = file.toString();
            if (name.endsWith(".class") && name.equals(MODULE_INFO) == false) {
                String msg = name + " found in top-level directory" + " (unnamed package not allowed in module)";
                throw new InvalidModuleDescriptorException(msg);
            }
            return Optional.empty();
        }

        String pn = parent.toString().replace(separator, ".");
        if (isPackageName(pn)) {
            return Optional.of(pn);
        } else {
            // not a valid package name
            return Optional.empty();
        }
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

    // Returns an optional containing the service name from a given services path name, or an
    // empty optional if none.
    static Optional<String> toServiceName(String cf) {
        if (cf.startsWith(SERVICES_PREFIX) == false) {
            throw new IllegalArgumentException("unexpected service " + cf);
        }
        if (SERVICES_PREFIX.length() < cf.length()) {
            String prefix = cf.substring(0, SERVICES_PREFIX.length());
            if (prefix.equals(SERVICES_PREFIX)) {
                String sn = cf.substring(SERVICES_PREFIX.length());
                if (isClassName(sn)) {
                    return Optional.of(sn);
                }
            }
        }
        return Optional.empty();
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

    private static final String MANIFEST_PATH = "META-INF/MANIFEST.MF";

    static String moduleNameFromManifestOrNull(Path path) throws IOException {
        Path mp = path.resolve(MANIFEST_PATH);
        if (Files.exists((mp))) {
            try (InputStream is = Files.newInputStream(mp)) {
                Manifest manifest = new Manifest(is);
                return manifest.getMainAttributes().getValue(AUTOMATIC_MODULE_NAME);
            }
        }
        return null;
    }
}
