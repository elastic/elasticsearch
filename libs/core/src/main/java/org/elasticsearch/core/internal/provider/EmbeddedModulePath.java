/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.core.internal.provider;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.lang.module.FindException;
import java.lang.module.InvalidModuleDescriptorException;
import java.lang.module.ModuleDescriptor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.jar.Attributes;
import java.util.jar.Manifest;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static java.nio.charset.StandardCharsets.UTF_8;

final class EmbeddedModulePath {

    private EmbeddedModulePath() {}

    private static final String MODULE_INFO = "module-info.class";

    private static final String SERVICES_PREFIX = "META-INF/services/";

    private static final Attributes.Name AUTOMATIC_MODULE_NAME = new Attributes.Name("Automatic-Module-Name");

    private static final int BASE_VERSION_FEATURE = 8; // lowest supported release version
    private static final int RUNTIME_VERSION_FEATURE = Runtime.version().feature();

    static {
        assert RUNTIME_VERSION_FEATURE >= BASE_VERSION_FEATURE;
    }

    private static final String MRJAR_VERSION_PREFIX = "META-INF/versions/";

    static boolean hasRootModuleInfo(Path path) {
        Path mi = path.resolve(MODULE_INFO);
        if (Files.exists(mi)) {
            return true;
        }
        return false;
    }

    static Set<String> explodedPackages(Path dir) {
        String separator = dir.getFileSystem().getSeparator();
        try {
            var fs = Files.find(dir, Integer.MAX_VALUE, ((path, attrs) -> attrs.isRegularFile()))
                .map(path -> dir.relativize(path))
                .map(path -> toPackageName(path, separator))
                .flatMap(Optional::stream)
                .collect(Collectors.toSet());
            return fs;
        } catch (IOException x) {
            throw new UncheckedIOException(x);
        }
    }

    static ModuleDescriptor readModuleInfo(Path miPath, Path pkgsRoot) {
        try (var is = Files.newInputStream(miPath)) {
            return ModuleDescriptor.read(is, () -> explodedPackages(pkgsRoot));
        } catch (IOException ioe) {
            throw new UncheckedIOException(ioe);
        }
    }

    static Optional<ModuleDescriptor> getModuleInfoVersioned(Path rootPath) {
        for (int v = RUNTIME_VERSION_FEATURE; v >= BASE_VERSION_FEATURE; v--) {
            Path mi = rootPath.resolve("META-INF").resolve("versions").resolve(Integer.toString(v)).resolve(MODULE_INFO);
            if (Files.exists(mi)) {
                return Optional.of(readModuleInfo(mi, rootPath));
            }
        }
        return Optional.empty();
    }

    /**
     * Returns a module descriptor for the module at the given path.
     *
     * @throws FindException if not exactly one module is found
     */
    static ModuleDescriptor descriptorFor(Path path) {
        Optional<ModuleDescriptor> vmd = getModuleInfoVersioned(path);
        if (vmd.isPresent()) {
            var md = vmd.get();
            // todo: perform scan and verification
            return md;
        } else if (hasRootModuleInfo(path)) {
            var md = readModuleInfo(path.resolve(MODULE_INFO), path);
            // todo: perform scan and verification
            return md;
        } else {
            return descriptorForAutomatic(path);
        }
    }

    // static ModuleDescriptor descriptorFor2(Path path) {
    // Optional<ModuleDescriptor> vmd = getModuleInfoVersioned(path);
    // if (vmd.isPresent()) {
    // var md = vmd.get();
    // // todo: perform scan and verification
    // return md;
    // } else if (hasRootModuleInfo(path)) {
    // //if (System.getProperty("os.name").startsWith("Windows")) {
    // // This is a loathsome workaround for JDK-8282444, which affects Windows only
    // var md = readModuleInfo(path.resolve(MODULE_INFO), path);
    // // todo: perform scan and verification
    // return md;
    // } else {
    // // just use the JDK's built-in module finder
    // Set<ModuleReference> mrefs = ModuleFinder.of(path).findAll();
    // if (mrefs.size() == 1) {
    // return mrefs.iterator().next().descriptor();
    // } else {
    // throw new FindException("more than one module found at path: %s, mods: %s.".formatted(path, mrefs));
    // }
    // }
    // } else {
    // return descriptorForAutomatic(path);
    // }
    // }

    record ScanResult(Set<String> classFiles, Set<String> serviceFiles) {}

    /**
     * Generates and returns a module descriptor for an automatic module at the given path.
     * Currently, only automatic modules with a manifest name are supported.
     */
    static ModuleDescriptor descriptorForAutomatic(Path path) {
        String moduleName = moduleNameFromManifestOrNull(path);
        if (moduleName == null && path.endsWith("log4j2-ecs-layout-1.2.0.jar")) {
            moduleName = "log4j2.ecs.layout";
        }
        if (moduleName == null && path.endsWith("ecs-logging-core-1.2.0.jar")) {
            moduleName = "ecs.logging.core";
        }
        if (moduleName == null) {
            throw new FindException("automatic module without a manifest name is not supported, for:" + path);
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

        // all packages are exported and open, since the auto-module bit has been set
        builder.packages(
            scan.classFiles().stream().map(EmbeddedModulePath::toPackageName).flatMap(Optional::stream).collect(Collectors.toSet())
        );

        services(scan.serviceFiles(), path).entrySet().forEach(e -> builder.provides(e.getKey(), e.getValue()));
        return builder.build();
    }

    static ScanResult scan(Path path) {
        try {
            // scan the names of the entries in the JAR file // TODO: add support for multi-release
            Map<Boolean, Set<String>> map = Files.walk(path)
                .filter(p -> Files.isDirectory(p) == false)
                .map(p -> path.relativize(p).toString())
                .filter(p -> (p.endsWith(".class") ^ p.startsWith(SERVICES_PREFIX)))
                .collect(Collectors.partitioningBy(e -> e.startsWith(SERVICES_PREFIX), Collectors.toSet()));
            return new ScanResult(map.get(Boolean.FALSE), map.get(Boolean.TRUE));
        } catch (IOException e) {
            throw new FindException(e);
        }
    }

    static final Pattern DASH_VERSION = Pattern.compile("-(\\d+(\\.|$))");

    static Optional<ModuleDescriptor.Version> version(String jarName) {
        assert jarName.length() > ".jar".length();
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

    static Map<String, List<String>> services(Set<String> serviceFiles, Path path) {
        // map names of service configuration files to service names
        Set<String> serviceNames = serviceFiles.stream()
            .map(EmbeddedModulePath::toServiceName)
            .flatMap(Optional::stream)
            .collect(Collectors.toSet());

        try {
            Map<String, List<String>> map = new HashMap<>();
            // parse each service configuration file
            for (String sn : serviceNames) {
                Path se = path.resolve(SERVICES_PREFIX + sn);
                List<String> providerClasses = new ArrayList<>();
                try (InputStream in = Files.newInputStream(se)) {
                    BufferedReader reader = new BufferedReader(new InputStreamReader(in, UTF_8));
                    String cn;
                    while ((cn = nextLine(reader)) != null) {
                        if (cn.isEmpty() == false) {
                            providerClasses.add(cn);
                        }
                    }
                }
                if (providerClasses.isEmpty() == false) {
                    map.put(sn, providerClasses);
                }
            }
            return map;
        } catch (IOException e) {
            throw new FindException(e);
        }
    }

    private static Optional<String> toPackageName(Path file, String separator) {
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

    private static Optional<String> toPackageName(String name) {
        assert name.endsWith("/") == false;
        int index = name.lastIndexOf("/");
        if (index == -1) {
            if (name.endsWith(".class") && name.equals(MODULE_INFO) == false) {
                String msg = name + " found in top-level directory (unnamed package not allowed in module)";
                throw new InvalidModuleDescriptorException(msg);
            }
            return Optional.empty();
        }

        String pn = name.substring(0, index).replace('/', '.');
        if (isPackageName(pn)) {
            return Optional.of(pn);
        } else {
            // not a valid package name
            return Optional.empty();
        }
    }

    static Optional<String> toServiceName(String cf) {
        assert cf.startsWith(SERVICES_PREFIX);
        int index = cf.lastIndexOf("/") + 1;
        if (index < cf.length()) {
            String prefix = cf.substring(0, index);
            if (prefix.equals(SERVICES_PREFIX)) {
                String sn = cf.substring(index);
                if (isClassName(sn)) {
                    return Optional.of(sn);
                }
            }
        }
        return Optional.empty();
    }

    static String nextLine(BufferedReader reader) throws IOException {
        String ln = reader.readLine();
        if (ln != null) {
            int ci = ln.indexOf('#');
            if (ci >= 0) {
                ln = ln.substring(0, ci);
            }
            ln = ln.trim();
        }
        return ln;
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

    static String moduleNameFromManifestOrNull(Path path) {
        Path mp = path.resolve(MANIFEST_PATH);
        if (Files.exists((mp))) {
            try (InputStream is = Files.newInputStream(mp)) {
                if (is != null) {
                    Manifest manifest = new Manifest(is);
                    return manifest.getMainAttributes().getValue(AUTOMATIC_MODULE_NAME);
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        return null;
    }
}
