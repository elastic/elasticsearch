/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins;

import org.elasticsearch.core.SuppressForbidden;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.lang.module.InvalidModuleDescriptorException;
import java.lang.module.ModuleDescriptor;
import java.lang.module.ModuleFinder;
import java.lang.module.ModuleReader;
import java.lang.module.ModuleReference;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.ZipFile;

/**
 * Support methods for creating a synthetic module.
 */
public class ModuleSupport {

    private ModuleSupport() {
        throw new AssertionError("Utility class, should not be instantiated");
    }

    static ModuleFinder ofSyntheticPluginModule(
        String name,
        Path[] jarPaths,
        Set<String> requires,
        Set<String> uses,
        Predicate<String> isPackageInParentLayers
    ) {
        try {
            return new InMemoryModuleFinder(
                new InMemoryModuleReference(
                    createModuleDescriptor(name, jarPaths, requires, uses, isPackageInParentLayers),
                    URI.create("module:/" + name)
                )
            );
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @SuppressForbidden(reason = "need access to the jar file")
    static ModuleDescriptor createModuleDescriptor(
        String name,
        Path[] jarPaths,
        Set<String> requires,
        Set<String> uses,
        Predicate<String> isPackageInParentLayers
    ) throws IOException {
        var builder = ModuleDescriptor.newOpenModule(name); // open module, for now
        requires.forEach(builder::requires);
        uses.forEach(builder::uses);

        // scan the names of the entries in the JARs
        Set<String> pkgs = new HashSet<>();
        Map<String, List<String>> allBundledProviders = new HashMap<>();
        Set<String> servicesUsedInBundle = new HashSet<>();
        for (Path path : jarPaths) {
            assert path.getFileName().toString().endsWith(".jar") : "expected jars suffix, in path: " + path;
            try (JarFile jf = new JarFile(path.toFile(), true, ZipFile.OPEN_READ, Runtime.version())) {
                // if we have a module declaration, trust its uses/provides
                JarEntry moduleInfo = jf.getJarEntry("module-info.class");
                if (moduleInfo != null) {
                    var descriptor = getDescriptorForModularJar(path);
                    pkgs.addAll(descriptor.packages());
                    servicesUsedInBundle.addAll(descriptor.uses());
                    for (ModuleDescriptor.Provides p : descriptor.provides()) {
                        String serviceName = p.service();
                        List<String> providersInModule = p.providers();

                        allBundledProviders.compute(serviceName, (k, v) -> createListOrAppend(v, providersInModule));
                        servicesUsedInBundle.add(serviceName);
                    }
                } else {
                    var scan = scan(jf);
                    scan.classFiles().stream().map(cf -> toPackageName(cf, "/")).flatMap(Optional::stream).forEach(pkgs::add);

                    // read providers from the list of service files
                    for (String serviceFileName : scan.serviceFiles()) {
                        String serviceName = getServiceName(serviceFileName);
                        List<String> providersInJar = getProvidersFromServiceFile(jf, serviceFileName);

                        allBundledProviders.compute(serviceName, (k, v) -> createListOrAppend(v, providersInJar));
                        servicesUsedInBundle.add(serviceName);
                    }
                }
            }
        }

        builder.packages(pkgs);

        // we don't want to add any services we already got from the parent layer
        servicesUsedInBundle.removeAll(uses);

        // Services that aren't exported in the parent layer or defined in our
        // bundle. This can happen for optional (compile-time) dependencies
        Set<String> missingServices = servicesUsedInBundle.stream()
            .filter(s -> isPackageInParentLayers.test(toPackageName(s, ".").orElseThrow()) == false)
            .filter(s -> pkgs.contains(toPackageName(s, ".").orElseThrow()) == false)
            .collect(Collectors.toSet());

        servicesUsedInBundle.stream().filter(s -> missingServices.contains(s) == false).forEach(builder::uses);
        allBundledProviders.entrySet()
            .stream()
            .filter(e -> missingServices.contains(e.getKey()) == false)
            .forEach(e -> builder.provides(e.getKey(), e.getValue()));
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

    /**
     * If a module has at least one unqualified export, then it has a public API
     * that can be used by other modules. If all of its exports are qualified, only
     * modules specified in its descriptor can read from it, and there's no
     * use in requiring it for a synthetic module.
     * @param md A module descriptor.
     * @return true if the module as at least one unqualified export, false otherwise
     */
    static boolean hasAtLeastOneUnqualifiedExport(ModuleDescriptor md) {
        return md.exports().stream().anyMatch(Predicate.not(ModuleDescriptor.Exports::isQualified));
    }

    /**
     * We assume that if a module name starts with "java." or "jdk.", it is a Java
     * platform module. We also assume that there are no Java platform modules that
     * start with other prefixes. This assumption is true as of Java 17, where "java."
     * is for Java SE APIs and "jdk." is for JDK-specific modules.
     */
    static boolean isJavaPlatformModule(ModuleDescriptor md) {
        return md.name().startsWith("java.") || md.name().startsWith("jdk.");
    }

    @SuppressForbidden(reason = "need access to the jar file")
    private static List<String> getProvidersFromServiceFile(JarFile jf, String sf) throws IOException {
        try (BufferedReader bf = new BufferedReader(new InputStreamReader(jf.getInputStream(jf.getEntry(sf)), StandardCharsets.UTF_8))) {
            return bf.lines().filter(Predicate.not(l -> l.startsWith("#"))).filter(Predicate.not(String::isEmpty)).toList();
        }
    }

    private static List<String> createListOrAppend(List<String> currentList, List<String> newList) {
        if (currentList == null) {
            return List.copyOf(newList);
        }
        return Stream.concat(currentList.stream(), newList.stream()).toList();
    }

    private static String getServiceName(String sf) {
        return sf.substring("META-INF/services/".length());
    }

    private static ModuleDescriptor getDescriptorForModularJar(Path path) {
        return ModuleFinder.of(path)
            .findAll()
            .stream()
            .findFirst()
            .orElseThrow(() -> new IllegalStateException("found a module descriptor but failed to load a module from " + path))
            .descriptor();
    }
}
