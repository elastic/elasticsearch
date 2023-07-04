/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.core.internal.provider;

import java.lang.module.ModuleDescriptor;
import java.lang.module.ModuleFinder;
import java.lang.module.ModuleReader;
import java.lang.module.ModuleReference;
import java.net.URI;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * An in-memory module finder.
 *
 * <p> An in-memory module finder finds module references that are <i>non-openable</i>. Attempts to
 * open a non-openable module reference returned by this finder will result in an
 * {@link UnsupportedOperationException}. In this way an in-memory module finder can be used for the
 * purpose of resolution only, and not actual class or resource loading (since it does not provide
 * access to an underlying module reader).
 */
class InMemoryModuleFinder implements ModuleFinder {

    private final Map<String, ModuleReference> namesToReference;

    /**
     * Creates a module finder that eagerly scans the given paths to build an in memory module
     * finder.
     *
     * <p> The set missingModules are filtered out of the requires directives of the retrieved
     * module descriptors.
     */
    static InMemoryModuleFinder of(Set<String> missingModules, Path... entries) {
        return new InMemoryModuleFinder(
            Arrays.stream(entries)
                .map(EmbeddedModulePath::descriptorFor)
                .map(md -> filterRequires(md, missingModules))
                .collect(
                    Collectors.toUnmodifiableMap(
                        ModuleDescriptor::name,
                        m -> new InMemoryModuleReference(m, URI.create("module:/" + m.name()))
                    )
                )
        );
    }

    static ModuleDescriptor filterRequires(ModuleDescriptor md, Set<String> missingModules) {
        if (missingModules.size() == 0
            || md.isAutomatic()
            || md.requires().stream().anyMatch(req -> missingModules.contains(req.name())) == false) {
            return md;
        }

        ModuleDescriptor.Builder builder;
        if (md.isOpen()) {
            builder = ModuleDescriptor.newOpenModule(md.name());
        } else {
            builder = ModuleDescriptor.newModule(md.name());
        }
        md.version().ifPresent(builder::version);
        md.requires().stream().filter(req -> missingModules.contains(req.name()) == false).forEach(builder::requires);
        md.exports().forEach(builder::exports);
        md.opens().forEach(builder::opens);
        md.provides().forEach(builder::provides);
        md.uses().forEach(builder::uses);
        builder.packages(md.packages());
        return builder.build();
    }

    /**
     * Creates a module finder of the given module descriptors.
     */
    static InMemoryModuleFinder of(ModuleDescriptor... descriptors) {
        return new InMemoryModuleFinder(
            Arrays.stream(descriptors)
                .collect(
                    Collectors.toUnmodifiableMap(
                        ModuleDescriptor::name,
                        m -> new InMemoryModuleReference(m, URI.create("module:/" + m.name()))
                    )
                )
        );
    }

    private InMemoryModuleFinder(Map<String, ModuleReference> mrefs) {
        this.namesToReference = mrefs;
    }

    @Override
    public Optional<ModuleReference> find(String name) {
        Objects.requireNonNull(name);
        return Optional.ofNullable(namesToReference.get(name));
    }

    @Override
    public Set<ModuleReference> findAll() {
        return Set.copyOf(namesToReference.values());
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
}
