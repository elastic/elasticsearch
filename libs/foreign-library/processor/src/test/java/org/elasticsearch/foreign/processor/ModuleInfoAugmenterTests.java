/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.foreign.processor;

import junit.framework.TestCase;

import java.lang.classfile.ClassFile;
import java.lang.classfile.attribute.ModuleAttribute;
import java.lang.classfile.attribute.ModuleProvideInfo;
import java.lang.constant.ClassDesc;
import java.lang.constant.ModuleDesc;
import java.lang.constant.PackageDesc;
import java.util.List;
import java.util.Set;

/**
 * Tests that {@link ModuleInfoAugmenter#augment(byte[], List)} injects {@code provides
 * org.elasticsearch.foreign.LibraryProvider} entries into a compiled {@code module-info.class}.
 */
public class ModuleInfoAugmenterTests extends TestCase {

    private static final String LIBRARY_PROVIDER_FQN = "org.elasticsearch.foreign.LibraryProvider";
    private static final ClassDesc LIBRARY_PROVIDER = ClassDesc.of(LIBRARY_PROVIDER_FQN);

    /**
     * When the source module-info declares no {@code provides LibraryProvider} directive, augment
     * must add one with each generated provider FQN as an implementation.
     */
    public void testAugmentAddsProvidesWhenAbsent() {
        byte[] original = buildModuleInfo("test.mod", builder -> {});

        byte[] augmented = ModuleInfoAugmenter.augment(original, List.of("test.MyLib$Provider"));

        ModuleAttribute attr = readModuleAttribute(augmented);
        List<ModuleProvideInfo> libraryProvides = attr.provides()
            .stream()
            .filter(p -> p.provides().asSymbol().equals(LIBRARY_PROVIDER))
            .toList();
        assertEquals("Expected exactly one `provides LibraryProvider` directive", 1, libraryProvides.size());
        assertEquals(
            List.of(ClassDesc.of("test.MyLib$Provider")),
            libraryProvides.get(0).providesWith().stream().map(e -> e.asSymbol()).toList()
        );
    }

    /**
     * When the source module-info already has a {@code provides LibraryProvider with <other>} directive,
     * augment must merge the new providers into the existing {@code with} list, de-duplicating and
     * preserving order (existing first, new appended).
     */
    public void testAugmentMergesWithExistingProvides() {
        byte[] original = buildModuleInfo(
            "test.mod",
            builder -> builder.provides(ModuleProvideInfo.of(LIBRARY_PROVIDER, ClassDesc.of("test.Existing$Provider")))
        );

        byte[] augmented = ModuleInfoAugmenter.augment(original, List.of("test.MyLib$Provider", "test.Existing$Provider"));

        ModuleAttribute attr = readModuleAttribute(augmented);
        List<ModuleProvideInfo> libraryProvides = attr.provides()
            .stream()
            .filter(p -> p.provides().asSymbol().equals(LIBRARY_PROVIDER))
            .toList();
        assertEquals(1, libraryProvides.size());
        assertEquals(
            "Existing impl must be kept, new one appended, duplicate ignored",
            List.of(ClassDesc.of("test.Existing$Provider"), ClassDesc.of("test.MyLib$Provider")),
            libraryProvides.get(0).providesWith().stream().map(e -> e.asSymbol()).toList()
        );
    }

    /**
     * Augmenting must not disturb other {@code provides} directives for different service types.
     */
    public void testAugmentPreservesOtherProvides() {
        ClassDesc otherService = ClassDesc.of("test.OtherService");
        byte[] original = buildModuleInfo(
            "test.mod",
            builder -> builder.provides(ModuleProvideInfo.of(otherService, ClassDesc.of("test.OtherImpl")))
        );

        byte[] augmented = ModuleInfoAugmenter.augment(original, List.of("test.MyLib$Provider"));

        ModuleAttribute attr = readModuleAttribute(augmented);
        List<ModuleProvideInfo> otherProvides = attr.provides().stream().filter(p -> p.provides().asSymbol().equals(otherService)).toList();
        assertEquals(1, otherProvides.size());
        assertEquals(List.of(ClassDesc.of("test.OtherImpl")), otherProvides.get(0).providesWith().stream().map(e -> e.asSymbol()).toList());
    }

    /**
     * Augmenting twice with the same providers must be idempotent.
     */
    public void testAugmentIsIdempotent() {
        byte[] original = buildModuleInfo("test.mod", builder -> {});
        byte[] once = ModuleInfoAugmenter.augment(original, List.of("test.MyLib$Provider"));
        byte[] twice = ModuleInfoAugmenter.augment(once, List.of("test.MyLib$Provider"));

        ModuleAttribute attr = readModuleAttribute(twice);
        List<ModuleProvideInfo> libraryProvides = attr.provides()
            .stream()
            .filter(p -> p.provides().asSymbol().equals(LIBRARY_PROVIDER))
            .toList();
        assertEquals(1, libraryProvides.size());
        assertEquals(
            List.of(ClassDesc.of("test.MyLib$Provider")),
            libraryProvides.get(0).providesWith().stream().map(e -> e.asSymbol()).toList()
        );
    }

    /**
     * A richly-populated module-info (requires, exports, opens, uses, provides, version) must
     * survive augmentation with only the {@code provides} directive modified.
     */
    public void testAugmentPreservesAllNonProvidesAttributes() {
        ClassDesc otherService = ClassDesc.of("test.OtherService");
        byte[] original = buildModuleInfo("test.mod", builder -> {
            builder.requires(ModuleDesc.of("java.logging"), 0, null);
            builder.exports(PackageDesc.of("test.api"), 0);
            builder.opens(PackageDesc.of("test.internal"), 0);
            builder.uses(otherService);
            builder.provides(ModuleProvideInfo.of(otherService, ClassDesc.of("test.OtherImpl")));
        });

        ModuleAttribute originalAttr = readModuleAttribute(original);
        byte[] augmented = ModuleInfoAugmenter.augment(original, List.of("test.MyLib$Provider"));
        ModuleAttribute augmentedAttr = readModuleAttribute(augmented);

        assertModuleAttributesEqual(originalAttr, augmentedAttr, Set.of("provides"));

        List<ModuleProvideInfo> libraryProvides = augmentedAttr.provides()
            .stream()
            .filter(p -> p.provides().asSymbol().equals(LIBRARY_PROVIDER))
            .toList();
        assertEquals(1, libraryProvides.size());
        assertEquals(
            List.of(ClassDesc.of("test.MyLib$Provider")),
            libraryProvides.get(0).providesWith().stream().map(e -> e.asSymbol()).toList()
        );
    }

    private static void assertModuleAttributesEqual(ModuleAttribute expected, ModuleAttribute actual, Set<String> suppress) {
        if (suppress.contains("moduleName") == false) {
            assertEquals(expected.moduleName(), actual.moduleName());
        }
        if (suppress.contains("moduleFlags") == false) {
            assertEquals(expected.moduleFlagsMask(), actual.moduleFlagsMask());
        }
        if (suppress.contains("moduleVersion") == false) {
            assertEquals(expected.moduleVersion(), actual.moduleVersion());
        }
        if (suppress.contains("requires") == false) {
            assertEquals(
                expected.requires().stream().map(r -> r.requires().asSymbol()).toList(),
                actual.requires().stream().map(r -> r.requires().asSymbol()).toList()
            );
        }
        if (suppress.contains("exports") == false) {
            assertEquals(
                expected.exports().stream().map(e -> e.exportedPackage().asSymbol()).toList(),
                actual.exports().stream().map(e -> e.exportedPackage().asSymbol()).toList()
            );
        }
        if (suppress.contains("opens") == false) {
            assertEquals(
                expected.opens().stream().map(o -> o.openedPackage().asSymbol()).toList(),
                actual.opens().stream().map(o -> o.openedPackage().asSymbol()).toList()
            );
        }
        if (suppress.contains("uses") == false) {
            assertEquals(expected.uses().stream().map(u -> u.asSymbol()).toList(), actual.uses().stream().map(u -> u.asSymbol()).toList());
        }
        if (suppress.contains("provides") == false) {
            assertEquals(
                expected.provides().stream().map(p -> p.provides().asSymbol()).toList(),
                actual.provides().stream().map(p -> p.provides().asSymbol()).toList()
            );
        }
    }

    private static byte[] buildModuleInfo(
        String moduleName,
        java.util.function.Consumer<ModuleAttribute.ModuleAttributeBuilder> configure
    ) {
        return ClassFile.of().buildModule(ModuleAttribute.of(ModuleDesc.of(moduleName), configure));
    }

    private static ModuleAttribute readModuleAttribute(byte[] bytes) {
        var classModel = ClassFile.of().parse(bytes);
        return classModel.findAttribute(java.lang.classfile.Attributes.module()).orElseThrow();
    }
}
