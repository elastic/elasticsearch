/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.flakiness;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.Opcodes;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

/**
 * Unit tests for the ASM header-only scan that answers the one question the resolver cannot answer from paths
 * alone: given a class, is it abstract, and which concrete classes extend it? Fixture bytecode is generated
 * in-test so the scanner runs against real {@code .class} files rather than a stubbed hierarchy.
 */
public class ClassHierarchyScannerTests {

    @Rule
    public TemporaryFolder tmp = new TemporaryFolder();

    @Test
    public void testReportsAbstractAndConcreteClasses() throws IOException {
        ClassHierarchyScanner scanner = scanFooHierarchy();

        assertThat(scanner.isAbstract("com.example.AbstractFooTests"), is(true));
        assertThat(scanner.isAbstract("com.example.BarTests"), is(false));
    }

    @Test
    public void testExpandsToTransitiveConcreteDescendantsInSortedOrder() throws IOException {
        ClassHierarchyScanner.Expansion all = scanFooHierarchy().expand("com.example.AbstractFooTests", 5);

        assertThat(all.wasAbstract(), is(true));
        assertThat(all.totalConcrete(), equalTo(3));
        // Deterministic sorted FQCN order; MidTests excluded (abstract), LeafTests included (transitive).
        assertThat(all.toRun(), contains("com.example.BarTests", "com.example.BazTests", "com.example.LeafTests"));
    }

    /**
     * The cap bounds how many subclasses are actually run, but {@code totalConcrete} must still report the
     * true count - that is what lets the plan say "ran 2 of 3" rather than silently under-reporting.
     */
    @Test
    public void testCapLimitsRunListButNotTotalCount() throws IOException {
        ClassHierarchyScanner.Expansion capped = scanFooHierarchy().expand("com.example.AbstractFooTests", 2);

        assertThat(capped.totalConcrete(), equalTo(3));
        assertThat(capped.toRun(), contains("com.example.BarTests", "com.example.BazTests"));
    }

    @Test
    public void testConcreteClassExpandsToItself() throws IOException {
        ClassHierarchyScanner.Expansion concrete = scanSingleConcreteClass().expand("com.example.BarTests", 5);

        assertThat(concrete.wasAbstract(), is(false));
        assertThat(concrete.toRun(), contains("com.example.BarTests"));
    }

    /**
     * A class the scan never saw is passed through untouched rather than dropped: the scan set is the repo's
     * compiled output, so a miss means "not compiled here", not "not a test".
     */
    @Test
    public void testUnknownClassPassesThrough() throws IOException {
        ClassHierarchyScanner.Expansion unknown = scanSingleConcreteClass().expand("com.example.NotCompiled", 5);

        assertThat(unknown.wasAbstract(), is(false));
        assertThat(unknown.toRun(), contains("com.example.NotCompiled"));
    }

    // ---- fixtures ----

    /**
     * {@code AbstractFooTests <- {BarTests, BazTests, MidTests(abstract) <- LeafTests}}, plus an unrelated
     * class - so one hierarchy covers abstractness, transitivity, abstract-intermediate exclusion and capping.
     */
    private ClassHierarchyScanner scanFooHierarchy() throws IOException {
        Path classes = tmp.newFolder("classes").toPath();
        writeClass(classes, "com/example/AbstractFooTests", "java/lang/Object", true);
        writeClass(classes, "com/example/BarTests", "com/example/AbstractFooTests", false);
        writeClass(classes, "com/example/BazTests", "com/example/AbstractFooTests", false);
        writeClass(classes, "com/example/MidTests", "com/example/AbstractFooTests", true);
        writeClass(classes, "com/example/LeafTests", "com/example/MidTests", false);
        writeClass(classes, "com/example/StandaloneTests", "java/lang/Object", false);
        return ClassHierarchyScanner.scan(List.of(classes));
    }

    private ClassHierarchyScanner scanSingleConcreteClass() throws IOException {
        Path classes = tmp.newFolder("classes").toPath();
        writeClass(classes, "com/example/BarTests", "java/lang/Object", false);
        return ClassHierarchyScanner.scan(List.of(classes));
    }

    private static void writeClass(Path root, String internalName, String superInternal, boolean isAbstract) throws IOException {
        ClassWriter cw = new ClassWriter(0);
        int access = Opcodes.ACC_PUBLIC | Opcodes.ACC_SUPER | (isAbstract ? Opcodes.ACC_ABSTRACT : 0);
        cw.visit(Opcodes.V17, access, internalName, null, superInternal, null);
        cw.visitEnd();
        Path out = root.resolve(internalName + ".class");
        Files.createDirectories(out.getParent());
        Files.write(out, cw.toByteArray());
    }
}
