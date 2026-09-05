/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.flakiness;

import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.Opcodes;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Stream;

/**
 * Reads the compiled test bytecode to answer two questions the TypeScript side cannot: is a class
 * {@code abstract}, and what are its concrete subclasses? This is the intended-ClassGraph enrichment,
 * implemented with ASM (already on the {@code build-tools-internal} classpath - see JAVA_RESOLVER_NOTES.md).
 *
 * <p>Scanning is header-only: {@link ClassReader} with {@code SKIP_CODE | SKIP_DEBUG | SKIP_FRAMES} reads
 * just the access flags and super-class from each {@code .class}, so it is cheap even over a whole source
 * set's output.
 */
public final class ClassHierarchyScanner {

    private final Map<String, Boolean> isAbstract = new HashMap<>();
    private final Map<String, Set<String>> children = new HashMap<>();
    private final Map<String, Path> originDir = new HashMap<>();

    /** Scan every {@code .class} under the given compiled-output directories. */
    public static ClassHierarchyScanner scan(List<Path> classDirs) {
        ClassHierarchyScanner scanner = new ClassHierarchyScanner();
        for (Path dir : classDirs) {
            if (Files.isDirectory(dir) == false) {
                continue;
            }
            try (Stream<Path> walk = Files.walk(dir)) {
                walk.filter(p -> p.toString().endsWith(".class")).forEach(p -> scanner.readClass(p, dir));
            } catch (IOException e) {
                throw new UncheckedIOException("Failed walking compiled classes under " + dir, e);
            }
        }
        return scanner;
    }

    private void readClass(Path classFile, Path root) {
        try (InputStream in = Files.newInputStream(classFile)) {
            ClassReader reader = new ClassReader(in);
            reader.accept(new HeaderVisitor(root), ClassReader.SKIP_CODE | ClassReader.SKIP_DEBUG | ClassReader.SKIP_FRAMES);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed reading class file " + classFile, e);
        }
    }

    /**
     * Which scanned class directory a class was found under, or {@code null} if it was never seen. Now that the
     * scan spans the whole repo, an expanded subclass may well live in a different project than the abstract
     * base it was expanded from, and the plan has to say so rather than silently attributing it to the base's
     * project (whose {@code Test} tasks would not run it).
     */
    public Path originDir(String fqcn) {
        return originDir.get(fqcn);
    }

    private final class HeaderVisitor extends ClassVisitor {
        private final Path root;

        HeaderVisitor(Path root) {
            super(Opcodes.ASM9);
            this.root = root;
        }

        @Override
        public void visit(int version, int access, String name, String signature, String superName, String[] interfaces) {
            String fqcn = dotted(name);
            // Interfaces are ACC_ABSTRACT too, but never a test base we expand; exclude them so an
            // accidental interface named *Tests does not read as an abstract base.
            boolean abstractClass = (access & Opcodes.ACC_ABSTRACT) != 0 && (access & Opcodes.ACC_INTERFACE) == 0;
            isAbstract.put(fqcn, abstractClass);
            // First scan root wins; classDirs is deterministically ordered, so this is reproducible.
            originDir.putIfAbsent(fqcn, root);
            if (superName != null) {
                children.computeIfAbsent(dotted(superName), k -> new HashSet<>()).add(fqcn);
            }
        }
    }

    /** Whether the given FQCN was seen at all in the scanned output. */
    public boolean isKnown(String fqcn) {
        return isAbstract.containsKey(fqcn);
    }

    public boolean isAbstract(String fqcn) {
        return Boolean.TRUE.equals(isAbstract.get(fqcn));
    }

    /** The result of expanding a class: which concrete FQCNs to run, and how many concrete descendants exist. */
    public record Expansion(List<String> toRun, int totalConcrete, boolean wasAbstract) {}

    /**
     * Expand a base target's FQCN into the concrete classes to run:
     * <ul>
     *   <li>concrete (or unknown) class -&gt; itself, a single run</li>
     *   <li>abstract class -&gt; its transitive concrete descendants, sorted by FQCN (deterministic) and
     *       capped at {@code cap}</li>
     * </ul>
     */
    public Expansion expand(String fqcn, int cap) {
        if (isKnown(fqcn) == false || isAbstract(fqcn) == false) {
            // Unknown => best-effort pass-through (the source file resolved, so run it as-is).
            return new Expansion(List.of(fqcn), 1, false);
        }
        // Transitive concrete descendants, deterministically ordered.
        TreeSet<String> concrete = new TreeSet<>();
        Set<String> visited = new HashSet<>();
        List<String> stack = new ArrayList<>(children.getOrDefault(fqcn, Set.of()));
        while (stack.isEmpty() == false) {
            String c = stack.remove(stack.size() - 1);
            if (visited.add(c) == false) {
                continue;
            }
            if (isAbstract(c) == false && isKnown(c)) {
                concrete.add(c);
            }
            stack.addAll(children.getOrDefault(c, Set.of()));
        }
        List<String> capped = concrete.stream().limit(Math.max(0, cap)).toList();
        return new Expansion(capped, concrete.size(), true);
    }

    private static String dotted(String internalName) {
        return internalName.replace('/', '.');
    }
}
