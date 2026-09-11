/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.fixtures;

import org.elasticsearch.core.PathUtils;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.greaterThan;

/**
 * Fails when our own code re-lists a dimension's values instead of reading them from the contract.
 *
 * <p>This is the defect that keeps recurring, and it never announces itself. A component writes its own
 * copy of a value set, the contract later gains a value, and the copy stays one short -- so coverage
 * quietly shrinks while every test still passes. Found five times in one sitting: DIALECT_SLOTS held in
 * two files, the delimiter character switched in two generators, dialect exclusions enumerated per slot,
 * and DISTRIBUTION_MODES hard-coded in two multi-node classes, both missing weighted_round_robin -- the
 * one mode that needs a multi-node cluster to mean anything.
 *
 * <p>The signature is mechanical: a line holding two or more string literals that are values of the SAME
 * declared dimension. That catches list and switch forms alike without needing to parse Java.
 *
 * <p>A deliberate copy stays possible, but has to say so: put {@code dimension-copy-ok} on the line with
 * a reason. An exception that must be written down is an exception someone can find later.
 */
public class DimensionCopyTests extends ESTestCase {

    private static final Pattern STRING_LITERAL = Pattern.compile("\"([a-z_0-9]{3,})\"");
    private static final String OPT_OUT = "dimension-copy-ok";

    /** The classes that legitimately hold the vocabulary: the contract's own readers. */
    private static final Set<String> CONTRACT_OWNERS = Set.of(
        "FixtureDimensions.java",
        "FixtureCapabilities.java",
        "FixtureMatrix.java",
        "FixtureContractAudit.java",
        "DimensionCopyTests.java"
    );

    public void testNoComponentKeepsItsOwnCopyOfADimensionsValues() throws IOException {
        List<Path> guardedRoots = locateGuardedRoots();
        assertThat("the per-format qa modules must be covered too", guardedRoots.size(), greaterThan(1));
        Map<String, Set<String>> dimensions = interestingDimensions();
        assertFalse("no dimensions parsed -- the gate would pass vacuously", dimensions.isEmpty());

        List<String> offenders = new ArrayList<>();
        List<Path> sources = new ArrayList<>();
        // Prune build directories during traversal rather than filtering afterwards: the test runner writes
        // and deletes temp files under build/ while this walks, so a post-hoc filter still descends into a
        // tree that is moving and Files.walk throws NoSuchFileException. A gate that fails on its own
        // scratch directory teaches nothing.
        for (Path guardedRoot : guardedRoots) {
            Files.walkFileTree(guardedRoot, new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) {
                    return dir.getFileName().toString().equals("build") ? FileVisitResult.SKIP_SUBTREE : FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                    String name = file.getFileName().toString();
                    if (name.endsWith(".java") && CONTRACT_OWNERS.contains(name) == false) {
                        sources.add(file);
                    }
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult visitFileFailed(Path file, IOException exc) {
                    return FileVisitResult.CONTINUE;
                }
            });
        }
        assertFalse("walked no sources -- the gate would pass vacuously", sources.isEmpty());

        for (Path source : sources) {
            List<String> lines = Files.readAllLines(source, StandardCharsets.UTF_8);
            for (int i = 0; i < lines.size(); i++) {
                String line = lines.get(i);
                if (line.contains(OPT_OUT)) {
                    continue;
                }
                Set<String> literals = new LinkedHashSet<>();
                Matcher m = STRING_LITERAL.matcher(line);
                while (m.find()) {
                    literals.add(m.group(1));
                }
                if (literals.size() < 2) {
                    continue;
                }
                for (Map.Entry<String, Set<String>> dimension : dimensions.entrySet()) {
                    Set<String> shared = new LinkedHashSet<>(literals);
                    shared.retainAll(dimension.getValue());
                    if (shared.size() >= 2) {
                        offenders.add(
                            String.format(
                                Locale.ROOT,
                                "%s:%d re-lists %d of %d [%s] values %s -- read them from the contract, "
                                    + "or mark the line '%s' with a reason",
                                source.getFileName(),
                                i + 1,
                                shared.size(),
                                dimension.getValue().size(),
                                dimension.getKey(),
                                shared,
                                OPT_OUT
                            )
                        );
                    }
                }
            }
        }
        assertTrue("copied dimension values:\n" + String.join("\n", offenders), offenders.isEmpty());
    }

    /**
     * Dimensions worth policing: at least two values, and not a plain boolean. A {@code true}/{@code false}
     * pair matches every boolean in the tree and would drown the real signal.
     */
    private static Map<String, Set<String>> interestingDimensions() {
        FixtureDimensions dimensions = FixtureDimensions.get();
        Map<String, Set<String>> out = new LinkedHashMap<>();
        for (String name : dimensions.names()) {
            Set<String> values = new LinkedHashSet<>(dimensions.values(name));
            if (values.size() < 2 || values.equals(Set.of("true", "false"))) {
                continue;
            }
            out.put(name, values);
        }
        return out;
    }

    /**
     * Every tree this gate covers, found by walking up rather than guessed from a working directory.
     *
     * <p>It covered only esql/qa at first, which left the per-format modules unguarded -- and a live copy
     * was sitting in one of them, three of the four distribution modes, the same defect already fixed
     * twice elsewhere. A gate that inspects some of the code is a gate that reports clean while the
     * defect it exists for is still there.
     */
    private static List<Path> locateGuardedRoots() {
        Path repo = PathUtils.get("").toAbsolutePath();
        while (repo != null && Files.isDirectory(repo.resolve("x-pack/plugin/esql/qa")) == false) {
            repo = repo.getParent();
        }
        if (repo == null) {
            throw new AssertionError("could not locate the repository; the gate must not pass by not looking");
        }
        List<Path> roots = new ArrayList<>();
        roots.add(repo.resolve("x-pack/plugin/esql/qa"));
        try (Stream<Path> plugins = Files.list(repo.resolve("x-pack/plugin"))) {
            plugins.filter(p -> p.getFileName().toString().startsWith("esql-datasource-"))
                .map(p -> p.resolve("qa"))
                .filter(Files::isDirectory)
                .forEach(roots::add);
        } catch (IOException e) {
            throw new AssertionError("could not enumerate the per-format qa modules", e);
        }
        return roots;
    }
}
