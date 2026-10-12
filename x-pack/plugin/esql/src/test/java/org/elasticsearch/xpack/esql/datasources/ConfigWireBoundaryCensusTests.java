/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

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
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Every boundary where a query's config map crosses the wire must drop the coordinator-only dataset version
 * first. On the relation and the exec the reason is live: a data node builds its storage provider from that map
 * and {@code StorageProviderCache} keys on the whole of it, so an older node, whose {@code FRAMEWORK_KEYS} cannot
 * know this key, fragments its client pool per dataset definition and throws at the pool's ceiling. On a split
 * the reason is defence in depth - nothing on a data node reads that key from a split today.
 * <p>
 * This fails the build on a NEW boundary. Each existing one is pinned behaviourally by its own round-trip test;
 * what those cannot do is notice a fourth {@code writeTo} appearing somewhere else.
 * <p>
 * <b>What it does not cover,</b> stated so the next reader does not rely on more than this checks. It requires
 * the enclosing {@code writeTo} to CALL {@code wireConfig} - comments are stripped before matching, so a comment
 * naming the method is not a guard - but it does not prove that the call wraps the argument actually written. A
 * {@code writeTo} that guarded one config and wrote a second unguarded one would pass. It also only recognises
 * the write forms in {@link #GENERIC_WRITE} and only sees a config whose expression says "config".
 */
public class ConfigWireBoundaryCensusTests extends ESTestCase {

    /** The forms that put a map on the wire value-by-value. {@code writeMap} takes the writer as a second argument. */
    private static final Pattern GENERIC_WRITE = Pattern.compile("out\\.write(?:GenericValue|GenericMap|Map)\\(");

    /** Comments are not code: a {@code writeTo} naming {@code wireConfig} in prose has not called it. */
    private static final Pattern COMMENTS = Pattern.compile("//[^\\n]*|/\\*.*?\\*/", Pattern.DOTALL);

    public void testEveryConfigWireBoundaryDropsTheCoordinatorOnlyKey() throws IOException {
        List<Path> roots = scannedSources();
        List<String> guarded = new ArrayList<>();
        List<String> unguarded = new ArrayList<>();
        for (Path root : roots) {
            Files.walkFileTree(root, new SimpleFileVisitor<>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    if (file.toString().endsWith(".java") == false) {
                        return FileVisitResult.CONTINUE;
                    }
                    String source = Files.readString(file, StandardCharsets.UTF_8);
                    Matcher write = GENERIC_WRITE.matcher(source);
                    while (write.find()) {
                        int signature = source.lastIndexOf("(StreamOutput ", write.start());
                        if (signature < 0) {
                            continue; // nothing takes a StreamOutput above this, so it is not a wire boundary
                        }
                        // What is WRITTEN decides whether this is a config boundary, not whether the word appears
                        // somewhere in the method - which also matches an unrelated `configuration` parameter, as
                        // DataNodeRequest's alias filters did. One hop of resolution, because a boundary may
                        // assign the stripped map to a local before writing it.
                        String body = COMMENTS.matcher(source.substring(signature, write.start())).replaceAll(" ");
                        String written = COMMENTS.matcher(argumentOf(source, write.end() - 1)).replaceAll(" ");
                        String effective = written + " " + assignmentsOf(body, written);
                        if (effective.toLowerCase(Locale.ROOT).contains("config") == false) {
                            continue; // this write carries something else
                        }
                        String site = file.getFileName().toString();
                        // Guarded means NO raw reference survives once every wireConfig(...) span is removed. Asking
                        // only whether the text appears would pass a conditional that wraps one arm and not the
                        // other - and the unwrapped arm can be the one every current node takes.
                        (unwrappedConfigReference(effective) ? unguarded : guarded).add(site);
                    }
                    return FileVisitResult.CONTINUE;
                }
            });
        }
        assertFalse("the census found no config wire boundaries, so it is checking nothing", guarded.isEmpty());
        assertEquals(
            "a writeTo carrying a query config map does not route it through ExternalSourceResolver.wireConfig, so "
                + "the coordinator-only dataset version reaches a data node. Wrap it, or say here why that boundary "
                + "is exempt.",
            List.of(),
            unguarded.stream().distinct().toList()
        );
    }

    /**
     * The esql plugin's own sources plus every {@code esql-datasource-*} module: a split or relation type added
     * in a plugin crosses the same wire, and scanning only esql main would not see it.
     */
    private static List<Path> scannedSources() {
        String probe = "x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/FileSplit.java";
        Path repo = PathUtils.get("").toAbsolutePath();
        for (int i = 0; i < 12 && repo != null && Files.exists(repo.resolve(probe)) == false; i++) {
            repo = repo.getParent();
        }
        assertNotNull("cannot locate the repository root from " + PathUtils.get("").toAbsolutePath(), repo);
        List<Path> roots = new ArrayList<>();
        roots.add(repo.resolve("x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql"));
        try (var modules = Files.newDirectoryStream(repo.resolve("x-pack/plugin"), "esql-datasource-*")) {
            for (Path module : modules) {
                Path main = module.resolve("src/main/java");
                if (Files.isDirectory(main)) {
                    roots.add(main);
                }
            }
        } catch (IOException e) {
            throw new AssertionError("cannot enumerate the esql datasource modules", e);
        }
        return roots;
    }

    /** The argument expression of a call whose opening parenthesis is at {@code open}. */
    private static String argumentOf(String source, int open) {
        int depth = 0;
        for (int i = open; i < source.length(); i++) {
            char ch = source.charAt(i);
            if (ch == '(') {
                depth++;
            } else if (ch == ')' && --depth == 0) {
                return source.substring(open + 1, i);
            }
        }
        return "";
    }

    /**
     * The right-hand sides assigned, earlier in the same method, to any local the written expression names.
     * <p>
     * Resolving EVERY identifier in the expression rather than only a bare one, because a boundary may both wrap
     * indirectly and write a conditional: one site assigns {@code wireConfig(config)} to a local and then writes
     * a ternary over it, where neither the argument alone nor a whole-argument identifier would show the guard.
     */
    private static String assignmentsOf(String body, String expression) {
        StringBuilder found = new StringBuilder();
        Matcher name = Pattern.compile("[A-Za-z_$][A-Za-z0-9_$]*").matcher(expression);
        Set<String> seen = new HashSet<>();
        while (name.find()) {
            if (seen.add(name.group()) == false) {
                continue;
            }
            Matcher assigned = Pattern.compile("\\b" + Pattern.quote(name.group()) + "\\s*=\\s*([^;]*);").matcher(body);
            while (assigned.find()) {
                found.append(assigned.group(1)).append(' ');
            }
        }
        return found.toString();
    }

    /**
     * Whether a raw {@code config} reference survives once every {@code wireConfig(...)} span is removed. That is
     * the question, rather than whether the text appears: a {@code writeTo} may wrap one arm of a conditional and
     * write the field directly on the other, and the direct arm can be the one every current node takes.
     */
    private static boolean unwrappedConfigReference(String expression) {
        StringBuilder remaining = new StringBuilder(expression);
        for (int at = remaining.indexOf("wireConfig("); at >= 0; at = remaining.indexOf("wireConfig(")) {
            int open = remaining.indexOf("(", at);
            int depth = 0;
            int end = remaining.length();
            for (int i = open; i < remaining.length(); i++) {
                char ch = remaining.charAt(i);
                if (ch == '(') {
                    depth++;
                } else if (ch == ')' && --depth == 0) {
                    end = i + 1;
                    break;
                }
            }
            remaining.delete(at, end);
        }
        return Pattern.compile("\\bconfig\\b", Pattern.CASE_INSENSITIVE).matcher(remaining).find();
    }

}
