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
 * <b>How it decides, and what it therefore catches.</b> For each write it takes the expression actually written,
 * strips comments from it, resolves every identifier it names one hop back - to that identifier's assignments in
 * the enclosing method, and to the first {@code return} of a same-file method of that name - then deletes every
 * {@code wireConfig(...)} span and fails if the word {@code config} survives. So unwiring any existing strip fails
 * it, a conditional that wraps one arm and writes the field raw on the other fails it, a comment naming the method
 * is not a guard, a helper handing the raw field back under another name fails it, and a second unguarded write
 * in the same {@code writeTo} is caught too, because each write is judged on its own argument rather than on the
 * method around it. {@link #testWhatTheCensusDecides} pins every one of those, through the same entry point the
 * file walk uses.
 * <p>
 * <b>Resolution, not a wider word test, is what closes the helper shape,</b> and the distinction is load-bearing.
 * Failing on any identifier merely CONTAINING "config" would catch {@code splitConfig()} and also redden
 * {@code ExternalSourceExec}, which writes {@code planConfig(onTheWire)} - a transform of the already-stripped
 * map, correctly guarded. The name of a helper says nothing; what its value resolves to says everything.
 * <p>
 * <b>What it does not cover,</b> stated so the next reader does not rely on more than this checks: a guard or a
 * leak applied through more than one hop, a config whose expression does not say "config", a write form outside
 * {@link #GENERIC_WRITE} or through a receiver not named {@code out}, a wire method whose {@code StreamOutput} is
 * not its first parameter (the signature probe looks for {@code "(StreamOutput "}), and any source root outside
 * the esql plugin and the {@code esql-datasource-*} modules. A helper defined in ANOTHER file is out of reach too,
 * which costs little for the shape that matters: to hand back a raw {@code config} field it would have to be
 * declared where that private field is, and one taking the field as a parameter leaves the word in the argument.
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
                    censusOf(source, file.getFileName().toString(), guarded, unguarded);
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

    /**
     * Files every config write in ONE source text under {@code site}, as guarded or unguarded. Lifted out of the
     * file walk because this is the whole decision: the walk only supplies the text. A fixture that could not
     * reach it was the reason a regression here went unnoticed between two runs of the reviewer's own mutations -
     * the predicates were testable in isolation and the walk around them was not.
     */
    private static void censusOf(String source, String site, List<String> guarded, List<String> unguarded) {
        Matcher write = GENERIC_WRITE.matcher(source);
        while (write.find()) {
            int signature = source.lastIndexOf("(StreamOutput ", write.start());
            if (signature < 0) {
                continue; // nothing takes a StreamOutput above this, so it is not a wire boundary
            }
            // What is WRITTEN decides whether this is a config boundary, not whether the word appears somewhere in
            // the method - which also matches an unrelated `configuration` parameter, as DataNodeRequest's alias
            // filters did. One hop of resolution, because a boundary may assign the stripped map to a local, or
            // hand the raw field back out of a helper, before writing it.
            String body = COMMENTS.matcher(source.substring(signature, write.start())).replaceAll(" ");
            String written = COMMENTS.matcher(argumentOf(source, write.end() - 1)).replaceAll(" ");
            String stripped = COMMENTS.matcher(source).replaceAll(" ");
            String effective = written + " " + oneHopBehind(body, stripped, written);
            if (effective.toLowerCase(Locale.ROOT).contains("config") == false) {
                continue; // this write carries something else
            }
            // Guarded means NOTHING survives once every wireConfig(...) span is removed. Asking only whether the
            // text appears would pass a conditional that wraps one arm and not the other - and the unwrapped arm
            // can be the one every current node takes.
            (unwrappedConfigReference(effective) ? unguarded : guarded).add(site);
        }
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
     * What every identifier in the written expression stands for, one hop back: the right-hand sides assigned to it
     * earlier in the same method, and the first {@code return} expression of a same-file method of that name.
     * <p>
     * Resolving EVERY identifier rather than only a bare one, because a boundary may both wrap indirectly and write
     * a conditional: one site assigns {@code wireConfig(config)} to a local and then writes a ternary over it,
     * where neither the argument alone nor a whole-argument identifier would show the guard. Resolving method names
     * as well as locals because a helper is the one shape where the name misleads in BOTH directions - see the
     * class javadoc - and only what it returns settles it.
     */
    private static String oneHopBehind(String body, String fileSource, String expression) {
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
            // Only a CALLED name has a return to resolve. Asking for every identifier would look up locals and
            // types as if they were methods, and the declaration pattern below is not tight enough to be asked
            // a question that broad.
            if (Pattern.compile("\\b" + Pattern.quote(name.group()) + "\\s*\\(").matcher(expression).find()) {
                found.append(firstReturnOf(fileSource, name.group())).append(' ');
            }
        }
        return found.toString();
    }

    /**
     * The first {@code return} expression inside a method of this name DECLARED in this file, or empty when the
     * file declares none. First rather than every one: a helper handing back a config has one, and a method whose
     * early returns are guard clauses would otherwise contribute their expressions as if they were the value.
     * <p>
     * A return type immediately before the name is what tells a declaration from a call, because a call can be
     * followed by an opening brace too - an {@code if} over a version check is exactly that shape - and the call
     * would otherwise adopt whatever the enclosing method returns next.
     */
    private static String firstReturnOf(String fileSource, String methodName) {
        Matcher declaration = Pattern.compile(
            "[A-Za-z_$][\\w.$<>\\[\\], ]*\\s" + Pattern.quote(methodName) + "\\s*\\([^)]*\\)\\s*(?:throws [^{]*)?\\{"
        ).matcher(fileSource);
        if (declaration.find()) {
            Matcher returned = Pattern.compile("\\breturn\\s+([^;]*);").matcher(fileSource);
            if (returned.find(declaration.end())) {
                return returned.group(1);
            }
        }
        return "";
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

    /**
     * The decision's own fixture, driven through the same entry point the file walk uses, so what it pins is the
     * census and not a helper underneath it. Each case is one shape named in this class's javadoc.
     */
    public void testWhatTheCensusDecides() {
        assertGuarded("a wrapped write", "        out.writeGenericMap(ExternalSourceResolver.wireConfig(config));");
        assertGuarded(
            "a guard reached through one assignment",
            "        Map<String, Object> onTheWire = ExternalSourceResolver.wireConfig(config);\n"
                + "        out.writeGenericValue(onTheWire);"
        );
        // ExternalSourceExec's live shape: a transform APPLIED TO the stripped map, whose own name says config.
        assertGuarded(
            "a transform of the stripped map",
            "        Map<String, Object> onTheWire = ExternalSourceResolver.wireConfig(config);\n"
                + "        out.writeGenericValue(v.supports(K) ? onTheWire : ExternalSourceResolver.planConfig(onTheWire));"
        );

        assertUnguarded("the raw field", "        out.writeGenericMap(config);");
        assertUnguarded(
            "the unwrapped arm of a conditional",
            "        out.writeGenericValue(v.supports(K) ? config : ExternalSourceResolver.wireConfig(config));"
        );
        assertUnguarded("a comment naming the guard", "        out.writeGenericMap(config /* wireConfig(config) */);");
        assertUnguarded(
            "a second write in the same method",
            "        out.writeGenericMap(ExternalSourceResolver.wireConfig(config));\n" + "        out.writeGenericMap(config);"
        );
        // The shape a whole-word exit test passed: a same-file helper handing the raw field back under a new name.
        assertUnguarded(
            "a helper handing the field back",
            "        out.writeGenericMap(splitConfig());",
            "    private Map<String, Object> splitConfig() {\n        return config;\n    }\n"
        );
    }

    /**
     * The same two shapes applied to a REAL boundary's source, read off disk and mutated in memory. A synthetic
     * class is a statement about the predicates; this is a statement about {@code FileSplit} as it is actually
     * written, and it costs no production edit to make. It exists because a mutation of the file on disk reported
     * the helper shape as guarded while the synthetic case reported it unguarded, and only one of those can be
     * true of this source.
     */
    public void testTheCensusDecidesTheRealBoundaryCorrectly() throws IOException {
        String source = Files.readString(fileSplitSource(), StandardCharsets.UTF_8);
        String wrapped = "out.writeGenericMap(ExternalSourceResolver.wireConfig(config));";
        assertTrue("FileSplit no longer writes its config this way; update this fixture", source.contains(wrapped));

        assertEquals("FileSplit as committed is guarded", List.of(), unguardedIn(source));
        assertEquals(
            "unwiring the strip must be caught",
            List.of("FileSplit.java"),
            unguardedIn(source.replace(wrapped, "out.writeGenericMap(config);"))
        );
        assertEquals(
            "a helper handing the field back must be caught",
            List.of("FileSplit.java"),
            unguardedIn(
                source.replace(wrapped, "out.writeGenericMap(splitConfig());")
                    .replace(
                        "    public void writeTo(StreamOutput out) throws IOException {",
                        "    private Map<String, Object> splitConfig() {\n        return config;\n    }\n\n"
                            + "    public void writeTo(StreamOutput out) throws IOException {"
                    )
            )
        );
    }

    /** The committed source of the split boundary, located the same way {@link #scannedSources} locates the roots. */
    private static Path fileSplitSource() {
        String probe = "x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/FileSplit.java";
        Path repo = PathUtils.get("").toAbsolutePath();
        for (int i = 0; i < 12 && repo != null && Files.exists(repo.resolve(probe)) == false; i++) {
            repo = repo.getParent();
        }
        assertNotNull("cannot locate the repository root", repo);
        return repo.resolve(probe);
    }

    private static List<String> unguardedIn(String source) {
        List<String> guarded = new ArrayList<>();
        List<String> unguarded = new ArrayList<>();
        censusOf(source, "FileSplit.java", guarded, unguarded);
        assertFalse("this source must carry a config write at all", guarded.isEmpty() && unguarded.isEmpty());
        return unguarded.stream().distinct().toList();
    }

    /** A plausible writer class around {@code writes}, plus any {@code members} declared beside it. */
    private static String sourceAround(String writes, String members) {
        return "class W {\n"
            + "    private final Map<String, Object> config = Map.of();\n"
            + members
            + "\n    public void writeTo(StreamOutput out) throws IOException {\n"
            + writes
            + "\n    }\n}\n";
    }

    private static void assertGuarded(String what, String writes) {
        assertEquals(what + " must be read as guarded", List.of(), decide(writes, ""));
    }

    private static void assertUnguarded(String what, String writes) {
        assertUnguarded(what, writes, "");
    }

    private static void assertUnguarded(String what, String writes, String members) {
        assertEquals(what + " must be read as unguarded", List.of("W.java"), decide(writes, members).stream().distinct().toList());
    }

    /** The unguarded sites the census finds in a synthetic source holding exactly {@code writes}. */
    private static List<String> decide(String writes, String members) {
        List<String> guarded = new ArrayList<>();
        List<String> unguarded = new ArrayList<>();
        censusOf(sourceAround(writes, members), "W.java", guarded, unguarded);
        assertFalse("the fixture's own source must contain a config write, or it proves nothing", guarded.isEmpty() && unguarded.isEmpty());
        return unguarded;
    }

}
