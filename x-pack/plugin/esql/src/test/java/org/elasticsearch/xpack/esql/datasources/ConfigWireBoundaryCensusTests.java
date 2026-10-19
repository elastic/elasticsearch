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
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Every boundary where a query's config map crosses the wire must drop the coordinator-only dataset version
 * first. On the relation and the exec the reason is live: a data node builds its storage provider from that map
 * and {@code StorageProviderCache} keys on the whole of it, so an older node, whose {@code FRAMEWORK_KEYS} cannot
 * know this key, fragments its client pool per dataset definition and throws at the pool's ceiling. On a split
 * the reason is defence in depth - nothing on a data node reads that key from a split today.
 * <p>
 * This fails the build on a NEW boundary; each existing one is also pinned behaviourally by its own round-trip
 * test, which cannot notice a fourth {@code writeTo} appearing somewhere else.
 * <p>
 * <b>How it decides.</b> Per write it takes the expression written, strips comments, resolves each identifier one
 * hop - to assignments in the enclosing method, and to every {@code return} of a same-file method of that name -
 * deletes each {@code wireConfig(...)} span, and fails if {@code config} survives. Where nothing resolved, the
 * name decides: an identifier it cannot follow whose name says config is a leak, unless its own argument resolves
 * to a guard, which is the {@code planConfig(onTheWire)} transform shape. Resolution before name, because
 * {@code splitConfig()} is a leak and {@code planConfig(onTheWire)} is not.
 * <p>
 * <b>What a green census LETS THROUGH, as a lower bound.</b> Every round of adversarial probing so far has taken
 * this list as closed and found a shape it had missed, so it is not closed now either, and a probe is the
 * only thing that has ever found a new one. "Lets through" rather than "reports as guarded" because the two
 * failure modes are indistinguishable to a reader: some of these are judged and called guarded, others are
 * skipped before they are judged at all, and the census is green either way.
 * <ul>
 *   <li>a resolved local MUTATED after assignment, since {@code onTheWire.putAll(config)} is not an assignment;</li>
 *   <li>a field aliased to ITSELF through a local - {@code Map&lt;String, Object&gt; rawConfig = this.rawConfig;}
 *       is a real assignment, so the name counts as resolved though it resolved to itself. Probed and confirmed
 *       still open; it is listed rather than asserted, because a case asserting the wrong answer is worse than
 *       a gap written down;</li>
 *   <li>an alias not named config and assigned outside the method body, a constructor say;</li>
 *   <li>a guard or leak more than one hop away: resolution follows one hop, and the name rule judges the
 *       identifiers it finds there rather than following them further, so {@code a = config; b = a; write(b)}
 *       is skipped;</li>
 *   <li>a write whose expression names nothing containing "config", such as {@code Splits.wireMap(this)}, which
 *       is skipped rather than judged;</li>
 *   <li>a guard called on the wrong receiver: {@code Other.wireConfig(config)} counts as the guard;</li>
 *   <li>a write outside {@link #GENERIC_WRITE}, or through a receiver not named {@code out};</li>
 *   <li>a wire method whose {@code StreamOutput} is not its first parameter;</li>
 *   <li>any source root outside the esql plugin and the {@code esql-datasource-*} modules.</li>
 * </ul>
 * So a green census is evidence about the shapes {@link #testWhatTheCensusDecides} pins, and about nothing else
 * - least of all about a shape nobody has tried.
 */
public class ConfigWireBoundaryCensusTests extends ESTestCase {

    /** The forms that put a map on the wire value-by-value. {@code writeMap} takes the writer as a second argument. */
    private static final Pattern GENERIC_WRITE = Pattern.compile("out\\.write(?:GenericValue|GenericMap|Map)\\(");

    /**
     * An assignment to one name, capturing its right-hand side. {@code =(?!=)} so that a {@code NAME == null}
     * guard clause is not read as an assignment, which resolved the very field it was guarding. One function
     * because two readers asked this question with two patterns and disagreed about that case.
     */
    private static final Function<String, Pattern> ASSIGNMENT = name -> Pattern.compile(
        "\\b" + Pattern.quote(name) + "\\s*=(?!=)\\s*([^;]*);"
    );

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
     * Files every config write in ONE source text under {@code site}. Lifted out of the file walk because this is
     * the whole decision - the walk only supplies the text - and a fixture that could not reach it let a
     * regression through while the predicates underneath stayed testable.
     */
    private static void censusOf(String source, String site, List<String> guarded, List<String> unguarded) {
        String stripped = COMMENTS.matcher(source).replaceAll(" ");
        Matcher write = GENERIC_WRITE.matcher(stripped);
        while (write.find()) {
            int signature = stripped.lastIndexOf("(StreamOutput ", write.start());
            if (signature < 0) {
                continue; // nothing takes a StreamOutput above this, so it is not a wire boundary
            }
            // What is WRITTEN decides this, not whether the word appears in the method - which also matched an
            // unrelated `configuration` parameter, as DataNodeRequest's alias filters did.
            String body = stripped.substring(signature, write.start());
            String written = argumentOf(stripped, write.end() - 1);
            String effective = written + " " + oneHopBehind(body, stripped, written);
            if (effective.toLowerCase(Locale.ROOT).contains("config") == false) {
                continue; // this write carries something else
            }
            // The name rule is asked of the resolution text as well, not only of what is written: assigning an
            // unresolvable accessor to a local hid it at hop 0 and then again at hop 1.
            Set<String> unresolved = unresolvedConfigNames(body, stripped, written);
            unresolved.addAll(unresolvedConfigNames(body, stripped, oneHopBehind(body, stripped, written)));
            // Guarded means nothing survives span removal. Asking only whether the text appears would pass a
            // conditional wrapping one arm and not the other, and the raw arm can be the live one.
            (unwrappedConfigReference(effective, unresolved) ? unguarded : guarded).add(site);
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
     * What each identifier in the written expression stands for, one hop back: what the enclosing method assigns
     * to it, and what a same-file method of that name returns. Every identifier rather than a bare one, because a
     * boundary may assign {@code wireConfig(config)} to a local and then write a ternary over it, where neither
     * the argument nor a whole-argument identifier shows the guard.
     */
    private static String oneHopBehind(String body, String fileSource, String expression) {
        StringBuilder found = new StringBuilder();
        Matcher name = Pattern.compile("[A-Za-z_$][A-Za-z0-9_$]*").matcher(expression);
        Set<String> seen = new HashSet<>();
        while (name.find()) {
            if (seen.add(name.group()) == false) {
                continue;
            }
            Matcher assigned = ASSIGNMENT.apply(name.group()).matcher(body);
            while (assigned.find()) {
                found.append(assigned.group(1)).append(' ');
            }
            // Only a CALLED name has a return to resolve. Asking for every identifier would look up locals and
            // types as if they were methods, and the declaration pattern is not tight enough to be asked a
            // question that broad.
            if (Pattern.compile("\\b" + Pattern.quote(name.group()) + "\\s*\\(").matcher(expression).find()) {
                found.append(returnsOf(fileSource, name.group())).append(' ');
            }
        }
        return found.toString();
    }

    /**
     * EVERY {@code return} inside a method of this name DECLARED in this file, bounded by its closing brace. Every
     * one rather than the first, because a helper returning the stripped map on one branch and the raw field on
     * another is a leak whose first {@code return} is clean; an unrelated early {@code return} can only add text
     * and so only over-reports, which is the safe direction. A return type immediately before the name is what
     * tells a declaration from a call, since a call can be followed by a brace too.
     */
    private static String returnsOf(String fileSource, String methodName) {
        Matcher declaration = Pattern.compile(
            "[A-Za-z_$][\\w.$<>\\[\\], ]*\\s" + Pattern.quote(methodName) + "\\s*\\([^)]*\\)\\s*(?:throws [^{]*)?\\{"
        ).matcher(fileSource);
        StringBuilder found = new StringBuilder();
        while (declaration.find()) {
            int open = fileSource.indexOf('{', declaration.end() - 1);
            int depth = 0;
            int close = fileSource.length();
            for (int i = open; i < fileSource.length(); i++) {
                char ch = fileSource.charAt(i);
                if (ch == '{') {
                    depth++;
                } else if (ch == '}' && --depth == 0) {
                    close = i;
                    break;
                }
            }
            Matcher returned = Pattern.compile("\\breturn\\s+([^;]*);").matcher(fileSource.substring(open, close));
            while (returned.find()) {
                found.append(returned.group(1)).append(' ');
            }
        }
        return found.toString();
    }

    /** Whether an identifier's own name claims to be about a config - the only thing knowable about an unresolved one. */
    private static boolean namesSomethingCalledConfig(String identifier) {
        return identifier.toLowerCase(Locale.ROOT).contains("config");
    }

    /**
     * Where a call to {@code wireConfig} starts, or -1. On a word boundary: a plain substring search erased
     * {@code rewireConfig(config)} as though it were the guard.
     */
    private static int guardCallAt(CharSequence expression) {
        Matcher call = Pattern.compile("\\bwireConfig\\s*\\(").matcher(expression);
        return call.find() ? call.start() : -1;
    }

    /**
     * Whether a config-named call's argument resolves to a {@code wireConfig(...)} span - the transform shape, as
     * {@code ExternalSourceExec} writes {@code planConfig(onTheWire)}. Resolved rather than merely counted,
     * because a helper in another file can take an argument and still leak: {@code Splits.splitConfig(this)}
     * passed when the test was only that one existed, and {@code FileSplit.config()} is a public accessor.
     */
    private static boolean callArgumentResolvesToAGuard(String body, String fileSource, String expression, String identifier) {
        Matcher call = Pattern.compile("\\b" + Pattern.quote(identifier) + "\\s*\\(").matcher(expression);
        if (call.find() == false) {
            return false;
        }
        String argument = argumentOf(expression, call.end() - 1);
        if (argument.isBlank()) {
            return false;
        }
        return guardCallAt(argument + " " + oneHopBehind(body, fileSource, argument)) >= 0;
    }

    /**
     * The config-named identifiers that resolved to NOTHING - no assignment in the method, no {@code return} in a
     * same-file method of that name - so whatever each stands for is written where this scan cannot see it.
     * Counting the unseeable as guarded let two probed shapes through.
     * <p>
     * Which of them matters is decided after the {@code wireConfig(...)} spans are removed, not here: the raw
     * field is unresolved at every site including the wrapped ones, and deciding before removal reported all three
     * live boundaries unguarded.
     */
    private static Set<String> unresolvedConfigNames(String body, String fileSource, String expression) {
        Set<String> unresolved = new HashSet<>();
        Matcher name = Pattern.compile("[A-Za-z_$][A-Za-z0-9_$]*").matcher(expression);
        while (name.find()) {
            String identifier = name.group();
            if (identifier.toLowerCase(Locale.ROOT).contains("config") == false) {
                continue;
            }
            if (callArgumentResolvesToAGuard(body, fileSource, expression, identifier)) {
                continue; // its argument IS the already-stripped map: the transform shape, not a leak
            }
            boolean assigned = ASSIGNMENT.apply(identifier).matcher(body).find();
            if (assigned == false && returnsOf(fileSource, identifier).isEmpty()) {
                unresolved.add(identifier);
            }
        }
        return unresolved;
    }

    /**
     * Whether a raw {@code config} reference survives once every {@code wireConfig(...)} span is removed. That is
     * the question, rather than whether the text appears: a {@code writeTo} may wrap one arm of a conditional and
     * write the field directly on the other, and the direct arm can be the one every current node takes.
     */
    private static boolean unwrappedConfigReference(String expression, Set<String> unresolved) {
        StringBuilder remaining = new StringBuilder(expression);
        for (int at = guardCallAt(remaining); at >= 0; at = guardCallAt(remaining)) {
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
        if (Pattern.compile("\\bconfig\\b", Pattern.CASE_INSENSITIVE).matcher(remaining).find()) {
            return true;
        }
        // Nothing the scan could follow survived, but an identifier it could NOT follow may have. Asked here
        // rather than during resolution, because the raw field is unresolved at a wrapped site too and its
        // mention lives inside the span just removed.
        for (String name : unresolved) {
            if (Pattern.compile("\\b" + Pattern.quote(name) + "\\b").matcher(remaining).find()) {
                return true;
            }
        }
        return false;
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
        // ExternalSourceExec's live shape, and the reason the name alone cannot decide.
        assertGuarded(
            "a transform of the stripped map",
            "        Map<String, Object> onTheWire = ExternalSourceResolver.wireConfig(config);\n"
                + "        out.writeGenericValue(v.supports(K) ? onTheWire : ExternalSourceResolver.planConfig(onTheWire));"
        );

        // Every case below is a leak. The last five are shapes an external probe got past earlier versions of
        // this scan; each assertion's message names the shape, so the code is left to speak for itself.
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
        assertUnguarded(
            "a helper handing the field back",
            "        out.writeGenericMap(splitConfig());",
            "    private Map<String, Object> splitConfig() {\n        return config;\n    }\n"
        );
        assertUnguarded(
            "a helper that leaks on its second branch",
            "        out.writeGenericMap(maybeStripped());",
            "    private Map<String, Object> maybeStripped() {\n"
                + "        if (v.supports(K)) {\n            return ExternalSourceResolver.wireConfig(config);\n        }\n"
                + "        return config;\n    }\n"
        );
        assertUnguarded("an accessor declared elsewhere", "        out.writeGenericMap(getConfig());");
        assertUnguarded("a raw field named configuration", "        out.writeGenericMap(configuration);");
        assertUnguarded("a cross-file helper taking an argument", "        out.writeGenericMap(Splits.splitConfig(this));");
        assertUnguarded(
            "a helper whose first return is a guard clause",
            "        out.writeGenericMap(splitConfig());",
            "    private Map<String, Object> splitConfig() {\n"
                + "        if (config.isEmpty()) {\n            return Map.of();\n        }\n"
                + "        return config;\n    }\n"
        );
        assertUnguarded("a call whose name ends in the guard's", "        out.writeGenericMap(rewireConfig(config));");
        assertGuarded(
            "a commented-out raw write beside a wrapped one",
            "        out.writeGenericMap(ExternalSourceResolver.wireConfig(config));\n" + "        // out.writeGenericMap(config);"
        );
        // An unresolvable accessor assigned to a local first. The name rule saw it at hop 0 and then missed it at
        // hop 1, so it is now asked of the resolution text too.
        assertUnguarded(
            "an accessor declared elsewhere, through a local",
            "        Map<String, Object> m = getConfig();\n        out.writeGenericMap(m);"
        );
        assertUnguarded(
            "a cross-file helper, through a local",
            "        Map<String, Object> m = Splits.splitConfig(this);\n        out.writeGenericMap(m);"
        );
        // A `== null` guard clause read as an assignment marked the field it guarded resolved.
        assertUnguarded(
            "a field guarded by an equality test",
            "        if (rawConfig == null) {\n            throw new IllegalStateException();\n        }\n"
                + "        out.writeGenericMap(rawConfig);",
            "    private Map<String, Object> rawConfig;\n"
        );
        // Overloads: the clean one is declared first, so reading only the first declaration passed the leak.
        assertUnguarded(
            "an overload whose other declaration leaks",
            "        out.writeGenericMap(splitConfig());",
            "    private Map<String, Object> splitConfig(int n) {\n"
                + "        return ExternalSourceResolver.wireConfig(config);\n    }\n"
                + "    private Map<String, Object> splitConfig() {\n        return config;\n    }\n"
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
