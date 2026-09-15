/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.core.IOUtils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * Repairs golden version declarations the compatibility floor has passed. Once no compatible version is below a
 * declaration, its {@code before_<name>} directory and its {@code expectationChangesAt}/{@code since} call describe
 * planning nobody can run anymore. Under {@code -Dgolden.gc.fix} the runner calls this instead of failing, so the PR
 * that moved the floor is repaired by one Gradle task rather than by hand.
 */
final class GoldenGc {

    private GoldenGc() {}

    /** What a repair pass did, so the runner can tell "already done" from "needs a human". */
    enum Outcome {
        REPAIRED,
        ABSENT,
        UNMATCHED
    }

    /** A call with its argument; a comment trailing it on the same line goes with it. */
    private static final Pattern CALL = Pattern.compile(
        "\\s*\\.(?:expectationChangesAt|since)\\(((?:[^()]|\\([^()]*\\))*)\\)(?:[ \\t]*//[^\\n]*)?"
    );

    static boolean fixMode() {
        return System.getProperty("golden.gc.fix") != null;
    }

    /** Deletes every directory called {@code dirName} under {@code root}; none left is already clean. */
    static void deleteDirectoriesNamed(Path root, String dirName) throws IOException {
        if (Files.notExists(root)) {
            return;
        }
        List<Path> dirs;
        try (Stream<Path> walk = Files.walk(root)) {
            dirs = walk.filter(p -> Files.isDirectory(p) && p.getFileName().toString().equals(dirName)).toList();
        }
        for (Path dir : dirs) {
            IOUtils.rm(dir);
        }
    }

    /**
     * Removes every {@code .expectationChangesAt(...)} and {@code .since(...)} of {@code versionName} from {@code javaSource},
     * whether written as a literal, through a {@code static final String} constant, or through a version constant whose
     * name resembles the version's, and drops the string constant once nothing else in the file uses it. Returns whether
     * the file changed, so a second pass is a no-op.
     */
    static boolean removeDeclarations(Path javaSource, String versionName) throws IOException {
        String source = Files.readString(javaSource);
        String repaired = removeDeclarations(source, versionName);
        if (repaired.equals(source)) {
            return false;
        }
        Files.writeString(javaSource, repaired);
        return true;
    }

    static String removeDeclarations(String source, String versionName) {
        String literal = '"' + versionName + '"';
        List<String> constants = constantsHolding(source, literal);
        StringBuilder repaired = new StringBuilder();
        Matcher call = CALL.matcher(source);
        int copied = 0;
        while (call.find()) {
            if (names(call.group(1), versionName, literal, constants)) {
                repaired.append(source, copied, call.start());
                copied = call.end();
            }
        }
        repaired.append(source, copied, source.length());
        String result = repaired.toString();
        for (String constant : constants) {
            Pattern declaration = Pattern.compile(
                "^[ \\t]*(?:private|protected|public)?[ \\t]*static[ \\t]+final[ \\t]+String[ \\t]+"
                    + Pattern.quote(constant)
                    + "[ \\t]*=[ \\t]*"
                    + Pattern.quote(literal)
                    + "[ \\t]*;[^\\n]*\\n",
                Pattern.MULTILINE
            );
            String withoutDeclaration = declaration.matcher(result).replaceFirst("");
            if (Pattern.compile("\\b" + Pattern.quote(constant) + "\\b").matcher(withoutDeclaration).find() == false) {
                result = withoutDeclaration;
            }
        }
        return result;
    }

    /**
     * Whether any declaration in {@code source} looks like it is about {@code versionName} under a looser reading than
     * {@link #removeDeclarations} uses. True after a repair changed nothing means a shape the rewrite refused, not an
     * already-repaired file.
     */
    static boolean mentions(String source, String versionName) {
        String name = normalize(versionName);
        Matcher call = CALL.matcher(source);
        while (call.find()) {
            String token = lastSegment(call.group(1));
            if (token.length() >= 6 && (token.contains(name) || name.contains(token))) {
                return true;
            }
        }
        return false;
    }

    /**
     * A literal or string constant matches exactly. A version constant matches by name: {@code Sum.ESQL_SUM_LONG_OVERFLOW_FIX},
     * {@code DimensionValues.DIMENSION_VALUES_VERSION} and {@code MvSingleValueOrNull.MV_SINGLE_VALUE_OR_NULL_TRANSPORT_VERSION}
     * all name their version, give or take a prefix and a {@code _VERSION} suffix. {@code TS_COLLAPSE_V2} does not name
     * {@code ts_collapse}.
     */
    private static boolean names(String argument, String versionName, String literal, List<String> constants) {
        String trimmed = argument.strip();
        if (trimmed.equals(literal) || constants.contains(trimmed)) {
            return true;
        }
        String token = lastSegment(trimmed);
        for (String suffix : new String[] { "transportversion", "version" }) {
            if (token.endsWith(suffix)) {
                token = token.substring(0, token.length() - suffix.length());
                break;
            }
        }
        String name = normalize(versionName);
        return token.length() >= 6 && (name.equals(token) || name.endsWith(token) || token.endsWith(name));
    }

    private static String lastSegment(String argument) {
        String trimmed = argument.strip();
        return normalize(trimmed.substring(trimmed.lastIndexOf('.') + 1));
    }

    private static String normalize(String identifier) {
        return identifier.toLowerCase(Locale.ROOT).replaceAll("[^a-z0-9]", "");
    }

    private static List<String> constantsHolding(String source, String literal) {
        Matcher m = Pattern.compile("static[ \\t]+final[ \\t]+String[ \\t]+(\\w+)[ \\t]*=[ \\t]*" + Pattern.quote(literal) + "[ \\t]*;")
            .matcher(source);
        List<String> constants = new ArrayList<>();
        while (m.find()) {
            constants.add(m.group(1));
        }
        return constants;
    }
}
