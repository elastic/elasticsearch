/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.doc;

import org.gradle.api.InvalidUserDataException;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class AsciidocSnippetParser implements SnippetParser {
    public static final Pattern SNIPPET_PATTERN = Pattern.compile("-{4,}\\s*");

    private static final String CATCH = "catch:\\s*((?:\\/[^\\/]+\\/)|[^ \\]]+)";
    private static final String SKIP_REGEX = "skip:([^\\]]+)";
    private static final String SETUP = "setup:([^ \\]]+)";
    private static final String TEARDOWN = "teardown:([^ \\]]+)";
    private static final String WARNING = "warning:(.+)";
    private static final String NON_JSON = "(non_json)";
    private static final String SCHAR = "(?:\\\\\\/|[^\\/])";
    private static final String SUBSTITUTION = "s\\/(" + SCHAR + "+)\\/(" + SCHAR + "*)\\/";
    private static final String TEST_SYNTAX = "(?:"
        + CATCH
        + "|"
        + SUBSTITUTION
        + "|"
        + SKIP_REGEX
        + "|(continued)|"
        + SETUP
        + "|"
        + TEARDOWN
        + "|"
        + WARNING
        + "|(skip_shard_failures)) ?";

    private final Map<String, String> defaultSubstitutions;

    public AsciidocSnippetParser(Map<String, String> defaultSubstitutions) {
        this.defaultSubstitutions = defaultSubstitutions;
    }

    @Override
    public List<Snippet> parseDoc(File rootDir, File docFile, List<Map.Entry<String, String>> substitutions) {
        String lastLanguage = null;
        Snippet snippet = null;
        String name = null;
        int lastLanguageLine = 0;
        StringBuilder contents = null;
        List<Snippet> snippets = new ArrayList<>();

        try (Stream<String> lines = Files.lines(docFile.toPath(), StandardCharsets.UTF_8)) {
            List<String> linesList = lines.collect(Collectors.toList());
            for (int lineNumber = 0; lineNumber < linesList.size(); lineNumber++) {
                String line = linesList.get(lineNumber);
                if (SNIPPET_PATTERN.matcher(line).matches()) {
                    if (snippet == null) {
                        Path path = rootDir.toPath().relativize(docFile.toPath());
                        snippet = new Snippet(path, lineNumber + 1, name);
                        snippets.add(snippet);
                        if (lastLanguageLine == lineNumber - 1) {
                            snippet.language = lastLanguage;
                        }
                        name = null;
                    } else {
                        snippet.end = lineNumber + 1;
                    }
                    continue;
                }

                Source source = matchSource(line);
                if (source.matches) {
                    lastLanguage = source.language;
                    lastLanguageLine = lineNumber;
                    name = source.name;
                    continue;
                }
                if (consoleHandled(docFile.getName(), lineNumber, line, snippet)) {
                    continue;
                }
                if (testHandled(docFile.getName(), lineNumber, line, snippet, substitutions)) {
                    continue;
                }
                if (testResponseHandled(docFile.getName(), lineNumber, line, snippet, substitutions)) {
                    continue;
                }
                if (line.matches("\\/\\/\s*TESTSETUP\s*")) {
                    snippet.testSetup = true;
                    continue;
                }
                if (line.matches("\\/\\/\s*TEARDOWN\s*")) {
                    snippet.testTearDown = true;
                    continue;
                }
                if (snippet == null) {
                    // Outside
                    continue;
                }
                if (snippet.end == Snippet.NOT_FINISHED) {
                    // Inside
                    if (contents == null) {
                        contents = new StringBuilder();
                    }
                    // We don't need the annotations
                    line = line.replaceAll("<\\d+>", "");
                    // Nor any trailing spaces
                    line = line.replaceAll("\s+$", "");
                    contents.append(line).append("\n");
                    continue;
                }
                // Allow line continuations for console snippets within lists
                if (snippet != null && line.trim().equals("+")) {
                    continue;
                }
                finalizeSnippet(snippet, contents.toString(), defaultSubstitutions, substitutions);
                substitutions = new ArrayList<>();
                ;
                snippet = null;
                contents = null;
            }
            if (snippet != null) {
                finalizeSnippet(snippet, contents.toString(), defaultSubstitutions, substitutions);
                contents = null;
                snippet = null;
                substitutions = new ArrayList<>();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return snippets;
    }

    static Snippet finalizeSnippet(
        final Snippet snippet,
        String contents,
        Map<String, String> defaultSubstitutions,
        Collection<Map.Entry<String, String>> substitutions
    ) {
        snippet.contents = contents.toString();
        snippet.validate();
        escapeSubstitutions(snippet, defaultSubstitutions, substitutions);
        return snippet;
    }

    private static void escapeSubstitutions(
        Snippet snippet,
        Map<String, String> defaultSubstitutions,
        Collection<Map.Entry<String, String>> substitutions
    ) {
        BiConsumer<String, String> doSubstitution = (pattern, subst) -> {
            /*
             * $body is really common but it looks like a
             * backreference so we just escape it here to make the
             * tests cleaner.
             */
            subst = subst.replace("$body", "\\$body");
            subst = subst.replace("$_path", "\\$_path");
            subst = subst.replace("\\n", "\n");
            snippet.contents = snippet.contents.replaceAll(pattern, subst);
        };
        defaultSubstitutions.forEach(doSubstitution);

        if (substitutions != null) {
            substitutions.forEach(e -> doSubstitution.accept(e.getKey(), e.getValue()));
        }
    }

    private boolean testResponseHandled(
        String name,
        int lineNumber,
        String line,
        Snippet snippet,
        final List<Map.Entry<String, String>> substitutions
    ) {
        Matcher matcher = Pattern.compile("\\/\\/\s*TESTRESPONSE(\\[(.+)\\])?\s*").matcher(line);
        if (matcher.matches()) {
            if (snippet == null) {
                throw new InvalidUserDataException(name + ":" + lineNumber + ": TESTRESPONSE not paired with a snippet at ");
            }
            snippet.testResponse = true;
            if (matcher.group(2) != null) {
                String loc = name + ":" + lineNumber;
                ParsingUtils.parse(
                    loc,
                    matcher.group(2),
                    "(?:" + SUBSTITUTION + "|" + NON_JSON + "|" + SKIP_REGEX + ") ?",
                    (Matcher m, Boolean last) -> {
                        if (m.group(1) != null) {
                            // TESTRESPONSE[s/adsf/jkl/]
                            substitutions.add(Map.entry(m.group(1), m.group(2)));
                        } else if (m.group(3) != null) {
                            // TESTRESPONSE[non_json]
                            substitutions.add(Map.entry("^", "/"));
                            substitutions.add(Map.entry("\n$", "\\\\s*/"));
                            substitutions.add(Map.entry("( +)", "$1\\\\s+"));
                            substitutions.add(Map.entry("\n", "\\\\s*\n "));
                        } else if (m.group(4) != null) {
                            // TESTRESPONSE[skip:reason]
                            snippet.skip = m.group(4);
                        }
                    }
                );
            }
            return true;
        }
        return false;
    }

    private boolean testHandled(String name, int lineNumber, String line, Snippet snippet, List<Map.Entry<String, String>> substitutions) {
        Matcher matcher = Pattern.compile("\\/\\/\s*TEST(\\[(.+)\\])?\s*").matcher(line);
        if (matcher.matches()) {
            if (snippet == null) {
                throw new InvalidUserDataException(name + ":" + lineNumber + ": TEST not paired with a snippet at ");
            }
            snippet.test = true;
            if (matcher.group(2) != null) {
                String loc = name + ":" + lineNumber;
                ParsingUtils.parse(loc, matcher.group(2), TEST_SYNTAX, (Matcher m, Boolean last) -> {
                    if (m.group(1) != null) {
                        snippet.catchPart = m.group(1);
                        return;
                    }
                    if (m.group(2) != null) {
                        substitutions.add(Map.entry(m.group(2), m.group(3)));
                        return;
                    }
                    if (m.group(4) != null) {
                        snippet.skip = m.group(4);
                        return;
                    }
                    if (m.group(5) != null) {
                        snippet.continued = true;
                        return;
                    }
                    if (m.group(6) != null) {
                        snippet.setup = m.group(6);
                        return;
                    }
                    if (m.group(7) != null) {
                        snippet.teardown = m.group(7);
                        return;
                    }
                    if (m.group(8) != null) {
                        snippet.warnings.add(m.group(8));
                        return;
                    }
                    if (m.group(9) != null) {
                        snippet.skipShardsFailures = true;
                        return;
                    }
                    throw new InvalidUserDataException("Invalid test marker: " + line);
                });
            }
            return true;
        }
        return false;
    }

    private boolean consoleHandled(String fileName, int lineNumber, String line, Snippet snippet) {
        if (line.matches("\\/\\/\s*CONSOLE\s*")) {
            if (snippet == null) {
                throw new InvalidUserDataException(fileName + ":" + lineNumber + ": CONSOLE not paired with a snippet");
            }
            if (snippet.console != null) {
                throw new InvalidUserDataException(fileName + ":" + lineNumber + ": Can't be both CONSOLE and NOTCONSOLE");
            }
            snippet.console = true;
            return true;
        } else if (line.matches("\\/\\/\s*NOTCONSOLE\s*")) {
            if (snippet == null) {
                throw new InvalidUserDataException(fileName + ":" + lineNumber + ": NOTCONSOLE not paired with a snippet");
            }
            if (snippet.console != null) {
                throw new InvalidUserDataException(fileName + ":" + lineNumber + ": Can't be both CONSOLE and NOTCONSOLE");
            }
            snippet.console = false;
            return true;
        }
        return false;
    }

    static Source matchSource(String line) {
        Pattern pattern = Pattern.compile("\\[\"?source\"?(?:\\.[^,]+)?,\\s*\"?([-\\w]+)\"?(,((?!id=).)*(id=\"?([-\\w]+)\"?)?(.*))?].*");
        Matcher matcher = pattern.matcher(line);
        if (matcher.matches()) {
            return new Source(true, matcher.group(1), matcher.group(5));
        }
        return new Source(false, null, null);
    }
}
