/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.docs;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;

import org.apache.commons.collections.map.HashedMap;
import org.gradle.api.Action;
import org.gradle.api.DefaultTask;
import org.gradle.api.InvalidUserDataException;
import org.gradle.api.file.ConfigurableFileTree;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.Internal;
import org.gradle.api.tasks.TaskAction;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
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

public class DocSnippetTask extends DefaultTask {

    private static final String SCHAR = "(?:\\\\\\/|[^\\/])";
    private static final String SUBSTITUTION = "s\\/(" + SCHAR + "+)\\/(" + SCHAR + "*)\\/";
    private static final String CATCH = "catch:\\s*((?:\\/[^\\/]+\\/)|[^ \\]]+)";
    private static final String SKIP_REGEX = "skip:([^\\]]+)";
    private static final String SETUP = "setup:([^ \\]]+)";
    private static final String TEARDOWN = "teardown:([^ \\]]+)";
    private static final String WARNING = "warning:(.+)";
    private static final String NON_JSON = "(non_json)";
    private static final String TEST_SYNTAX =
        "(?:" + CATCH + "|" + SUBSTITUTION + "|" + SKIP_REGEX + "|(continued)|" + SETUP + "|" + TEARDOWN + "|" + WARNING + "|(skip_shard_failures)) ?";

    public static final Pattern SNIPPET_PATTERN = Pattern.compile("-{4,}\\s*");
    /**
     * Action to take on each snippet. Called with a single parameter, an
     * instance of Snippet.
     */
    @Internal
    Action<Snippet> perSnippet;

    /**
     * The docs to scan. Defaults to every file in the directory exception the
     * build.gradle file because that is appropriate for Elasticsearch's docs
     * directory.
     */
    @InputFiles
    ConfigurableFileTree docs;

    /**
     * Substitutions done on every snippet's contents.
     */
    @Input
    Map<String, String> defaultSubstitutions = new HashedMap();

    Snippet emit(
        final Snippet snippet,
        String contents,
        Map<String, String> defaultSubstitutions,
        Collection<Map.Entry<String, String>> substitutions
    ) {
        snippet.contents = contents.toString();
        if (snippet.language == null) {
            throw new InvalidUserDataException(
                snippet.name
                    + ": "
                    + "Snippet missing a language. This is required by "
                    + "Elasticsearch's doc testing infrastructure so we "
                    + "be sure we don't accidentally forget to test a "
                    + "snippet."
            );
        }
        assertValidCurlInput(snippet);
        assertValidJsonInput(snippet);
        escapeSubstitutions(snippet, defaultSubstitutions, substitutions);

        if (perSnippet != null) {
            perSnippet.execute(snippet);
        }
        return snippet;
    }

    private static void escapeSubstitutions(Snippet snippet, Map<String, String> defaultSubstitutions, Collection<Map.Entry<String, String>> substitutions) {
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

    private static void assertValidCurlInput(Snippet snippet) {
        // Try to detect snippets that contain `curl`
        if (snippet.language == "sh" || snippet.language == "shell") {
            snippet.curl = snippet.contents.contains("curl");
            if (snippet.console == Boolean.FALSE && snippet.curl == false) {
                throw new InvalidUserDataException(
                    snippet.name + ": " + "No need for NOTCONSOLE if snippet doesn't " + "contain `curl`."
                );
            }
        }
    }

    private static void assertValidJsonInput(Snippet snippet) {
        if (snippet.testResponse && ("js" == snippet.language || "console-result" == snippet.language) && null == snippet.skip) {
            String quoted = snippet.contents
                // quote values starting with $
                .replaceAll("([:,])\\s*(\\$[^ ,\\n}]+)", "$1 \"$2\"")
                // quote fields starting with $
                .replaceAll("(\\$[^ ,\\n}]+)\\s*:", "\"$1\":");

            JsonFactory jf = new JsonFactory();
            jf.configure(JsonParser.Feature.ALLOW_BACKSLASH_ESCAPING_ANY_CHARACTER, true);
            JsonParser jsonParser;

            try {
                jsonParser = jf.createParser(quoted);
                while (jsonParser.isClosed() == false) {
                    jsonParser.nextToken();
                }
            } catch (JsonParseException e) {
                throw new InvalidUserDataException(
                    "Invalid json in "
                        + snippet.toString()
                        + ". The error is:\n"
                        + e.getMessage()
                        + ".\n"
                        + "After substitutions and munging, the json looks like:\n"
                        + quoted,
                    e
                );
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @TaskAction
    void executeTask() {
        for (File file : docs) {
            String lastLanguage;
            String name;
            int lastLanguageLine;
            Snippet snippet = null;
            StringBuilder contents = null;
            List<Map.Entry<String, String>> substitutions = new ArrayList<>();

            parseDocFile(docs.getDir(), file, substitutions);
            //
            // emit(snippet, contents, defaultSubstitutions, substitutions);
        }
    }

    List<Snippet> parseDocFile(File rootDir, File docFile, List<Map.Entry<String, String>> substitutions) {
        String lastLanguage = null;
        Snippet snippet = null;
        String name = null;
        int lastLanguageLine = 0;
        List<Snippet> snippets = new ArrayList<>();

        try (Stream<String> lines = java.nio.file.Files.lines(docFile.toPath(), StandardCharsets.UTF_8)) {
            List<String> linesList = lines.collect(Collectors.toList());
            for(int lineNumber=0; lineNumber<linesList.size(); lineNumber++){
                String line = linesList.get(lineNumber);
                if(SNIPPET_PATTERN.matcher(line).matches()) { //.matches() in Java works like ==~ in Groovy
                    if (snippet == null) {
                        Path path = rootDir.toPath().relativize(docFile.toPath());
                        snippet = new Snippet(path, lineNumber, name);
                        snippets.add(snippet);
                        if (lastLanguageLine == lineNumber - 1) {
                            snippet.language = lastLanguage;
                        }
                        name = null;
                    } else {
                        snippet.end = lineNumber;
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
                if(consoleHandled(docFile.getName(), lineNumber, line, snippet)) {
                    continue;
                }
                if(testHandled(docFile.getName(), lineNumber, line, snippet, substitutions)) {
                    continue;
                }


            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return snippets;
    }

    private boolean testHandled(String name, int lineNumber, String line, Snippet snippet, final List<Map.Entry<String, String>> substitutions) {
        Matcher matcher = Pattern.compile("\\/\\/\s*TEST(\\[(.+)\\])?\s*").matcher(line);
        if (matcher.matches()) {
            if (snippet == null) {
                throw new InvalidUserDataException(name + ":" + lineNumber
                    + ": TEST not paired with a snippet at ");
            }
            snippet.test = true;
            if (matcher.group(2) != null) {
                String loc = name+":" + lineNumber;
                parse(loc, matcher.group(2), TEST_SYNTAX, (Matcher m, Boolean last) -> {
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
                    throw new InvalidUserDataException(
                        "Invalid test marker: " + line);
                });
            }
            return true;
        }
        return false;
    }

    public void extraContent(String message, String s, int offset, String location, String pattern) {
        StringBuilder cutOut = new StringBuilder();
        cutOut.append(s.substring(offset - 6, offset));
        cutOut.append('*');
        cutOut.append(s.substring(offset, Math.min(offset + 5, s.length())));

        String cutOutNoNl = cutOut.toString().replace("\n", "\\n");

        throw new InvalidUserDataException(location + ": Extra content "
            + message + " ('" + cutOutNoNl + "') matching [" + pattern + "]: " + s);
    }

    /**
     * Repeatedly match the pattern to the string, calling the closure with the
     * matchers each time there is a match. If there are characters that don't
     * match then blow up. If the closure takes two parameters then the second
     * one is "is this the last match?".
     */
    protected void parse(String location, String s, String pattern, BiConsumer<Matcher,Boolean> testHandler) {
        if (s == null) {
            return; // Silly null, only real stuff gets to match!
        }
        Matcher m = Pattern.compile(pattern).matcher(s);
        int offset = 0;
        while (m.find()) {
            if (m.start() != offset) {
                extraContent("between [$offset] and [${m.start()}]", s, offset, location, pattern);
            }
            offset = m.end();
            testHandler.accept(m, offset == s.length());
        }
        if (offset == 0) {
            throw new InvalidUserDataException(location + ": Didn't match "
                +  pattern +": " + s);
        }
        if (offset != s.length()) {
            extraContent("after [" + offset+ "]", s, offset, location, pattern);
        }
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

    public static final class Source {
        boolean matches;
        String language;
        String name;

        public Source(boolean matches, String language, String name) {
            this.matches = matches;
            this.language = language;
            this.name = name;
        }
    }

    public static class Snippet {
        static final int NOT_FINISHED = -1;

        /**
         * Path to the file containing this snippet. Relative to docs.dir of the
         * SnippetsTask that created it.
         */
        Path path;
        int start;
        int end = NOT_FINISHED;
        public String contents;

        Boolean console = null;
        boolean test = false;
        boolean testResponse = false;
        boolean testSetup = false;
        boolean testTearDown = false;
        String skip = null;
        boolean continued = false;
        String language = null;
        String catchPart = null;
        String setup = null;
        String teardown = null;
        boolean curl;
        List<String> warnings = new ArrayList();
        boolean skipShardsFailures = false;
        String name;

        public Snippet(Path path, int start, String name) {
            this.path = path;
            this.start = start;
            this.name = name;
        }

    }
}
