/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.gradle.doc

import org.gradle.api.DefaultTask
import org.gradle.api.InvalidUserDataException
import org.gradle.api.file.ConfigurableFileTree
import org.gradle.api.tasks.InputFiles
import org.gradle.api.tasks.TaskAction

import java.nio.file.Path
import java.util.regex.Matcher

/**
 * A task which will run a closure on each snippet in the documentation.
 */
public class SnippetsTask extends DefaultTask {
    private static final String SCHAR = /(?:\\\/|[^\/])/
    private static final String SUBSTITUTION = /s\/($SCHAR+)\/($SCHAR*)\//
    private static final String CATCH = /catch:\s*((?:\/[^\/]+\/)|[^ \]]+)/
    private static final String SKIP = /skip:([^\]]+)/
    private static final String SETUP = /setup:([^ \]]+)/
    private static final String WARNING = /warning:(.+)/
    private static final String TEST_SYNTAX =
        /(?:$CATCH|$SUBSTITUTION|$SKIP|(continued)|$SETUP|$WARNING) ?/

    /**
     * Action to take on each snippet. Called with a single parameter, an
     * instance of Snippet.
     */
    Closure perSnippet

    /**
     * The docs to scan. Defaults to every file in the directory exception the
     * build.gradle file because that is appropriate for Elasticsearch's docs
     * directory.
     */
    @InputFiles
    ConfigurableFileTree docs = project.fileTree(project.projectDir) {
        // No snippets in the build file
        exclude 'build.gradle'
        // That is where the snippets go, not where they come from!
        exclude 'build'
    }

    @TaskAction
    public void executeTask() {
        /*
         * Walks each line of each file, building snippets as it encounters
         * the lines that make up the snippet.
         */
        for (File file: docs) {
            String lastLanguage
            int lastLanguageLine
            Snippet snippet = null
            StringBuilder contents = null
            List substitutions = null
            Closure emit = {
                snippet.contents = contents.toString()
                contents = null
                if (substitutions != null) {
                    substitutions.each { String pattern, String subst ->
                        /*
                         * $body is really common but it looks like a
                         * backreference so we just escape it here to make the
                         * tests cleaner.
                         */
                        subst = subst.replace('$body', '\\$body')
                        // \n is a new line....
                        subst = subst.replace('\\n', '\n')
                        snippet.contents = snippet.contents.replaceAll(
                            pattern, subst)
                    }
                    substitutions = null
                }
                perSnippet(snippet)
                snippet = null
            }
            file.eachLine('UTF-8') { String line, int lineNumber ->
                Matcher matcher
                if (line ==~ /-{4,}\s*/) { // Four dashes looks like a snippet
                    if (snippet == null) {
                        Path path = docs.dir.toPath().relativize(file.toPath())
                        snippet = new Snippet(path: path, start: lineNumber)
                        if (lastLanguageLine == lineNumber - 1) {
                            snippet.language = lastLanguage
                        }
                    } else {
                        snippet.end = lineNumber
                    }
                    return
                }
                matcher = line =~ /\[source,(\w+)]\s*/
                if (matcher.matches()) {
                    lastLanguage = matcher.group(1)
                    lastLanguageLine = lineNumber
                    return
                }
                if (line ==~ /\/\/\s*AUTOSENSE\s*/) {
                    throw new InvalidUserDataException("AUTOSENSE has been " +
                            "replaced by CONSOLE. Use that instead at " +
                            "$file:$lineNumber")
                }
                if (line ==~ /\/\/\s*CONSOLE\s*/) {
                    if (snippet == null) {
                        throw new InvalidUserDataException("CONSOLE not " +
                            "paired with a snippet at $file:$lineNumber")
                    }
                    snippet.console = true
                    return
                }
                matcher = line =~ /\/\/\s*TEST(\[(.+)\])?\s*/
                if (matcher.matches()) {
                    if (snippet == null) {
                        throw new InvalidUserDataException("TEST not " +
                            "paired with a snippet at $file:$lineNumber")
                    }
                    snippet.test = true
                    if (matcher.group(2) != null) {
                        String loc = "$file:$lineNumber"
                        parse(loc, matcher.group(2), TEST_SYNTAX) {
                            if (it.group(1) != null) {
                                snippet.catchPart = it.group(1)
                                return
                            }
                            if (it.group(2) != null) {
                                if (substitutions == null) {
                                    substitutions = []
                                }
                                substitutions.add([it.group(2), it.group(3)])
                                return
                            }
                            if (it.group(4) != null) {
                                snippet.skipTest = it.group(4)
                                return
                            }
                            if (it.group(5) != null) {
                                snippet.continued = true
                                return
                            }
                            if (it.group(6) != null) {
                                snippet.setup = it.group(6)
                                return
                            }
                            if (it.group(7) != null) {
                                snippet.warnings.add(it.group(7))
                                return
                            }
                            throw new InvalidUserDataException(
                                    "Invalid test marker: $line")
                        }
                    }
                    return
                }
                matcher = line =~ /\/\/\s*TESTRESPONSE(\[(.+)\])?\s*/
                if (matcher.matches()) {
                    if (snippet == null) {
                        throw new InvalidUserDataException("TESTRESPONSE not " +
                            "paired with a snippet at $file:$lineNumber")
                    }
                    snippet.testResponse = true
                    if (matcher.group(2) != null) {
                        if (substitutions == null) {
                            substitutions = []
                        }
                        String loc = "$file:$lineNumber"
                        parse(loc, matcher.group(2), /$SUBSTITUTION ?/) {
                            substitutions.add([it.group(1), it.group(2)])
                        }
                    }
                    return
                }
                if (line ==~ /\/\/\s*TESTSETUP\s*/) {
                    snippet.testSetup = true
                    return
                }
                if (snippet == null) {
                    // Outside
                    return
                }
                if (snippet.end == Snippet.NOT_FINISHED) {
                    // Inside
                    if (contents == null) {
                        contents = new StringBuilder()
                    }
                    // We don't need the annotations
                    line = line.replaceAll(/<\d+>/, '')
                    // Nor any trailing spaces
                    line = line.replaceAll(/\s+$/, '')
                    contents.append(line).append('\n')
                    return
                }
                // Just finished
                emit()
            }
            if (snippet != null) emit()
        }
    }

    static class Snippet {
        static final int NOT_FINISHED = -1

        /**
         * Path to the file containing this snippet. Relative to docs.dir of the
         * SnippetsTask that created it.
         */
        Path path
        int start
        int end = NOT_FINISHED
        String contents

        boolean console = false
        boolean test = false
        boolean testResponse = false
        boolean testSetup = false
        String skipTest = null
        boolean continued = false
        String language = null
        String catchPart = null
        String setup = null
        List warnings = new ArrayList()

        @Override
        public String toString() {
            String result = "$path[$start:$end]"
            if (language != null) {
                result += "($language)"
            }
            if (console) {
                result += '// CONSOLE'
            }
            if (test) {
                result += '// TEST'
                if (catchPart) {
                    result += "[catch: $catchPart]"
                }
                if (skipTest) {
                    result += "[skip=$skipTest]"
                }
                if (continued) {
                    result += '[continued]'
                }
                if (setup) {
                    result += "[setup:$setup]"
                }
                for (String warning in warnings) {
                    result += "[warning:$warning]"
                }
            }
            if (testResponse) {
                result += '// TESTRESPONSE'
            }
            if (testSetup) {
                result += '// TESTSETUP'
            }
            return result
        }
    }

    /**
     * Repeatedly match the pattern to the string, calling the closure with the
     * matchers each time there is a match. If there are characters that don't
     * match then blow up. If the closure takes two parameters then the second
     * one is "is this the last match?".
     */
    protected parse(String location, String s, String pattern, Closure c) {
        if (s == null) {
            return // Silly null, only real stuff gets to match!
        }
        Matcher m = s =~ pattern
        int offset = 0
        Closure extraContent = { message ->
            StringBuilder cutOut = new StringBuilder()
            cutOut.append(s[offset - 6..offset - 1])
            cutOut.append('*')
            cutOut.append(s[offset..Math.min(offset + 5, s.length() - 1)])
            String cutOutNoNl = cutOut.toString().replace('\n', '\\n')
            throw new InvalidUserDataException("$location: Extra content "
                + "$message ('$cutOutNoNl') matching [$pattern]: $s")
        }
        while (m.find()) {
            if (m.start() != offset) {
                extraContent("between [$offset] and [${m.start()}]")
            }
            offset = m.end()
            if (c.maximumNumberOfParameters == 1) {
                c(m)
            } else {
                c(m, offset == s.length())
            }
        }
        if (offset == 0) {
            throw new InvalidUserDataException("$location: Didn't match "
                + "$pattern: $s")
        }
        if (offset != s.length()) {
            extraContent("after [$offset]")
        }
    }
}
