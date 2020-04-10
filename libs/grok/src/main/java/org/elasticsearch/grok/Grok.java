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

package org.elasticsearch.grok;

import org.jcodings.specific.UTF8Encoding;
import org.joni.Matcher;
import org.joni.NameEntry;
import org.joni.Option;
import org.joni.Regex;
import org.joni.Region;
import org.joni.Syntax;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public final class Grok {

    private static final String NAME_GROUP = "name";
    private static final String SUBNAME_GROUP = "subname";
    private static final String PATTERN_GROUP = "pattern";
    private static final String DEFINITION_GROUP = "definition";
    private static final String GROK_PATTERN =
            "%\\{" +
            "(?<name>" +
            "(?<pattern>[A-z0-9]+)" +
            "(?::(?<subname>[[:alnum:]@\\[\\]_:.-]+))?" +
            ")" +
            "(?:=(?<definition>" +
            "(?:[^{}]+|\\.+)+" +
            ")" +
            ")?" + "\\}";
    private static final Regex GROK_PATTERN_REGEX = new Regex(GROK_PATTERN.getBytes(StandardCharsets.UTF_8), 0,
            GROK_PATTERN.getBytes(StandardCharsets.UTF_8).length, Option.NONE, UTF8Encoding.INSTANCE, Syntax.DEFAULT);

    private static final Map<String, String> builtinPatterns;

    static {
        try {
            builtinPatterns = loadBuiltinPatterns();
        } catch (IOException e) {
            throw new UncheckedIOException("unable to load built-in grok patterns", e);
        }
    }

    private final Map<String, String> patternBank;
    private final boolean namedCaptures;
    private final Regex compiledExpression;
    private final MatcherWatchdog matcherWatchdog;

    public Grok(Map<String, String> patternBank, String grokPattern) {
        this(patternBank, grokPattern, true, MatcherWatchdog.noop());
    }

    public Grok(Map<String, String> patternBank, String grokPattern, MatcherWatchdog matcherWatchdog) {
        this(patternBank, grokPattern, true, matcherWatchdog);
    }

    Grok(Map<String, String> patternBank, String grokPattern, boolean namedCaptures) {
        this(patternBank, grokPattern, namedCaptures, MatcherWatchdog.noop());
    }

    private Grok(Map<String, String> patternBank, String grokPattern, boolean namedCaptures, MatcherWatchdog matcherWatchdog) {
        this.patternBank = patternBank;
        this.namedCaptures = namedCaptures;
        this.matcherWatchdog = matcherWatchdog;

        for (Map.Entry<String, String> entry : patternBank.entrySet()) {
            String name = entry.getKey();
            String pattern = entry.getValue();
            forbidCircularReferences(name, new ArrayList<>(), pattern);
        }

        String expression = toRegex(grokPattern);
        byte[] expressionBytes = expression.getBytes(StandardCharsets.UTF_8);
        this.compiledExpression = new Regex(expressionBytes, 0, expressionBytes.length, Option.DEFAULT, UTF8Encoding.INSTANCE);
    }

    /**
     * Checks whether patterns reference each other in a circular manner and if so fail with an exception
     *
     * In a pattern, anything between <code>%{</code> and <code>}</code> or <code>:</code> is considered
     * a reference to another named pattern. This method will navigate to all these named patterns and
     * check for a circular reference.
     */
    private void forbidCircularReferences(String patternName, List<String> path, String pattern) {
        if (pattern.contains("%{" + patternName + "}") || pattern.contains("%{" + patternName + ":")) {
            String message;
            if (path.isEmpty()) {
                message = "circular reference in pattern [" + patternName + "][" + pattern + "]";
            } else {
                message = "circular reference in pattern [" + path.remove(path.size() - 1) + "][" + pattern +
                    "] back to pattern [" + patternName + "]";
                // add rest of the path:
                if (path.isEmpty() == false) {
                    message += " via patterns [" + String.join("=>", path) + "]";
                }
            }
            throw new IllegalArgumentException(message);
        }

        for (int i = pattern.indexOf("%{"); i != -1; i = pattern.indexOf("%{", i + 1)) {
            int begin = i + 2;
            int brackedIndex = pattern.indexOf('}', begin);
            int columnIndex = pattern.indexOf(':', begin);
            int end;
            if (brackedIndex != -1 && columnIndex == -1) {
                end = brackedIndex;
            } else if (columnIndex != -1 && brackedIndex == -1) {
                end = columnIndex;
            } else if (brackedIndex != -1 && columnIndex != -1) {
                end = Math.min(brackedIndex, columnIndex);
            } else {
                throw new IllegalArgumentException("pattern [" + pattern + "] has circular references to other pattern definitions");
            }
            String otherPatternName = pattern.substring(begin, end);
            path.add(otherPatternName);
            forbidCircularReferences(patternName, path, patternBank.get(otherPatternName));
        }
    }

    public String groupMatch(String name, Region region, String pattern) {
        int number = GROK_PATTERN_REGEX.nameToBackrefNumber(name.getBytes(StandardCharsets.UTF_8), 0,
            name.getBytes(StandardCharsets.UTF_8).length, region);
        int begin = region.beg[number];
        int end = region.end[number];
        if (begin < 0) { // no match found
            return null;
        }
        return new String(pattern.getBytes(StandardCharsets.UTF_8), begin, end - begin, StandardCharsets.UTF_8);
    }

    /**
     * converts a grok expression into a named regex expression
     *
     * @return named regex expression
     */
    public String toRegex(String grokPattern) {
        byte[] grokPatternBytes = grokPattern.getBytes(StandardCharsets.UTF_8);
        Matcher matcher = GROK_PATTERN_REGEX.matcher(grokPatternBytes);

        int result;
        try {
            matcherWatchdog.register(matcher);
            result = matcher.search(0, grokPatternBytes.length, Option.NONE);
        } finally {
            matcherWatchdog.unregister(matcher);
        }
        if (result >= 0) {
            Region region = matcher.getEagerRegion();
            String namedPatternRef = groupMatch(NAME_GROUP, region, grokPattern);
            String subName = groupMatch(SUBNAME_GROUP, region, grokPattern);
            // TODO(tal): Support definitions
            @SuppressWarnings("unused")
            String definition = groupMatch(DEFINITION_GROUP, region, grokPattern);
            String patternName = groupMatch(PATTERN_GROUP, region, grokPattern);

            String pattern = patternBank.get(patternName);
            if (pattern == null) {
                throw new IllegalArgumentException("Unable to find pattern [" + patternName + "] in Grok's pattern dictionary");
            }
            if (pattern.contains("%{" + patternName + "}") || pattern.contains("%{" + patternName + ":")) {
                throw new IllegalArgumentException("circular reference in pattern back [" + patternName + "]");
            }

            String grokPart;
            if (namedCaptures && subName != null) {
                grokPart = String.format(Locale.US, "(?<%s>%s)", namedPatternRef, pattern);
            } else if (!namedCaptures) {
                grokPart = String.format(Locale.US, "(?<%s>%s)", patternName + "_" + String.valueOf(result), pattern);
            } else {
                grokPart = String.format(Locale.US, "(?:%s)", pattern);
            }

            String start = new String(grokPatternBytes, 0, result, StandardCharsets.UTF_8);
            String rest = new String(grokPatternBytes, region.end[0], grokPatternBytes.length - region.end[0], StandardCharsets.UTF_8);
            return start + toRegex(grokPart + rest);
        }

        return grokPattern;
    }

    /**
     * Checks whether a specific text matches the defined grok expression.
     *
     * @param text the string to match
     * @return true if grok expression matches text or there is a timeout, false otherwise.
     */
    public boolean match(String text) {
        Matcher matcher = compiledExpression.matcher(text.getBytes(StandardCharsets.UTF_8));
        int result;
        try {
            matcherWatchdog.register(matcher);
            result = matcher.search(0, text.length(), Option.DEFAULT);
        } finally {
            matcherWatchdog.unregister(matcher);
        }
        return (result != -1);
    }

    /**
     * Matches and returns any named captures within a compiled grok expression that matched
     * within the provided text.
     *
     * @param text the text to match and extract values from.
     * @return a map containing field names and their respective coerced values that matched.
     */
    public Map<String, Object> captures(String text) {
        byte[] textAsBytes = text.getBytes(StandardCharsets.UTF_8);
        Matcher matcher = compiledExpression.matcher(textAsBytes);
        int result;
        try {
            matcherWatchdog.register(matcher);
            result = matcher.search(0, textAsBytes.length, Option.DEFAULT);
        } finally {
            matcherWatchdog.unregister(matcher);
        }
        if (result == Matcher.INTERRUPTED) {
            throw new RuntimeException("grok pattern matching was interrupted after [" +
                matcherWatchdog.maxExecutionTimeInMillis() + "] ms");
        } else if (result == Matcher.FAILED) {
            // TODO: I think we should throw an error here?
            return null;
        } else if (compiledExpression.numberOfNames() > 0) {
            Map<String, Object> fields = new HashMap<>();
            Region region = matcher.getEagerRegion();
            for (Iterator<NameEntry> entry = compiledExpression.namedBackrefIterator(); entry.hasNext();) {
                NameEntry e = entry.next();
                String groupName = new String(e.name, e.nameP, e.nameEnd - e.nameP, StandardCharsets.UTF_8);
                for (int number : e.getBackRefs()) {
                    if (region.beg[number] >= 0) {
                        String matchValue = new String(textAsBytes, region.beg[number], region.end[number] - region.beg[number],
                            StandardCharsets.UTF_8);
                        GrokMatchGroup match = new GrokMatchGroup(groupName, matchValue);
                        fields.put(match.getName(), match.getValue());
                        break;
                    }
                }
            }
            return fields;
        } else {
            return Collections.emptyMap();
        }
    }

    public static Map<String, String> getBuiltinPatterns() {
        return builtinPatterns;
    }

    private static Map<String, String> loadBuiltinPatterns() throws IOException {
        // Code for loading built-in grok patterns packaged with the jar file:
        String[] PATTERN_NAMES = new String[] {
            "aws", "bacula", "bind", "bro", "exim", "firewalls", "grok-patterns", "haproxy",
            "java", "junos", "linux-syslog", "maven", "mcollective-patterns", "mongodb", "nagios",
            "postgresql", "rails", "redis", "ruby", "squid"
        };
        Map<String, String> builtinPatterns = new HashMap<>();
        for (String pattern : PATTERN_NAMES) {
            try(InputStream is = Grok.class.getResourceAsStream("/patterns/" + pattern)) {
                loadPatterns(builtinPatterns, is);
            }
        }
        return Collections.unmodifiableMap(builtinPatterns);
    }

    private static void loadPatterns(Map<String, String> patternBank, InputStream inputStream) throws IOException {
        String line;
        BufferedReader br = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
        while ((line = br.readLine()) != null) {
            String trimmedLine = line.replaceAll("^\\s+", "");
            if (trimmedLine.startsWith("#") || trimmedLine.length() == 0) {
                continue;
            }

            String[] parts = trimmedLine.split("\\s+", 2);
            if (parts.length == 2) {
                patternBank.put(parts[0], parts[1]);
            }
        }
    }

}

