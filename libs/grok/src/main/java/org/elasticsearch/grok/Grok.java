/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.grok;

import org.jcodings.specific.UTF8Encoding;
import org.joni.Matcher;
import org.joni.NameEntry;
import org.joni.Option;
import org.joni.Regex;
import org.joni.Region;
import org.joni.Syntax;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Consumer;

public final class Grok {

    private static final String NAME_GROUP = "name";
    private static final String SUBNAME_GROUP = "subname";
    private static final String PATTERN_GROUP = "pattern";
    private static final String DEFINITION_GROUP = "definition";
    private static final String GROK_PATTERN = "%\\{"
        + "(?<name>"
        + "(?<pattern>[A-z0-9]+)"
        + "(?::(?<subname>[[:alnum:]@\\[\\]_:.-]+))?"
        + ")"
        + "(?:=(?<definition>"
        + "(?:[^{}]+|\\.+)+"
        + ")"
        + ")?"
        + "\\}";
    private static final Regex GROK_PATTERN_REGEX = new Regex(
        GROK_PATTERN.getBytes(StandardCharsets.UTF_8),
        0,
        GROK_PATTERN.getBytes(StandardCharsets.UTF_8).length,
        Option.NONE,
        UTF8Encoding.INSTANCE,
        Syntax.DEFAULT
    );

    private static final int MAX_TO_REGEX_ITERATIONS = 100_000; // sanity limit

    private final boolean namedCaptures;
    private final Regex compiledExpression;
    private final MatcherWatchdog matcherWatchdog;
    private final List<GrokCaptureConfig> captureConfig;

    public Grok(PatternBank patternBank, String grokPattern, Consumer<String> logCallBack) {
        this(patternBank, grokPattern, true, MatcherWatchdog.noop(), logCallBack);
    }

    public Grok(PatternBank patternBank, String grokPattern, MatcherWatchdog matcherWatchdog, Consumer<String> logCallBack) {
        this(patternBank, grokPattern, true, matcherWatchdog, logCallBack);
    }

    Grok(PatternBank patternBank, String grokPattern, boolean namedCaptures, Consumer<String> logCallBack) {
        this(patternBank, grokPattern, namedCaptures, MatcherWatchdog.noop(), logCallBack);
    }

    private Grok(
        PatternBank patternBank,
        String grokPattern,
        boolean namedCaptures,
        MatcherWatchdog matcherWatchdog,
        Consumer<String> logCallBack
    ) {
        this.namedCaptures = namedCaptures;
        this.matcherWatchdog = matcherWatchdog;

        String expression = toRegex(patternBank, grokPattern);
        byte[] expressionBytes = expression.getBytes(StandardCharsets.UTF_8);
        this.compiledExpression = new Regex(
            expressionBytes,
            0,
            expressionBytes.length,
            Option.DEFAULT,
            UTF8Encoding.INSTANCE,
            message -> logCallBack.accept(message)
        );

        List<GrokCaptureConfig> grokCaptureConfigs = new ArrayList<>();
        for (Iterator<NameEntry> entry = compiledExpression.namedBackrefIterator(); entry.hasNext();) {
            grokCaptureConfigs.add(new GrokCaptureConfig(entry.next()));
        }
        this.captureConfig = List.copyOf(grokCaptureConfigs);
    }

    private String groupMatch(String name, Region region, String pattern) {
        int number = GROK_PATTERN_REGEX.nameToBackrefNumber(
            name.getBytes(StandardCharsets.UTF_8),
            0,
            name.getBytes(StandardCharsets.UTF_8).length,
            region
        );
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
    protected String toRegex(PatternBank patternBank, String grokPattern) {
        StringBuilder res = new StringBuilder();
        for (int i = 0; i < MAX_TO_REGEX_ITERATIONS; i++) {
            byte[] grokPatternBytes = grokPattern.getBytes(StandardCharsets.UTF_8);
            Matcher matcher = GROK_PATTERN_REGEX.matcher(grokPatternBytes);

            int result;
            try {
                matcherWatchdog.register(matcher);
                result = matcher.search(0, grokPatternBytes.length, Option.NONE);
            } finally {
                matcherWatchdog.unregister(matcher);
            }

            if (result < 0) {
                return res.append(grokPattern).toString();
            }

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
            } else if (namedCaptures) {
                grokPart = String.format(Locale.US, "(?:%s)", pattern);
            } else {
                grokPart = String.format(Locale.US, "(?<%s>%s)", patternName + "_" + result, pattern);
            }
            String start = new String(grokPatternBytes, 0, result, StandardCharsets.UTF_8);
            String rest = new String(grokPatternBytes, region.end[0], grokPatternBytes.length - region.end[0], StandardCharsets.UTF_8);
            grokPattern = grokPart + rest;
            res.append(start);
        }
        throw new IllegalArgumentException("Can not convert grok patterns to regular expression");
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
     * Matches and returns any named captures.
     *
     * @param text the text to match and extract values from.
     * @return a map containing field names and their respective coerced values that matched or null if the pattern didn't match
     */
    public Map<String, Object> captures(String text) {
        byte[] utf8Bytes = text.getBytes(StandardCharsets.UTF_8);
        GrokCaptureExtracter.MapExtracter extracter = new GrokCaptureExtracter.MapExtracter(captureConfig);
        if (match(utf8Bytes, 0, utf8Bytes.length, extracter)) {
            return extracter.result();
        }
        return null;
    }

    /**
     * Matches and collects any named captures.
     * @param utf8Bytes array containing the text to match against encoded in utf-8
     * @param offset offset {@code utf8Bytes} of the start of the text
     * @param length length of the text to match
     * @param extracter collector for captures. {@link GrokCaptureConfig#nativeExtracter} can build these.
     * @return true if there was a match, false otherwise
     * @throws RuntimeException if there was a timeout
     */
    public boolean match(byte[] utf8Bytes, int offset, int length, GrokCaptureExtracter extracter) {
        Matcher matcher = compiledExpression.matcher(utf8Bytes, offset, offset + length);
        int result;
        try {
            matcherWatchdog.register(matcher);
            result = matcher.search(offset, offset + length, Option.DEFAULT);
        } finally {
            matcherWatchdog.unregister(matcher);
        }
        if (result == Matcher.INTERRUPTED) {
            throw new RuntimeException(
                "grok pattern matching was interrupted after [" + matcherWatchdog.maxExecutionTimeInMillis() + "] ms"
            );
        }
        if (result == Matcher.FAILED) {
            return false;
        }
        extracter.extract(utf8Bytes, offset, matcher.getEagerRegion());
        return true;
    }

    /**
     * The list of values that this {@linkplain Grok} can capture.
     */
    public List<GrokCaptureConfig> captureConfig() {
        return captureConfig;
    }

    public Regex getCompiledExpression() {
        return compiledExpression;
    }

}
