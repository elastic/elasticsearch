/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.grok;

import org.elasticsearch.test.ESTestCase;
import org.joni.Matcher;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class WatchedMatcherTests extends ESTestCase {

    private Grok grok(String pattern, MatcherWatchdog watchdog) {
        return new Grok(GrokBuiltinPatterns.get(randomBoolean()), pattern, watchdog, logger::warn);
    }

    public void testFindsSuccessiveMatches() {
        byte[] text = "one two".getBytes(StandardCharsets.UTF_8);
        try (WatchedMatcher matcher = grok("%{WORD:w}", MatcherWatchdog.noop()).watchedMatcher(text, 0, text.length)) {
            assertThat(matcher.search(0, text.length) >= 0, equalTo(true));
            assertThat(matcher.getEnd(), equalTo(3));
            assertThat(matcher.search(3, text.length) >= 0, equalTo(true));
            assertThat(matcher.getEnd(), equalTo(7));
            assertThat(matcher.search(7, text.length), equalTo(Matcher.FAILED));
        }
    }

    public void testRegistersOnCreationAndUnregistersOnClose() {
        List<String> events = new ArrayList<>();
        MatcherWatchdog recording = new MatcherWatchdog() {
            @Override
            public void register(Matcher matcher) {
                events.add("register");
            }

            @Override
            public long maxExecutionTimeInMillis() {
                return Long.MAX_VALUE;
            }

            @Override
            public void unregister(Matcher matcher) {
                events.add("unregister");
            }
        };
        byte[] text = "word".getBytes(StandardCharsets.UTF_8);

        Grok grok = grok("%{WORD:w}", recording);
        events.clear();   // compiling the pattern also goes through the watchdog

        try (WatchedMatcher matcher = grok.watchedMatcher(text, 0, text.length)) {
            assertThat(events, equalTo(List.of("register")));
            matcher.search(0, text.length);
        }
        assertThat(events, equalTo(List.of("register", "unregister")));
    }

    public void testInterruptedSearchThrowsRatherThanReportingNoMatch() {
        // exponential backtracking, see GrokTests#testExponentialExpressions
        String pattern = "Bonsuche mit folgender Anfrage: Belegart->\\[%{WORD:param2},(?<param5>(\\s*%{NOTSPACE})*)\\] "
            + "Zustand->ABGESCHLOSSEN Kassennummer->%{WORD:param9} Bonnummer->%{WORD:param10} Datum->%{DATESTAMP_OTHER:param11}";
        byte[] text = ("Bonsuche mit folgender Anfrage: Belegart->[EINGESCHRAENKTER_VERKAUF, VERKAUF, NACHERFASSUNG] "
            + "Zustand->ABGESCHLOSSEN Kassennummer->2 Bonnummer->6362 Datum->Mon Jan 08 00:00:00 UTC 2018").getBytes(
                StandardCharsets.UTF_8
            );

        try (WatchedMatcher matcher = grok(pattern, MatcherWatchdog.newInstance(0)).watchedMatcher(text, 0, text.length)) {
            Exception e = expectThrows(RuntimeException.class, () -> matcher.search(0, text.length));
            assertThat(e.getMessage(), equalTo("grok pattern matching was interrupted after [0] ms"));
        }
    }
}
