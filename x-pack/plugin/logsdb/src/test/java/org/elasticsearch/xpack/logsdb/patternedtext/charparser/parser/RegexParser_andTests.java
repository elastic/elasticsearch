/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.patternedtext.charparser.parser;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.api.Argument;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.api.IPv4Argument;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.api.IntegerArgument;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.api.PatternedMessage;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.api.Timestamp;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;

public class RegexParser_andTests extends ESTestCase {

    private static final Pattern IPV4_PATTERN = Pattern.compile("\\b(\\d{1,3}(?:\\.\\d{1,3}){3})\\b");
    private static final Pattern INTEGER_PATTERN = Pattern.compile("\\b\\d+\\b");

    // New timestamp pattern and format
    private static final Pattern TIMESTAMP_1_PATTERN = Pattern.compile("\\b\\d{2}/[A-Za-z]{3}/\\d{4}:\\d{2}:\\d{2}:\\d{2} [+-]\\d{4}\\b");
    private static final String TIMESTAMP_1_FORMAT = "dd/MMM/yyyy:HH:mm:ss Z";
    private static final ThreadLocal<SimpleDateFormat> TIMESTAMP_1_FORMATTER = ThreadLocal.withInitial(
        () -> new SimpleDateFormat(TIMESTAMP_1_FORMAT, Locale.ENGLISH)
    );

    // Existing timestamp pattern and format
    private static final Pattern TIMESTAMP_2_PATTERN = Pattern.compile(
        "\\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec) \\d{2}, \\d{4} \\d{2}:\\d{2}:\\d{2} (?:AM|PM)\\b"
    );
    private static final String TIMESTAMP_2_FORMAT = "MMM dd, yyyy hh:mm:ss a";
    private static final ThreadLocal<SimpleDateFormat> TIMESTAMP_2_FORMATTER = ThreadLocal.withInitial(
        () -> new SimpleDateFormat(TIMESTAMP_2_FORMAT, Locale.ENGLISH)
    );

    private final StringBuilder patternBuilder = new StringBuilder();
    private final List<Argument<?>> arguments = new ArrayList<>();

    public void testParsing() {
        RegexParser_andTests parser = new RegexParser_andTests();
        PatternedMessage patternedMessage = parser.parse("Oct 05, 2023 02:48:00 PM INFO Response from 127.0.0.1 took 2000 ms");
        assertEquals("%T INFO Response from %4 took %I ms", patternedMessage.pattern());
        Argument<?>[] arguments = patternedMessage.arguments();
        assertEquals(2, arguments.length);
        assertEquals("IPV4", arguments[0].type().name());
        assertEquals("INTEGER", arguments[1].type().name());
        assertNotNull(patternedMessage.timestamp());

        // Test new timestamp format
        patternedMessage = parser.parse("05/Oct/2023:14:48:00 +0200 GET /index.html 127.0.0.1 200");
        assertEquals("%T GET /index.html %4 %I", patternedMessage.pattern());
        Argument<?>[] args2 = patternedMessage.arguments();
        assertEquals(2, args2.length);
        assertEquals("IPV4", args2[0].type().name());
        assertNotNull(patternedMessage.timestamp());
    }

    public PatternedMessage parse(String rawMessage) {
        if (rawMessage == null || rawMessage.isEmpty()) {
            throw new IllegalArgumentException("rawMessage cannot be null or empty");
        }

        patternBuilder.setLength(0);
        arguments.clear();
        Timestamp timestampArg = null;

        // 1. Find and extract timestamp substring (prefer TIMESTAMP_1, then TIMESTAMP_2)
        int tsStart = -1, tsEnd = -1;
        String tsString = null;
        SimpleDateFormat usedFormatter = null;

        java.util.regex.Matcher ts1Matcher = TIMESTAMP_1_PATTERN.matcher(rawMessage);
        if (ts1Matcher.find()) {
            tsString = ts1Matcher.group();
            tsStart = ts1Matcher.start();
            tsEnd = ts1Matcher.end();
            usedFormatter = TIMESTAMP_1_FORMATTER.get();
        } else {
            java.util.regex.Matcher ts2Matcher = TIMESTAMP_2_PATTERN.matcher(rawMessage);
            if (ts2Matcher.find()) {
                tsString = ts2Matcher.group();
                tsStart = ts2Matcher.start();
                tsEnd = ts2Matcher.end();
                usedFormatter = TIMESTAMP_2_FORMATTER.get();
            }
        }

        if (tsString != null) {
            patternBuilder.append(rawMessage, 0, tsStart);
            try {
                Date date = usedFormatter.parse(tsString);
                timestampArg = new Timestamp(date.getTime(), usedFormatter.toPattern());
                patternBuilder.append("%T");
            } catch (ParseException e) {
                patternBuilder.append(tsString);
            }
        }

        // 2. Tokenize and process the rest
        String remainingMessage = tsEnd >= 0 ? rawMessage.substring(tsEnd) : rawMessage;
        String[] tokens = remainingMessage.split("\\s+");
        for (int i = 0; i < tokens.length; i++) {
            String token = tokens[i];
            if (IPV4_PATTERN.matcher(token).matches()) {
                patternBuilder.append("%4");
                String[] octets = token.split("\\.");
                int[] octetValues = new int[4];
                for (int j = 0; j < 4; j++) {
                    octetValues[j] = Integer.parseInt(octets[j]);
                }
                arguments.add(new IPv4Argument(octetValues));
            } else if (INTEGER_PATTERN.matcher(token).matches()) {
                patternBuilder.append("%I");
                arguments.add(new IntegerArgument(Integer.parseInt(token)));
            } else {
                patternBuilder.append(token);
            }
            if (i != tokens.length - 1) {
                patternBuilder.append(" ");
            }
        }

        return new PatternedMessage(patternBuilder.toString(), timestampArg, arguments.toArray(new Argument<?>[0]));
    }
}
