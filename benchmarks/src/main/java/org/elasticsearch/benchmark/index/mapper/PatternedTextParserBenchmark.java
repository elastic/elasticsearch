/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.index.mapper;

import org.elasticsearch.xpack.logsdb.patternedtext.charparser.api.Argument;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.api.IPv4Argument;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.api.IntegerArgument;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.api.ParseException;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.api.Parser;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.api.ParserFactory;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.api.PatternedMessage;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.api.Timestamp;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.text.SimpleDateFormat;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

/**
 * Run using the following command: ./gradlew -p benchmarks run --args 'PatternedTextParserBenchmark'
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
public class PatternedTextParserBenchmark {

    private Parser parser;
    private RegexParser regexParser;
    private String testMessageWithComma;
    private String testMessageNoComma;
    @SuppressWarnings("FieldCanBeLocal") // used for measurement of timestamp parsing overhead
    private DateTimeFormatter dateTimeFormatter;

    @Setup
    public void setup() {
        parser = ParserFactory.createParser();
        regexParser = new RegexParser();
        testMessageWithComma = "Oct 05, 2023 02:48:00 PM INFO Response from 127.0.0.1 took 2000 ms";
        testMessageNoComma = "Oct 05 2023 02:48:00 PM INFO Response from 127.0.0.1 took 2000 ms";
        dateTimeFormatter = DateTimeFormatter.ofPattern("MMM dd yyyy hh:mm:ss a").withLocale(java.util.Locale.US);
    }

    @Benchmark
    public void parseWithCharParser(Blackhole blackhole) throws ParseException {
        PatternedMessage result = parser.parse(testMessageNoComma);
        blackhole.consume(result);
        // long timestamp = TimestampFormat.parseTimestamp(dateTimeFormatter, "Oct 05 2023 02:48:00 PM");
        // blackhole.consume(timestamp);
    }

    @Benchmark
    public void parseWithRegexParser(Blackhole blackhole) {
        PatternedMessage result = regexParser.parse(testMessageWithComma);
        blackhole.consume(result);
    }

    private static class RegexParser {
        private static final Pattern IPV4_PATTERN = Pattern.compile("\\b(\\d{1,3}(?:\\.\\d{1,3}){3})\\b");
        private static final Pattern INTEGER_PATTERN = Pattern.compile("\\b\\d+\\b");

        // New timestamp pattern and format
        private static final Pattern TIMESTAMP_1_PATTERN = Pattern.compile(
            "\\b\\d{2}/[A-Za-z]{3}/\\d{4}:\\d{2}:\\d{2}:\\d{2} [+-]\\d{4}\\b"
        );
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
                } catch (java.text.ParseException e) {
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
}
