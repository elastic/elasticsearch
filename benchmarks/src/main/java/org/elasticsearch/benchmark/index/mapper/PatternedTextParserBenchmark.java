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
import java.util.regex.Matcher;
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
        List<Argument<?>> arguments = parser.parse(testMessageNoComma);
        blackhole.consume(arguments);
        // long timestamp = TimestampFormat.parseTimestamp(dateTimeFormatter, "Oct 05 2023 02:48:00 PM");
        // blackhole.consume(timestamp);
    }

    @Benchmark
    public void parseWithRegexParser(Blackhole blackhole) throws ParseException {
        List<Argument<?>> arguments = regexParser.parse(testMessageWithComma);
        blackhole.consume(arguments);
    }

    private static class RegexParser implements Parser {

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

        /**
         * Checks if a position range overlaps with any existing argument in the list
         * @param arguments List of existing arguments
         * @param startPos Start position of the range to check
         * @param length Length of the range to check
         * @return true if there is an overlap, false otherwise
         */
        private boolean isOverlappingWithExistingArguments(List<Argument<?>> arguments, int startPos, int length) {
            int endPos = startPos + length;
            for (Argument<?> arg : arguments) {
                int argStart = arg.startPosition();
                int argEnd = argStart + arg.length();

                // Check if ranges overlap
                if ((startPos <= argEnd) && (endPos >= argStart)) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public List<Argument<?>> parse(String rawMessage) throws ParseException {
            if (rawMessage == null || rawMessage.isEmpty()) {
                throw new IllegalArgumentException("rawMessage cannot be null or empty");
            }

            List<Argument<?>> arguments = new ArrayList<>();

            // 1. Find and extract timestamp substring (prefer TIMESTAMP_1, then TIMESTAMP_2)
            int tsStart = -1, tsEnd = -1;
            String tsString = null;
            SimpleDateFormat usedFormatter = null;

            Matcher ts1Matcher = TIMESTAMP_1_PATTERN.matcher(rawMessage);
            if (ts1Matcher.find()) {
                tsString = ts1Matcher.group();
                tsStart = ts1Matcher.start();
                tsEnd = ts1Matcher.end();
                usedFormatter = TIMESTAMP_1_FORMATTER.get();
            } else {
                Matcher ts2Matcher = TIMESTAMP_2_PATTERN.matcher(rawMessage);
                if (ts2Matcher.find()) {
                    tsString = ts2Matcher.group();
                    tsStart = ts2Matcher.start();
                    tsEnd = ts2Matcher.end();
                    usedFormatter = TIMESTAMP_2_FORMATTER.get();
                }
            }

            if (tsString != null) {
                try {
                    Date date = usedFormatter.parse(tsString);
                    arguments.add(new Timestamp(tsStart, tsEnd - tsStart, date.getTime(), usedFormatter.toPattern()));
                } catch (java.text.ParseException e) {
                    throw new ParseException("Failed to parse timestamp: " + tsString, e);
                }
            }

            // 2. Process the rest of the message for IP addresses and integers
            String remaining = tsEnd >= 0 ? rawMessage.substring(tsEnd) : rawMessage;

            // Find IP addresses
            Matcher ipMatcher = IPV4_PATTERN.matcher(remaining);
            while (ipMatcher.find()) {
                String ipStr = ipMatcher.group();
                int startPos = tsEnd + ipMatcher.start();
                int length = ipMatcher.end() - ipMatcher.start();

                // Only add if not overlapping with existing arguments
                if (isOverlappingWithExistingArguments(arguments, startPos, length) == false) {
                    String[] octets = ipStr.split("\\.");
                    int[] octetValues = new int[4];
                    for (int j = 0; j < 4; j++) {
                        octetValues[j] = Integer.parseInt(octets[j]);
                    }
                    arguments.add(new IPv4Argument(startPos, length, octetValues));
                }
            }

            // Find integers
            Matcher intMatcher = INTEGER_PATTERN.matcher(remaining);
            while (intMatcher.find()) {
                String intStr = intMatcher.group();
                int startPos = tsEnd + intMatcher.start();
                int length = intMatcher.end() - intMatcher.start();

                // Only add if not overlapping with existing arguments
                if (isOverlappingWithExistingArguments(arguments, startPos, length) == false) {
                    int value = Integer.parseInt(intStr);
                    arguments.add(new IntegerArgument(startPos, length, value));
                }
            }

            return arguments;
        }
    }
}
