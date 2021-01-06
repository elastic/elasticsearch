/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;

import com.carrotsearch.randomizedtesting.generators.RandomNumbers;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.util.set.Sets;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.Arrays.asList;
import static java.util.regex.Pattern.quote;
import static org.elasticsearch.xpack.sql.expression.function.scalar.datetime.DateTimeTestUtils.dateTime;

/**
 * Generates an sql file that can be used to generate the test dataset for the {@link DateTimeToCharProcessorTests}.
 */
public class ToCharTestGenerator {

    private static class TestRecord {

        // timezones that are valid both in Java and in Postgres
        public static final List<String> POSTGRES_TEST_ZONE_LIST = asList(
            // location-based and long-names
            "US/Samoa", "Pacific/Honolulu", "Pacific/Marquesas", "Pacific/Gambier", "America/Juneau", "Canada/Yukon", "America/Vancouver",
            "Pacific/Easter", "US/Mountain", "America/Chicago", "US/Michigan", "Atlantic/Bermuda", "Canada/Newfoundland",
            "Atlantic/Cape_Verde", "Pacific/Kiritimati", "Pacific/Chatham", "Pacific/Auckland", "Asia/Sakhalin", "Australia/Tasmania",
            "Australia/North", "Asia/Tokyo", "Australia/Eucla", "Asia/Singapore", "Asia/Rangoon", "Indian/Chagos", "Asia/Calcutta",
            "Asia/Tashkent", "Asia/Tehran", "Asia/Dubai", "Africa/Nairobi", "Europe/Brussels", "Europe/Vienna", "Europe/London",
            "Etc/GMT+12",
            // short names of zones
            "GMT", "UTC", "CET",
            // offsets
            "+11:00", "+04:30", "+01:00", "+00:00", "-00:00", "-01:15", "-02:00", "-11:00");
        
        private final BigDecimal secondsAndFractionsSinceEpoch;
        private final String formatString;
        private final String zoneId;

        TestRecord(BigDecimal secondsAndFractionsSinceEpoch, String formatString) {
            this.secondsAndFractionsSinceEpoch = secondsAndFractionsSinceEpoch;
            this.formatString = formatString;
            this.zoneId = ToCharTestGenerator.randomFromCollection(POSTGRES_TEST_ZONE_LIST);
        }

    }

    private static final long SECONDS_IN_YEAR = 365 * 24 * 60 * 60L;
    public static final String DELIMITER = "|";
    public static final String PATTERN_DELIMITER = " @ ";
    // these patterns are hard to sync up between PostgreSQL and Elasticsearch, so we just warn, but actually
    // accept the output of Elasticsearch as is
    public static final Set<String> NOT_FULLY_MATCHABLE_PATTERNS = Set.of("TZ", "tz");
    private static final Set<String> UNSUPPORTED_PATTERN_MODIFIERS = Set.of("FX", "TM", "SP");
    private static final List<String> PATTERNS = new ArrayList<>(ToCharFormatter.FORMATTER_MAP.keySet());
    private static final List<String> FILL_MODIFIERS = asList("FM", "fm", "");
    private static final List<String> ORDINAL_SUFFIX_MODIFIERS = asList("TH", "th", "");

    @SuppressForbidden(reason = "It is ok to use ThreadLocalRandom outside of an actual test")
    private static Random rnd() {
        return ThreadLocalRandom.current();
    }

    private static BigDecimal randomSecondsWithFractions(int minYear, int maxYear) {
        BigDecimal
            seconds =
            new BigDecimal(RandomNumbers.randomLongBetween(rnd(), (minYear - 1970) * SECONDS_IN_YEAR, (maxYear - 1970) * SECONDS_IN_YEAR));
        BigDecimal fractions = new BigDecimal(RandomNumbers.randomIntBetween(rnd(), 0, 999_999)).movePointLeft(6);
        return seconds.add(fractions);
    }

    private static <T> T randomFromCollection(Collection<T> list) {
        List<T> l = new ArrayList<>(list);
        return l.get(rnd().nextInt(l.size()));
    }

    private static String patternWithRandomModifiers(String pattern) {
        if (NOT_FULLY_MATCHABLE_PATTERNS.contains(pattern)) {
            return pattern;
        }
        return randomFromCollection(FILL_MODIFIERS) + pattern + randomFromCollection(ORDINAL_SUFFIX_MODIFIERS);
    }

    private static List<TestRecord> generateTestCases() {

        final List<BigDecimal> testEpochSeconds = new LinkedList<>();
        final int MAX_YEAR_TO_TEST = 2500;
        
        final int NUMBER_OF_TIMESTAMPS_WITH_FULL_RANGE = 100;
        for (int i = 0; i < NUMBER_OF_TIMESTAMPS_WITH_FULL_RANGE; i++) {
            testEpochSeconds.add(randomSecondsWithFractions(-MAX_YEAR_TO_TEST, MAX_YEAR_TO_TEST));
        }
        
        for (int i = 0; i < 5; i++) {
            testEpochSeconds.add(randomSecondsWithFractions(-1, 1));
        }

        List<TestRecord> testRecords = new ArrayList<>();
        
        patternsOneByOne(testEpochSeconds, testRecords);
        allPatternsTogether(testEpochSeconds, testRecords);
        lowercasePatterns(testEpochSeconds, testRecords);
        postgreSQLPatternParsingBehaviour(testEpochSeconds, testRecords);
        monthsAsRomanNumbers(testRecords);
        randomPatterns(testEpochSeconds, testRecords);

        return testRecords;
    }

    private static void randomPatterns(List<BigDecimal> testEpochSeconds, List<TestRecord> testRecords) {
        final String randomCharacters = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYabcdefghijklmnopqrstuvwxy _-.:;";

        for (int i = 0; i < 20; i++) {
            String patternWithLiterals =
                IntStream.rangeClosed(1, 30)
                    .mapToObj(idx -> rnd().nextInt(100) < 80 ?
                        randomCharacters.substring(rnd().nextInt(randomCharacters.length())).substring(0, 1) :
                        (randomFromCollection(FILL_MODIFIERS) + randomFromCollection(PATTERNS) + randomFromCollection(
                            ORDINAL_SUFFIX_MODIFIERS)))
                    .collect(Collectors.joining());

            // clean up the random string from the unsupported modifiers
            for (String unsupportedPatternModifier : Sets.union(UNSUPPORTED_PATTERN_MODIFIERS, NOT_FULLY_MATCHABLE_PATTERNS)) {
                patternWithLiterals = patternWithLiterals
                    .replace(unsupportedPatternModifier, "")
                    .replace(unsupportedPatternModifier.toLowerCase(Locale.ROOT), "");
            }
            testRecords.add(new TestRecord(randomFromCollection(testEpochSeconds), patternWithLiterals));
        }
    }

    private static void monthsAsRomanNumbers(List<TestRecord> testRecords) {
        for (int i = 1; i <= 12; i++) {
            testRecords.add(new TestRecord(
                new BigDecimal(dateTime(0).withMonth(i).toEpochSecond()),
                rnd().nextBoolean() ? "RM" : "rm"));
        }
    }

    private static void postgreSQLPatternParsingBehaviour(List<BigDecimal> testEpochSeconds, List<TestRecord> testRecords) {
        // potentially ambiguous format string test cases, to check if our format string parsing is in-sync with PostgreSQL
        // that is greedy and prefers the longer format strings
        testRecords.add(new TestRecord(randomFromCollection(testEpochSeconds), "YYYYYYYYYYYYYYY,YYYYYYYYY"));
        testRecords.add(new TestRecord(randomFromCollection(testEpochSeconds), "SSSSSSSSSSSSSSSS"));
        testRecords.add(new TestRecord(randomFromCollection(testEpochSeconds), "DDDDDDDD"));
    }

    private static void lowercasePatterns(List<BigDecimal> testEpochSeconds, List<TestRecord> testRecords) {
        testRecords.add(new TestRecord(
            randomFromCollection(testEpochSeconds),
            IntStream.range(0, PATTERNS.size())
                .mapToObj(idx -> (idx + ":" + patternWithRandomModifiers(PATTERNS.get(idx))).toLowerCase(Locale.ROOT))
                .collect(Collectors.joining(PATTERN_DELIMITER))));
    }

    private static void allPatternsTogether(List<BigDecimal> testEpochSeconds, List<TestRecord> testRecords) {
        for (BigDecimal es : testEpochSeconds) {
            testRecords.add(new TestRecord(
                es,
                IntStream.range(0, PATTERNS.size())
                    .mapToObj(idx -> idx + ":" + patternWithRandomModifiers(PATTERNS.get(idx)))
                    .collect(Collectors.joining(PATTERN_DELIMITER))));
        }
    }

    private static void patternsOneByOne(List<BigDecimal> testEpochSeconds, List<TestRecord> testRecords) {
        for (String pattern : PATTERNS) {
            testRecords.add(new TestRecord(
                randomFromCollection(testEpochSeconds),
                NOT_FULLY_MATCHABLE_PATTERNS.contains(pattern) ?
                    pattern :
                    String.join(PATTERN_DELIMITER, pattern, FILL_MODIFIERS.get(0) + pattern + ORDINAL_SUFFIX_MODIFIERS.get(0))));
        }
    }

    private static String adjustZoneIdToPostgres(String zoneId) {
        // when the zone is specified by the offset in Postgres, it follows the POSIX definition, so the +- signs are flipped
        // compared to ISO-8601, see more info at: https://www.postgresql.org/docs/current/datetime-posix-timezone-specs.html
        if (zoneId.startsWith("+")) {
            zoneId = zoneId.replaceFirst(quote("+"), "-");
        } else if (zoneId.startsWith("-")) {
            zoneId = zoneId.replaceFirst(quote("-"), "+");
        }
        return zoneId;
    }

    /**
     * Generates an SQL file that can be used to create the test dataset for the unit test of the <code>TO_CHAR</code>
     * implementation. In case the <code>TO_CHAR</code> implementation needs an upgrade, add the list of the new format
     * strings to the list of the format string, regenerate the SQL, run it against the PostgreSQL version you are targeting
     * and update the test CSV file.
     */
    private static String unitTestExporterScript() {
        String header =
            "\n\\echo #" +
            "\n\\echo # DO NOT EDIT manually, was generated using " + ToCharTestGenerator.class.getName() +
            "\n\\echo #\n\n";
        String testCases = generateTestCases().stream().map(tc -> {
            long seconds = tc.secondsAndFractionsSinceEpoch.longValue();
            BigDecimal fractions = tc.secondsAndFractionsSinceEpoch.remainder(BigDecimal.ONE).movePointRight(6);
            return String.format(Locale.ROOT, 
                "SET TIME ZONE '%6$s';\n"
                    + "\\copy (SELECT %1$s as epoch_seconds_and_microsends, '%5$s' as zone_id, '%4$s' as format_string, " 
                    + "(TO_TIMESTAMP(%2$d) + INTERVAL '%3$d microseconds') as to_timestamp_result, "
                    + "TO_CHAR((TO_TIMESTAMP(%2$d) + INTERVAL '%3$d microseconds'), '%4$s') as to_char_result) to stdout " 
                    + "with DELIMITER as '" + DELIMITER + "' NULL as '' csv \n",
                tc.secondsAndFractionsSinceEpoch.toPlainString(),
                seconds,
                fractions.intValue(),
                tc.formatString,
                tc.zoneId,
                adjustZoneIdToPostgres(tc.zoneId));
        }).collect(Collectors.joining("\n"));
        return header + testCases;
    }

    public static void main(String[] args) throws Exception {
        String scriptFilename = args.length < 1 ? "/tmp/postgresql-tochar-test.sql" : args[0];
        Files.writeString(Path.of(scriptFilename), unitTestExporterScript(), StandardCharsets.UTF_8);
    }
}
