/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;

import com.carrotsearch.randomizedtesting.generators.RandomNumbers;

import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.common.util.set.Sets;

import java.math.BigDecimal;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.Arrays.asList;
import static java.util.regex.Pattern.quote;
import static org.elasticsearch.xpack.sql.expression.function.scalar.datetime.DateTimeTestUtils.dateTime;

/**
 * Generates an psql file that can be used to generate the test dataset for the {@link DateTimeToCharProcessorTests}.
 */
public class ToCharTestScript {

    private class TestRecord {
        private final BigDecimal secondsAndFractionsSinceEpoch;
        private final String formatString;
        private final String zoneId;

        TestRecord(BigDecimal secondsAndFractionsSinceEpoch, String formatString) {
            this.secondsAndFractionsSinceEpoch = secondsAndFractionsSinceEpoch;
            this.formatString = formatString;
            this.zoneId = ToCharTestScript.this.randomFromCollection(TIMEZONES_TO_TEST);
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
    private static final List<String> MATCHABLE_PATTERNS;
    static {
        MATCHABLE_PATTERNS = new ArrayList<>(PATTERNS);
        MATCHABLE_PATTERNS.removeAll(NOT_FULLY_MATCHABLE_PATTERNS);
    }
    private static final List<String> FILL_MODIFIERS = asList("FM", "fm", "");
    private static final List<String> ORDINAL_SUFFIX_MODIFIERS = asList("TH", "th", "");
    // timezones that are valid both in Java and in Postgres
    public static final List<String> TIMEZONES_TO_TEST =
            readAllLinesWithoutComment(ToCharTestScript.class.getResource("tochar-test-timezones.txt"));

    private final List<TestRecord> testRecords = new ArrayList<>();
    private final List<BigDecimal> testEpochSeconds = new ArrayList<>();
    private final Random random;

    public ToCharTestScript(Random random) {
        this.random = random;
        generateTestTimestamps();

        patternsOneByOne();
        allPatternsTogether();
        postgreSQLPatternParsingBehaviour();
        monthsAsRomanNumbers();
        randomizedPatternStrings();
    }

    private void generateTestTimestamps() {
        final int latestYearToTest = 3000;
        int countOfTestYears = 150;
        for (int i = 0; i < countOfTestYears; i++) {
            testEpochSeconds.add(randomSecondsWithFractions(-latestYearToTest, latestYearToTest));
        }

        int countOfTestYearsAroundYearZero = 10;
        for (int i = 0; i < countOfTestYearsAroundYearZero; i++) {
            testEpochSeconds.add(randomSecondsWithFractions(-1, 1));
        }
    }

    private void patternsOneByOne() {
        for (String pattern : MATCHABLE_PATTERNS) {
            testRecords.add(new TestRecord(
                randomFromCollection(testEpochSeconds),
                NOT_FULLY_MATCHABLE_PATTERNS.contains(pattern) ?
                    pattern :
                    String.join(PATTERN_DELIMITER, pattern, FILL_MODIFIERS.get(0) + pattern + ORDINAL_SUFFIX_MODIFIERS.get(0))));
        }
    }

    private void allPatternsTogether() {
        for (BigDecimal es : testEpochSeconds) {
            testRecords.add(new TestRecord(
                es,
                IntStream.range(0, MATCHABLE_PATTERNS.size())
                    .mapToObj(idx -> idx + ":" + patternWithRandomModifiers(MATCHABLE_PATTERNS.get(idx)))
                    .collect(Collectors.joining(PATTERN_DELIMITER))));
        }
    }

    private void postgreSQLPatternParsingBehaviour() {
        // potentially ambiguous format string test cases, to check if our format string parsing is in-sync with PostgreSQL
        // that is greedy and prefers the longer format strings
        testRecords.add(new TestRecord(randomFromCollection(testEpochSeconds), "YYYYYYYYYYYYYYY,YYYYYYYYY"));
        testRecords.add(new TestRecord(randomFromCollection(testEpochSeconds), "SSSSSSSSSSSSSSSS"));
        testRecords.add(new TestRecord(randomFromCollection(testEpochSeconds), "DDDDDDDD"));
        testRecords.add(new TestRecord(randomFromCollection(testEpochSeconds), "FMFMFMFMAAthththth"));
        testRecords.add(new TestRecord(randomFromCollection(testEpochSeconds), "1FMth"));
    }

    private void monthsAsRomanNumbers() {
        for (int i = 1; i <= 12; i++) {
            testRecords.add(new TestRecord(
                new BigDecimal(dateTime(0).withMonth(i).toEpochSecond()),
                random.nextBoolean() ? "RM" : "rm"));
        }
    }

    private void randomizedPatternStrings() {
        final String randomCharacters = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYabcdefghijklmnopqrstuvwxy _-.:;";

        final int randomizedPatternCount = 50;
        final int lengthOfRandomizedPattern = 50;
        final int pctChanceOfRandomCharacter = 80;
        for (int i = 0; i < randomizedPatternCount; i++) {
            String patternWithLiterals = IntStream.rangeClosed(1, lengthOfRandomizedPattern)
                    .mapToObj(idx -> {
                        if (random.nextInt(100) < pctChanceOfRandomCharacter) {
                            return randomCharacters.substring(random.nextInt(randomCharacters.length())).substring(0, 1);
                        } else {
                            return (randomFromCollection(FILL_MODIFIERS) + randomFromCollection(PATTERNS)
                                + randomFromCollection(ORDINAL_SUFFIX_MODIFIERS));
                        }})
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

    private BigDecimal randomSecondsWithFractions(int minYear, int maxYear) {
        BigDecimal seconds =
            new BigDecimal(RandomNumbers.randomLongBetween(random, (minYear - 1970) * SECONDS_IN_YEAR, (maxYear - 1970) * SECONDS_IN_YEAR));
        BigDecimal fractions = new BigDecimal(RandomNumbers.randomIntBetween(random, 0, 999_999)).movePointLeft(6);
        return seconds.add(fractions);
    }

    private <T> T randomFromCollection(Collection<T> list) {
        List<T> l = new ArrayList<>(list);
        return l.get(random.nextInt(l.size()));
    }

    private String patternWithRandomModifiers(String pattern) {
        return randomFromCollection(FILL_MODIFIERS) + pattern + randomFromCollection(ORDINAL_SUFFIX_MODIFIERS);
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

    static List<String> readAllLinesWithoutComment(URL url) {
        try {
            URI uri = url.toURI();
            return Files.readAllLines(PathUtils.get(uri))
                .stream()
                .filter(s -> s.startsWith("#") == false)
                .filter(s -> s.isBlank() == false)
                .collect(Collectors.toList());
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    /**
     * Generates an SQL file (psql input) that can be used to create the test dataset for the unit test of the <code>TO_CHAR</code>
     * implementation. In case the <code>TO_CHAR</code> implementation needs an upgrade, add the list of the new format
     * strings to the list of the format string, regenerate the SQL, run it against the PostgreSQL version you are targeting
     * and update the test CSV file.
     */
    private String unitTestExporterScript() {
        String header =
            "\n\\echo #" +
            "\n\\echo # DO NOT EDIT manually, was generated using " + ToCharTestScript.class.getName() +
            "\n\\echo #\n\n";
        String testCases = testRecords.stream().map(tc -> {
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

    @SuppressForbidden(reason = "It is ok to use Random outside of an actual test")
    private static Random rnd() {
        return new Random();
    }

    public static void main(String[] args) throws Exception {
        String scriptFilename = args.length < 1 ? "postgresql-tochar-test.sql" : args[0];
        Files.writeString(PathUtils.get(scriptFilename), new ToCharTestScript(rnd()).unitTestExporterScript(), StandardCharsets.UTF_8);
    }
}
