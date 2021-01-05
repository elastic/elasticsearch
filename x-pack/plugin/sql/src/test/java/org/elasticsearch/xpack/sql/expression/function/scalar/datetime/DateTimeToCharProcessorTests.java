/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import com.carrotsearch.randomizedtesting.generators.RandomNumbers;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.ZoneId;
import java.time.ZonedDateTime;
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
import static org.elasticsearch.xpack.ql.expression.function.scalar.FunctionTestUtils.l;
import static org.elasticsearch.xpack.sql.expression.function.scalar.datetime.DateTimeTestUtils.dateTime;

public class DateTimeToCharProcessorTests extends ESTestCase {

    private static class TestCase {
        private static int COUNTER = 0;
        
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

        private final int id;
        private final BigDecimal secondsAndFractionsSinceEpoch;
        private final String formatString;
        private final String zoneId;
        
        TestCase(BigDecimal secondsAndFractionsSinceEpoch, String formatString) {
            COUNTER += 1;
            this.id = COUNTER;
            this.secondsAndFractionsSinceEpoch = secondsAndFractionsSinceEpoch;
            this.formatString = formatString;
            this.zoneId = TestGenerator.randomFromCollection(POSTGRES_TEST_ZONE_LIST);
        }

    }

    /**
     * Generates an sql file that can be used to generate the test dataset for this unit test (to be used with the <code>psql</code> cli).
     *
     * Run the @{link {@link TestGenerator#main(String[])}} class and execute the following command to generate the output dataset with:
     *
     * <p>
     *  <code>
     *      # easy way to spin up the latest Postgres
     *      docker run --rm --name postgres-latest -e POSTGRES_PASSWORD=mysecretpassword -p 5432:5432 -d postgres:latest
     *
     *      # generate the csv for the unit test
     *      PGPASSWORD="mysecretpassword" psql --quiet -h localhost -p 5432 -U postgres -f /tmp/postgresql-tochar-test.sql \
     *          &gt; /path/to/tochar.csv
     *  </code>
     * </p>
     */
    private static class TestGenerator {
        
        private static final long SECONDS_IN_YEAR = 365 * 24 * 60 * 60L;
        private static final String DELIMITER = "|";
        private static final String PATTERN_DELIMITER = " @ ";
        // these patterns are hard to sync up between PostgreSQL and Elasticsearch, so we just warn, but actually
        // accept the output of Elasticsearch as is
        private static final Set<String> NOT_FULLY_MATCHABLE_PATTERNS = Set.of("TZ", "tz");
        private static final Set<String> UNSUPPORTED_PATTERN_MODIFIERS = Set.of("FX", "TM", "SP");
        private static final List<String> PATTERNS = new ArrayList<>(ToCharFormatter.FORMATTER_MAP.keySet());
        private static final List<String> FILL_MODIFIERS = asList("FM", "fm", "");
        private static final List<String> ORDINAL_SUFFIX_MODIFIERS = asList("TH", "th", "");

        @SuppressForbidden(reason = "It is ok to use ThreadLocalRandom outside of an actual test")
        private static Random rnd() {
            return ThreadLocalRandom.current();
        }

        private static BigDecimal randomSecondsWithFractions(int minYear, int maxYear) {
            BigDecimal seconds = new BigDecimal(RandomNumbers.randomLongBetween(rnd(), 
                (minYear - 1970) * SECONDS_IN_YEAR, (maxYear - 1970) * SECONDS_IN_YEAR));
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
    
        private static List<TestCase> generateTestCases() {
            
            final List<BigDecimal> testEpochSeconds = new LinkedList<>();
            for (int i = 0; i < 50; i++) {
                testEpochSeconds.add(randomSecondsWithFractions(-1200, 2500));
            }
            for (int i = 0; i < 5; i++) {
                testEpochSeconds.add(randomSecondsWithFractions(-1, 1));
            }
    
            List<TestCase> testCases = new ArrayList<>();
    
            // check each format string alone
            for (String pattern : PATTERNS) {
                testCases.add(new TestCase(randomFromCollection(testEpochSeconds), NOT_FULLY_MATCHABLE_PATTERNS.contains(pattern) 
                    ? pattern 
                    : String.join(PATTERN_DELIMITER, pattern, FILL_MODIFIERS.get(0) + pattern + ORDINAL_SUFFIX_MODIFIERS.get(0))));
            }
            
            // let's check all of the format strings with a non-ambiguous format string
            for (BigDecimal es : testEpochSeconds) {
                testCases.add(new TestCase(es, IntStream.range(0, PATTERNS.size())
                    .mapToObj(idx -> idx + ":" + patternWithRandomModifiers(PATTERNS.get(idx)))
                    .collect(Collectors.joining(PATTERN_DELIMITER))));
            }
    
            // check the lowercase versions of the format strings
            testCases.add(new TestCase(randomFromCollection(testEpochSeconds), IntStream.range(0, PATTERNS.size())
                .mapToObj(idx -> (idx + ":" + patternWithRandomModifiers(PATTERNS.get(idx))).toLowerCase(Locale.ROOT))
                .collect(Collectors.joining(PATTERN_DELIMITER))));
    
            // potentially ambiguous format string test cases, to check if our format string parsing is in-sync with PostgreSQL
            // greedy and prefers the longer format strings
            testCases.add(new TestCase(randomFromCollection(testEpochSeconds), "YYYYYYYYYYYYYYY,YYYYYYYYY"));
            testCases.add(new TestCase(randomFromCollection(testEpochSeconds), "SSSSSSSSSSSSSSSS"));
            testCases.add(new TestCase(randomFromCollection(testEpochSeconds), "DDDDDDDD"));
    
            // Roman numbers
            for (int i = 1; i <= 12; i++) {
                testCases.add(new TestCase(
                    new BigDecimal(dateTime(0).withMonth(i).toEpochSecond()), rnd().nextBoolean() ? "RM" : "rm"));
            }
            
            // random strings made out of template patterns and random characters
            final String randomCharacters = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYabcdefghijklmnopqrstuvwxy _-.:;";
            final List<String> formatStringsToRandomTest = PATTERNS.stream()
                .filter(s -> NOT_FULLY_MATCHABLE_PATTERNS.contains(s) == false)
                .collect(Collectors.toList());
            
            for (int i = 0; i < 20; i++) {
                String patternWithLiterals = IntStream.rangeClosed(1, 30).mapToObj(idx -> 
                    rnd().nextInt(100) < 80 
                        ? randomCharacters.substring(rnd().nextInt(randomCharacters.length())).substring(0, 1) 
                        : (randomFromCollection(FILL_MODIFIERS) 
                            + randomFromCollection(formatStringsToRandomTest) 
                            + randomFromCollection(ORDINAL_SUFFIX_MODIFIERS)))
                    .collect(Collectors.joining());
                
                // clean up the random string from the unsupported modifiers
                for (String unsupportedPatternModifier : UNSUPPORTED_PATTERN_MODIFIERS) {
                    patternWithLiterals = patternWithLiterals
                        .replace(unsupportedPatternModifier, "")
                        .replace(unsupportedPatternModifier.toLowerCase(Locale.ROOT), "");
                }
                testCases.add(new TestCase(randomFromCollection(testEpochSeconds), patternWithLiterals));
            }
    
            return testCases;
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
            return generateTestCases().stream().map(tc -> {
                long seconds = tc.secondsAndFractionsSinceEpoch.longValue();
                BigDecimal fractions = tc.secondsAndFractionsSinceEpoch.remainder(BigDecimal.ONE).movePointRight(6);
                return String.format(Locale.ROOT,
                    "SET TIME ZONE '%7$s';\n" +
                    "\\copy (SELECT %1$d as id, %2$s as epoch_seconds_and_microsends, '%6$s' as zone_id, '%5$s' as format_string, " +
                        "(TO_TIMESTAMP(%3$d) + INTERVAL '%4$d microseconds') as to_timestamp_result, " +
                        "TO_CHAR((TO_TIMESTAMP(%3$d) + INTERVAL '%4$d microseconds'), '%5$s') as to_char_result) to stdout " +
                        "with DELIMITER as '" + DELIMITER + "' NULL as '' csv \n",
                    tc.id, tc.secondsAndFractionsSinceEpoch.toPlainString(), seconds, fractions.intValue(), tc.formatString, tc.zoneId, 
                    adjustZoneIdToPostgres(tc.zoneId)
                );
            }).collect(Collectors.joining("\n"));
        }
        
        public static void main(String[] args) throws Exception {
            String scriptFilename = args.length < 1 ? "/tmp/postgresql-tochar-test.sql" : args[0];
            Files.writeString(Path.of(scriptFilename), unitTestExporterScript(), StandardCharsets.UTF_8);
        }
    }

    @ParametersFactory(argumentFormatting = "%1$s: timestamp=%5$s, zone=%3$s, format=%4$s")
    public static List<Object[]> readCsv() throws Exception {
        Path testFilePath = Path.of(DateTimeToCharProcessorTests.class.getResource("tochar.csv").toURI());
        return Files.readAllLines(testFilePath).stream().map(line -> {
            String[] cols = line.split(quote(TestGenerator.DELIMITER));
            return new Object[]{ cols[0], cols[1], cols[2], cols[3], cols[4], cols[5] };
        }).collect(Collectors.toList());
    }

    private final String id;
    private final String secondsAndFractionsSinceEpoch;
    private final String zone;
    private final String formatString;
    private final String posgresTimestamp;
    private final String expectedResult;

    public DateTimeToCharProcessorTests(String id, String secondsAndFractionsSinceEpoch, String zone, String formatString,
        String postgresTimestamp, String expectedResult) {
        this.id = id;
        this.secondsAndFractionsSinceEpoch = secondsAndFractionsSinceEpoch;
        this.zone = zone;
        this.formatString = formatString;
        this.posgresTimestamp = postgresTimestamp;
        this.expectedResult = expectedResult;
    }

    private static ZonedDateTime dateTimeWithFractions(String secondAndFractionsSinceEpoch) {
        BigDecimal b = new BigDecimal(secondAndFractionsSinceEpoch);
        long seconds = b.longValue();
        int fractions = b.remainder(BigDecimal.ONE).movePointRight(9).intValueExact();
        int adjustment = 0;
        if (fractions < 0) {
            fractions += 1e9;
            adjustment = -1;
        }
        return dateTime((seconds + adjustment) * 1000).withNano(fractions);
    }
    
    public void test() throws Exception {
        ZoneId zoneId = ZoneId.of(zone);
        ZonedDateTime timestamp = dateTimeWithFractions(secondsAndFractionsSinceEpoch);
        String actualResult =
            (String) new ToChar(Source.EMPTY, l(timestamp, DataTypes.DATETIME), l(formatString, DataTypes.KEYWORD), zoneId)
                .makePipe()
                .asProcessor()
                .process(null);
        List<String> expectedResultSplitted = asList(expectedResult.split(quote(TestGenerator.PATTERN_DELIMITER)));
        List<String> resultSplitted = asList(actualResult.split(quote(TestGenerator.PATTERN_DELIMITER)));
        List<String> formatStringSplitted = asList(formatString.split(TestGenerator.PATTERN_DELIMITER));
        assertEquals(formatStringSplitted.size(), resultSplitted.size());
        assertEquals(formatStringSplitted.size(), expectedResultSplitted.size());
        for (int i = 0; i < formatStringSplitted.size(); i++) {
            String patternMaybeWithIndex = formatStringSplitted.get(i);
            String pattern = patternMaybeWithIndex.contains(":") 
                ? patternMaybeWithIndex.substring(patternMaybeWithIndex.indexOf(":") + 1)
                : patternMaybeWithIndex;
            String expectedPart = expectedResultSplitted.get(i);
            String actualPart = resultSplitted.get(i);
            try {
                assertEquals(
                    String.format(Locale.ROOT, 
                        "\n" + 
                        "Test id:                            %s\n" +
                        "zone:                               %s\n" +
                        "timestamp (as epoch):               %s\n" +
                        "timestamp (java, UTC):              %s\n" +
                        "timestamp (postgres, to_timestamp): %s\n" +
                        "timestamp (java with zone):         %s\n" +
                        "format string:                      %s\n" +
                        "expected (postgres to_char result): %s\n" +
                        "actual (ES to_char result):         %s\n" +
                        "    FAILED (sub)pattern: %s,",
                        id, zone, secondsAndFractionsSinceEpoch, timestamp, posgresTimestamp, timestamp.withZoneSameInstant(zoneId), 
                        formatString, expectedResult, actualResult, patternMaybeWithIndex), 
                    expectedPart, actualPart);
            } catch (AssertionError err) {
                if (TestGenerator.NOT_FULLY_MATCHABLE_PATTERNS.stream().anyMatch(pattern::contains)) {
                    logger.info("Known pattern failure ('TZ' and 'tz' cannot be matched with Postgres): " + err.getMessage());
                } else {
                    throw err;
                }
            }
        }
    }
}