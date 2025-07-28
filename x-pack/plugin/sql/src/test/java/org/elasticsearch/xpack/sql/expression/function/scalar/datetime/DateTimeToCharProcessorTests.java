/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.core.PathUtils;
import org.elasticsearch.test.ESTestCase;

import java.math.BigDecimal;
import java.nio.file.Files;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import static java.util.Arrays.asList;
import static java.util.regex.Pattern.quote;
import static org.elasticsearch.xpack.ql.expression.function.scalar.FunctionTestUtils.l;
import static org.elasticsearch.xpack.ql.tree.Source.EMPTY;
import static org.elasticsearch.xpack.ql.type.DataTypes.DATETIME;
import static org.elasticsearch.xpack.ql.type.DataTypes.KEYWORD;
import static org.elasticsearch.xpack.sql.expression.function.scalar.datetime.DateTimeTestUtils.dateTime;
import static org.elasticsearch.xpack.sql.expression.function.scalar.datetime.ToCharTestScript.DELIMITER;
import static org.elasticsearch.xpack.sql.expression.function.scalar.datetime.ToCharTestScript.PATTERN_DELIMITER;

/**
 * Tests the {@link ToCharFormatter} against actual PostgreSQL output.
 *
 * Process to (re)generate the test data:
 * <ol>
 *     <li>Run the @{link {@link ToCharTestScript#main(String[])}} class</li>
 *     <li>Spin up a Postgres instance (latest or a specific version) using docker:
 *       <pre>
 *       docker run --rm --name postgres-latest -e POSTGRES_PASSWORD=mysecretpassword -p 5432:5432 -d postgres:latest
 *       </pre>
 *     </li>
 *     <li>Generate the test dataset by execution the SQL against PostgreSQL and capturing the output:
 *       <pre>
 *       PGPASSWORD="mysecretpassword" psql --quiet -h localhost -p 5432 -U postgres -f /tmp/postgresql-tochar-test.sql \
 *           &gt; /path/to/tochar-generated.csv
 *       </pre>
 *     </li>
 * </ol>
 *
 * In case you need to mute any of the tests, mute all tests by adding {@link org.apache.lucene.tests.util.LuceneTestCase.AwaitsFix}
 * on the class level.
 */
public class DateTimeToCharProcessorTests extends ESTestCase {

    @ParametersFactory(argumentFormatting = "%1$s:%2$s %5$s")
    public static Iterable<Object[]> parameters() throws Exception {
        List<Object[]> params = new ArrayList<>();
        String testFile = "tochar-generated.csv";
        int lineNumber = 0;
        for (String line : Files.readAllLines(PathUtils.get(DateTimeToCharProcessorTests.class.getResource(testFile).toURI()))) {
            lineNumber += 1;
            if (line.startsWith("#")) {
                continue;
            }
            String[] cols = line.split(quote(DELIMITER));
            params.add(new Object[] { testFile, lineNumber, cols[0], cols[1], cols[2], cols[3], cols[4] });
        }
        return params;
    }

    private final String testFile;
    private final int lineNumber;
    private final String secondsAndFractionsSinceEpoch;
    private final String zone;
    private final String formatString;
    private final String posgresTimestamp;
    private final String expectedResult;

    /**
     * @param testFile The name of the testfile where this testcase is coming from
     * @param lineNumber The line number of the testcase within the testfile
     * @param secondsAndFractionsSinceEpoch The date represented by seconds and fractions since epoch that was used to
     *                                      generate the TO_CHAR() PostgreSQL output.
     * @param zone The long/short name or offset for the timezone used when generating the expected TO_CHAR() output.
     * @param formatString The pattern to be tested (this is exactly the pattern that was passed into the TO_CHAR() function in PostgreSQL).
     * @param posgresTimestamp The timestamp represented by PostgreSQL as string in the default format (without calling TO_CHAR()).
     * @param expectedResult The PostgreSQL output of <code>TO_CHAR(
     *                       (TO_TIMESTAMP([[secondsSinceEpoch]]) + INTERVAL '[[fractions]] microseconds'),
     *                       '[[formatString]]')</code>.
     */
    public DateTimeToCharProcessorTests(
        String testFile,
        int lineNumber,
        String secondsAndFractionsSinceEpoch,
        String zone,
        String formatString,
        String posgresTimestamp,
        String expectedResult
    ) {

        this.testFile = testFile;
        this.lineNumber = lineNumber;
        this.secondsAndFractionsSinceEpoch = secondsAndFractionsSinceEpoch;
        this.zone = zone;
        this.formatString = formatString;
        this.posgresTimestamp = posgresTimestamp;
        this.expectedResult = expectedResult;
    }

    public void test() {
        ZoneId zoneId = ZoneId.of(zone);
        ZonedDateTime timestamp = dateTimeWithFractions(secondsAndFractionsSinceEpoch);
        String actualResult = (String) new ToChar(EMPTY, l(timestamp, DATETIME), l(formatString, KEYWORD), zoneId).makePipe()
            .asProcessor()
            .process(null);
        List<String> expectedResultSplitted = asList(expectedResult.split(quote(PATTERN_DELIMITER)));
        List<String> resultSplitted = asList(actualResult.split(quote(PATTERN_DELIMITER)));
        List<String> formatStringSplitted = asList(formatString.split(PATTERN_DELIMITER));
        assertEquals(formatStringSplitted.size(), resultSplitted.size());
        assertEquals(formatStringSplitted.size(), expectedResultSplitted.size());
        for (int i = 0; i < formatStringSplitted.size(); i++) {
            String patternMaybeWithIndex = formatStringSplitted.get(i);
            String expectedPart = expectedResultSplitted.get(i);
            String actualPart = resultSplitted.get(i);
            assertEquals(
                String.format(
                    Locale.ROOT,
                    """

                        Line number:                        %s (in %s)
                        zone:                               %s
                        timestamp (as epoch):               %s
                        timestamp (java, UTC):              %s
                        timestamp (postgres, to_timestamp): %s
                        timestamp (java with zone):         %s
                        format string:                      %s
                        expected (postgres to_char result): %s
                        actual (ES to_char result):         %s
                            FAILED (sub)pattern: %s,""",
                    lineNumber,
                    testFile,
                    zone,
                    secondsAndFractionsSinceEpoch,
                    timestamp,
                    posgresTimestamp,
                    timestamp.withZoneSameInstant(zoneId),
                    formatString,
                    expectedResult,
                    actualResult,
                    patternMaybeWithIndex
                ),
                expectedPart,
                actualPart
            );
        }
    }

    private static ZonedDateTime dateTimeWithFractions(String secondAndFractionsSinceEpoch) {
        BigDecimal b = new BigDecimal(secondAndFractionsSinceEpoch);
        long seconds = b.longValue();
        int fractions = b.remainder(BigDecimal.ONE).movePointRight(9).intValueExact();
        int adjustment = 0;
        if (fractions < 0) {
            fractions += (int) 1e9;
            adjustment = -1;
        }
        return dateTime((seconds + adjustment) * 1000).withNano(fractions);
    }
}
