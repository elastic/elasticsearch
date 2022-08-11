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
import org.elasticsearch.xpack.ql.tree.Source;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

import static org.elasticsearch.xpack.ql.expression.function.scalar.FunctionTestUtils.l;
import static org.elasticsearch.xpack.ql.type.DataTypes.DATETIME;
import static org.elasticsearch.xpack.ql.type.DataTypes.KEYWORD;

/**
 * Tests the {@link DateFormatter} against actual MySQL output.
 * <p>
 * Process to (re)generate the test data:
 * <ol>
 *     <li>Run the @{link {@link DateFormatRandomDatasetGenerator#main(String[])}} class.
 *         This class generates mysql-dateformat-test.sql file.
 *     </li>
 *     <li>If you have MySQL installed use this command ( for example in Ubuntu):
 *       <pre>
 *       sudo mysql -u root &lt; /path/to/mysql-dateformat-test.sql | tr "\\t" "|" &gt; /path/to/dateformat-generated.csv
 *       </pre>
 *     </li>
 *     <li>Copy file dateformat-generated.csv to resources and run this test class.
 *     </li>
 * </ol>
 */
public class DateTimeDateFormatProcessorTests extends ESTestCase {
    @ParametersFactory
    public static Iterable<Object> parameters() throws URISyntaxException, IOException {
        List<Object> params = new ArrayList<>();
        String testfile = "dateformat-generated.csv";
        int lineNumber = 0;
        for (String line : Files.readAllLines(
            PathUtils.get(Objects.requireNonNull(DateTimeDateFormatProcessorTests.class.getResource(testfile)).toURI())
        )) {
            lineNumber += 1;
            if (line.startsWith("randomized_timestamp")) {
                continue;
            }
            String[] columns = line.split("\\|");
            params.add(new Object[] { testfile, lineNumber, columns[0].replace("T", " "), columns[1], columns[2] });
        }
        return params;
    }

    private final String testFile;
    private final int lineNumber;
    private final String randomizedTimestamp;
    private final String patternString;
    private final String expectedResult;

    public DateTimeDateFormatProcessorTests(
        String testFile,
        int lineNumber,
        String randomizedTimestamp,
        String patternString,
        String expectedResult
    ) {
        this.testFile = testFile;
        this.lineNumber = lineNumber;
        this.randomizedTimestamp = randomizedTimestamp;
        this.patternString = patternString;
        this.expectedResult = expectedResult;
    }

    public void test() {
        ZoneId zoneId = ZoneId.of("Asia/Dubai");
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.n", Locale.ROOT);
        LocalDateTime dateTime = LocalDateTime.parse(randomizedTimestamp, dateTimeFormatter);
        ZonedDateTime dateTimeWithZone = dateTime.atZone(zoneId);
        String actualResult = (String) new DateFormat(Source.EMPTY, l(dateTimeWithZone, DATETIME), l(patternString, KEYWORD), zoneId)
            .makePipe()
            .asProcessor()
            .process(null);
        assertEquals(String.format(Locale.ROOT, """

            Testfile: %s
            Line number: %s
            Timestamp: %s
            Pattern: %s
            Expected(mysql date_format): %s
            Actual(ES date_format): %s

            """, testFile, lineNumber, randomizedTimestamp, patternString, expectedResult, actualResult), expectedResult, actualResult);
    }
}
