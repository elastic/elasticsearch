/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;

import com.carrotsearch.randomizedtesting.generators.RandomNumbers;

import org.elasticsearch.core.PathUtils;
import org.elasticsearch.core.SuppressForbidden;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Generates mysql-dateformat-test.sql file that can be used to generate the test dataset for the {@link DateTimeDateFormatProcessorTests}.
 * <p>
 *  This file contains MySQL queries with the DATE_FORMAT function and random timestamps.
 *  <p>
 *  MySQL will generate a .csv file that will be used for testing.
 *
 */
public class DateFormatRandomDatasetGenerator {

    private static class TestRecord {
        private final LocalDateTime localDateTime;
        private final String formatString;

        TestRecord(LocalDateTime localDateTime, String formatString) {
            this.localDateTime = localDateTime;
            this.formatString = formatString;
        }
    }

    private static final List<String> PATTERNS = new ArrayList<>(DateFormatter.FORMATTER_MAP.keySet());
    private static final List<TestRecord> TESTRECORDS = new ArrayList<>();

    private static Random RANDOM = null;

    @SuppressForbidden(reason = "It is ok to use Random outside of an actual test")
    private static Random rnd() {
        return new Random();
    }

    public static void main(String[] args) throws IOException {
        RANDOM = rnd();
        String scriptFilename = args.length < 1 ? "mysql-dateformat-test.sql" : args[0];
        generateTestRecords();
        generateTestRecordsWithLiterals();
        String sqlFileContent = createSQLFile();
        Files.writeString(PathUtils.get(scriptFilename), sqlFileContent, StandardCharsets.UTF_8);
    }

    private static void generateTestRecords() {
        for (String pattern : PATTERNS) {
            for (int i = 1; i <= 30; i++) {
                TESTRECORDS.add(new TestRecord(randomizedTimestamp(), pattern));
            }
        }

    }

    private static void generateTestRecordsWithLiterals() {
        String randomCharacters = "012%3456%789AB%CDEFGH%IJKLMN%OPQRST%UVWXYabcde%fghijklm%nopqrstu%vwxy% _-.:;";
        for (int i = 1; i <= 30; i++) {
            String patternWithLiterals = IntStream.rangeClosed(1, 40).mapToObj(idx -> {
                int randomNumber = RANDOM.nextInt(randomCharacters.length());
                if (randomNumber == randomCharacters.length() - 1) {
                    return randomCharacters.substring(randomNumber);
                } else {
                    return randomCharacters.substring(randomNumber, randomNumber + 1);
                }
            }).collect(Collectors.joining());
            TESTRECORDS.add(new TestRecord(randomizedTimestamp(), patternWithLiterals));
        }
    }

    private static LocalDateTime randomizedTimestamp() {
        int year = RandomNumbers.randomIntBetween(RANDOM, 1000, 9999);
        int month = RandomNumbers.randomIntBetween(RANDOM, 1, 12);
        int dayOfMonth = RandomNumbers.randomIntBetween(RANDOM, 1, LocalDate.of(year, month, 1).lengthOfMonth());
        int hour = RandomNumbers.randomIntBetween(RANDOM, 0, 23);
        int minute = RandomNumbers.randomIntBetween(RANDOM, 0, 59);
        int second = RandomNumbers.randomIntBetween(RANDOM, 0, 59);
        int nanoOfSecond = RandomNumbers.randomIntBetween(RANDOM, 0, 999999999);
        return LocalDateTime.of(year, month, dayOfMonth, hour, minute, second, nanoOfSecond);
    }

    private static String createSQLFile() {
        return TESTRECORDS.stream()
            .map(
                tc -> String.format(
                    Locale.ROOT,
                    "SELECT '%1$s' as randomized_timestamp, '%2$s' as format_string, "
                        + "DATE_FORMAT('%1$s', '%2$s') as date_format_result;",
                    tc.localDateTime.toString(),
                    tc.formatString
                )
            )
            .collect(Collectors.joining("\n"));
    }
}
