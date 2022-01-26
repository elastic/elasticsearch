/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.test.ESTestCase;

import java.text.ParseException;

import static org.elasticsearch.xpack.core.ilm.IndexLifecycleOriginationDateParser.parseIndexNameAndExtractDate;
import static org.elasticsearch.xpack.core.ilm.IndexLifecycleOriginationDateParser.shouldParseIndexName;
import static org.hamcrest.Matchers.is;

public class IndexLifecycleOriginationDateParserTests extends ESTestCase {

    private static final DateFormatter dateFormatter = DateFormatter.forPattern("uuuu.MM.dd");

    public void testShouldParseIndexNameReturnsFalseWhenOriginationDateIsSet() {
        Settings settings = Settings.builder().put(LifecycleSettings.LIFECYCLE_ORIGINATION_DATE, 1L).build();
        assertThat(shouldParseIndexName(settings), is(false));
    }

    public void testShouldParseIndexNameReturnsFalseIfParseOriginationDateIsDisabled() {
        Settings settings = Settings.builder().put(LifecycleSettings.LIFECYCLE_PARSE_ORIGINATION_DATE, false).build();
        assertThat(shouldParseIndexName(settings), is(false));
    }

    public void testShouldParseIndexNameReturnsTrueIfParseOriginationDateIsTrueAndOriginationDateIsNotSet() {
        Settings settings = Settings.builder().put(LifecycleSettings.LIFECYCLE_PARSE_ORIGINATION_DATE, true).build();
        assertThat(shouldParseIndexName(settings), is(true));
    }

    public void testParseIndexNameThatMatchesExpectedFormat() throws ParseException {
        long expectedDate = dateFormatter.parseMillis("2019.09.04");
        {
            long parsedDate = parseIndexNameAndExtractDate("indexName-2019.09.04");
            assertThat("indexName-yyyy.MM.dd is a valid index format", parsedDate, is(expectedDate));
        }

        {
            long parsedDate = parseIndexNameAndExtractDate("indexName-2019.09.04-0000001");
            assertThat("indexName-yyyy.MM.dd-\\d+$ is a valid index format", parsedDate, is(expectedDate));
        }

        {
            long parsedDate = parseIndexNameAndExtractDate("indexName-2019.09.04-2019.09.24");
            long secondDateInIndexName = dateFormatter.parseMillis("2019.09.24");
            assertThat(
                "indexName-yyyy.MM.dd-yyyy.MM.dd is a valid index format and the second date should be parsed",
                parsedDate,
                is(secondDateInIndexName)
            );
        }

        {
            long parsedDate = parseIndexNameAndExtractDate("index-2019.09.04-2019.09.24-00002");
            long secondDateInIndexName = dateFormatter.parseMillis("2019.09.24");
            assertThat(
                "indexName-yyyy.MM.dd-yyyy.MM.dd-digits is a valid index format and the second date should be parsed",
                parsedDate,
                is(secondDateInIndexName)
            );
        }
    }

    public void testParseIndexNameThrowsIllegalArgumentExceptionForInvalidIndexFormat() {
        expectThrows(
            IllegalArgumentException.class,
            "plainIndexName does not match the expected pattern",
            () -> parseIndexNameAndExtractDate("plainIndexName")
        );

        expectThrows(
            IllegalArgumentException.class,
            "indexName--00001 does not match the expected pattern as the origination date is missing",
            () -> parseIndexNameAndExtractDate("indexName--00001")
        );

        expectThrows(
            IllegalArgumentException.class,
            "indexName-00001 does not match the expected pattern as the origination date is missing",
            () -> parseIndexNameAndExtractDate("indexName-00001")
        );

        expectThrows(
            IllegalArgumentException.class,
            "indexName_2019.09.04_00001 does not match the expected pattern as _ is not the expected delimiter",
            () -> parseIndexNameAndExtractDate("indexName_2019.09.04_00001")
        );
    }

    public void testParseIndexNameThrowsIllegalArgumentExceptionForInvalidDateFormat() {
        expectThrows(
            IllegalArgumentException.class,
            "indexName-2019.04-00001 does not match the expected pattern as the date does not conform with the yyyy.MM.dd pattern",
            () -> parseIndexNameAndExtractDate("indexName-2019.04-00001")
        );

        expectThrows(
            IllegalArgumentException.class,
            "java.lang.IllegalArgumentException: failed to parse date field [2019.09.44] with format [yyyy.MM.dd]",
            () -> parseIndexNameAndExtractDate("index-2019.09.44")
        );
    }
}
