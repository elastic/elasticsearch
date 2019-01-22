/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.action;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.sql.action.BasicFormatter.FormatOption;
import org.elasticsearch.xpack.sql.proto.ColumnInfo;
import org.elasticsearch.xpack.sql.proto.Mode;

import java.util.Arrays;

import static org.elasticsearch.xpack.sql.action.BasicFormatter.FormatOption.CLI;
import static org.hamcrest.Matchers.arrayWithSize;

public class BasicFormatterTests extends ESTestCase {
    private final FormatOption format = randomFrom(FormatOption.values());
    private final SqlQueryResponse firstResponse = new SqlQueryResponse("", format == CLI ? Mode.CLI : Mode.PLAIN,
            Arrays.asList(
                    new ColumnInfo("", "foo", "string", 0),
                    new ColumnInfo("", "bar", "long", 15),
                    new ColumnInfo("", "15charwidename!", "double", 25),
                    new ColumnInfo("", "superduperwidename!!!", "double", 25),
                    new ColumnInfo("", "baz", "keyword", 0),
                    new ColumnInfo("", "date", "datetime", 24)),
            Arrays.asList(
                Arrays.asList("15charwidedata!", 1, 6.888, 12, "rabbit", "1953-09-02T00:00:00.000Z"),
                Arrays.asList("dog", 1.7976931348623157E308, 123124.888, 9912, "goat", "2000-03-15T21:34:37.443Z")));
    private final BasicFormatter formatter = new BasicFormatter(firstResponse.columns(), firstResponse.rows(), format);

    /**
     * Tests for {@link BasicFormatter#formatWithHeader}, values
     * of exactly the minimum column size, column names of exactly
     * the minimum column size, column headers longer than the
     * minimum column size, and values longer than the minimum
     * column size.
     */
    public void testFormatWithHeader() {
        String[] result = formatter.formatWithHeader(firstResponse.columns(), firstResponse.rows()).split("\n");
        assertThat(result, arrayWithSize(4));
        assertEquals("      foo      |         bar          |15charwidename!|superduperwidename!!!|      baz      |"
                + "          date          ", result[0]);
        assertEquals("---------------+----------------------+---------------+---------------------+---------------+"
                + "------------------------", result[1]);
        assertEquals("15charwidedata!|1                     |6.888          |12                   |rabbit         |"
                + "1953-09-02T00:00:00.000Z", result[2]);
        assertEquals("dog            |1.7976931348623157E308|123124.888     |9912                 |goat           |"
                + "2000-03-15T21:34:37.443Z", result[3]);
    }

    /**
     * Tests for {@link BasicFormatter#formatWithoutHeader} and
     * truncation of long columns.
     */
    public void testFormatWithoutHeader() {
        String[] result = formatter.formatWithoutHeader(
                Arrays.asList(
                        Arrays.asList("ohnotruncateddata", 4, 1, 77, "wombat", "1955-01-21T01:02:03.342Z"),
                        Arrays.asList("dog", 2, 123124.888, 9912, "goat", "2231-12-31T23:59:59.999Z"))).split("\n");
        assertThat(result, arrayWithSize(2));
        assertEquals("ohnotruncatedd~|4                     |1              |77                   |wombat         |"
                + "1955-01-21T01:02:03.342Z", result[0]);
        assertEquals("dog            |2                     |123124.888     |9912                 |goat           |"
                + "2231-12-31T23:59:59.999Z", result[1]);
    }

    /**
     * Ensure that our estimates are perfect in at least some cases.
     */
    public void testEstimateSize() {
        assertEquals(formatter.formatWithHeader(firstResponse.columns(), firstResponse.rows()).length(),
                formatter.estimateSize(firstResponse.rows().size() + 2));
        assertEquals(formatter.formatWithoutHeader(firstResponse.rows()).length(),
                formatter.estimateSize(firstResponse.rows().size()));
    }
}
