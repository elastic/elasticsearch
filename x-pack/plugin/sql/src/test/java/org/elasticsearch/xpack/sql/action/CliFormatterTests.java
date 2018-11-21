/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.action;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.sql.proto.ColumnInfo;

import java.sql.Types;
import java.util.Arrays;

import static org.hamcrest.Matchers.arrayWithSize;

public class CliFormatterTests extends ESTestCase {
    private final SqlQueryResponse firstResponse = new SqlQueryResponse("",
            Arrays.asList(
                    new ColumnInfo("", "foo", "string", Types.VARCHAR, 0),
                    new ColumnInfo("", "bar", "long", Types.BIGINT, 15),
                    new ColumnInfo("", "15charwidename!", "double", Types.DOUBLE, 25),
                    new ColumnInfo("", "superduperwidename!!!", "double", Types.DOUBLE, 25),
                    new ColumnInfo("", "baz", "keyword", Types.VARCHAR, 0)),
            Arrays.asList(
                Arrays.asList("15charwidedata!", 1, 6.888, 12, "rabbit"),
                Arrays.asList("dog", 1.7976931348623157E308, 123124.888, 9912, "goat")));
    private final CliFormatter formatter = new CliFormatter(firstResponse.columns(), firstResponse.rows());

    /**
     * Tests for {@link CliFormatter#formatWithHeader}, values
     * of exactly the minimum column size, column names of exactly
     * the minimum column size, column headers longer than the
     * minimum column size, and values longer than the minimum
     * column size.
     */
    public void testFormatWithHeader() {
        String[] result = formatter.formatWithHeader(firstResponse.columns(), firstResponse.rows()).split("\n");
        assertThat(result, arrayWithSize(4));
        assertEquals("      foo      |         bar          |15charwidename!|superduperwidename!!!|      baz      ", result[0]);
        assertEquals("---------------+----------------------+---------------+---------------------+---------------", result[1]);
        assertEquals("15charwidedata!|1                     |6.888          |12                   |rabbit         ", result[2]);
        assertEquals("dog            |1.7976931348623157E308|123124.888     |9912                 |goat           ", result[3]);
    }

    /**
     * Tests for {@link CliFormatter#formatWithoutHeader} and
     * truncation of long columns.
     */
    public void testFormatWithoutHeader() {
        String[] result = formatter.formatWithoutHeader(
                Arrays.asList(
                        Arrays.asList("ohnotruncateddata", 4, 1, 77, "wombat"),
                        Arrays.asList("dog", 2, 123124.888, 9912, "goat"))).split("\n");
        assertThat(result, arrayWithSize(2));
        assertEquals("ohnotruncatedd~|4                     |1              |77                   |wombat         ", result[0]);
        assertEquals("dog            |2                     |123124.888     |9912                 |goat           ", result[1]);
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
