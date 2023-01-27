/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.formatter;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.action.ColumnInfo;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;

import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.arrayWithSize;

public class TextFormatterTests extends ESTestCase {
    private final List<ColumnInfo> columns = Arrays.asList(
        new ColumnInfo("foo", "string"),
        new ColumnInfo("bar", "long"),
        new ColumnInfo("15charwidename!", "double"),
        new ColumnInfo("null_field1", "integer"),
        new ColumnInfo("superduperwidename!!!", "double"),
        new ColumnInfo("baz", "keyword"),
        new ColumnInfo("date", "datetime"),
        new ColumnInfo("null_field2", "keyword")
    );
    EsqlQueryResponse esqlResponse = new EsqlQueryResponse(
        columns,
        Arrays.asList(
            Arrays.asList("15charwidedata!", 1, 6.888, null, 12, "rabbit", "1953-09-02T00:00:00.000Z", null),
            Arrays.asList("dog", 1.7976931348623157E308, 123124.888, null, 9912, "goat", "2000-03-15T21:34:37.443Z", null)
        ),
        randomBoolean()
    );

    TextFormatter formatter = new TextFormatter(esqlResponse);

    /**
     * Tests for {@link TextFormatter#format} with header, values
     * of exactly the minimum column size, column names of exactly
     * the minimum column size, column headers longer than the
     * minimum column size, and values longer than the minimum
     * column size.
     */
    public void testFormatWithHeader() {
        String[] result = formatter.format(true).split("\n");
        assertThat(result, arrayWithSize(4));
        assertEquals(
            "      foo      |         bar          |15charwidename!|  null_field1  |superduperwidename!!!|      baz      |"
                + "          date          |  null_field2  ",
            result[0]
        );
        assertEquals(
            "---------------+----------------------+---------------+---------------+---------------------+---------------+"
                + "------------------------+---------------",
            result[1]
        );
        assertEquals(
            "15charwidedata!|1                     |6.888          |null           |12                   |rabbit         |"
                + "1953-09-02T00:00:00.000Z|null           ",
            result[2]
        );
        assertEquals(
            "dog            |1.7976931348623157E308|123124.888     |null           |9912                 |goat           |"
                + "2000-03-15T21:34:37.443Z|null           ",
            result[3]
        );
    }

    /**
     * Tests for {@link TextFormatter#format} without header and
     * truncation of long columns.
     */
    public void testFormatWithoutHeader() {
        EsqlQueryResponse response = new EsqlQueryResponse(
            columns,
            Arrays.asList(
                Arrays.asList("doggie", 4, 1, null, 77, "wombat", "1955-01-21T01:02:03.342Z", null),
                Arrays.asList("dog", 2, 123124.888, null, 9912, "goat", "2231-12-31T23:59:59.999Z", null)
            ),
            randomBoolean()
        );

        String[] result = new TextFormatter(response).format(false).split("\n");
        assertThat(result, arrayWithSize(2));
        assertEquals(
            "doggie         |4              |1              |null           |77                   |wombat         |"
                + "1955-01-21T01:02:03.342Z|null           ",
            result[0]
        );
        assertEquals(
            "dog            |2              |123124.888     |null           |9912                 |goat           |"
                + "2231-12-31T23:59:59.999Z|null           ",
            result[1]
        );
    }

    /**
     * Ensure that our estimates are perfect in at least some cases.
     */
    public void testEstimateSize() {
        assertEquals(formatter.format(true).length(), formatter.estimateSize(esqlResponse.values().size() + 2));
    }
}
