/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.formatter;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleArrayVector;
import org.elasticsearch.compute.data.LongArrayVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.action.ColumnInfo;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;

import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.xpack.ql.util.DateUtils.UTC_DATE_TIME_FORMATTER;
import static org.hamcrest.Matchers.arrayWithSize;

public class TextFormatterTests extends ESTestCase {
    private final List<ColumnInfo> columns = Arrays.asList(
        new ColumnInfo("foo", "keyword"),
        new ColumnInfo("bar", "long"),
        new ColumnInfo("15charwidename!", "double"),
        new ColumnInfo("null_field1", "integer"),
        new ColumnInfo("superduperwidename!!!", "double"),
        new ColumnInfo("baz", "keyword"),
        new ColumnInfo("date", "date"),
        new ColumnInfo("null_field2", "keyword")
    );
    EsqlQueryResponse esqlResponse = new EsqlQueryResponse(
        columns,
        List.of(
            new Page(
                BytesRefBlock.newBlockBuilder(2)
                    .appendBytesRef(new BytesRef("15charwidedata!"))
                    .appendBytesRef(new BytesRef("dog"))
                    .build(),
                new LongArrayVector(new long[] { 1, 2 }, 2).asBlock(),
                new DoubleArrayVector(new double[] { 6.888, 123124.888 }, 2).asBlock(),
                Block.constantNullBlock(2),
                new DoubleArrayVector(new double[] { 12, 9912 }, 2).asBlock(),
                BytesRefBlock.newBlockBuilder(2).appendBytesRef(new BytesRef("rabbit")).appendBytesRef(new BytesRef("goat")).build(),
                new LongArrayVector(
                    new long[] {
                        UTC_DATE_TIME_FORMATTER.parseMillis("1953-09-02T00:00:00.000Z"),
                        UTC_DATE_TIME_FORMATTER.parseMillis("2000-03-15T21:34:37.443Z") },
                    2
                ).asBlock(),
                Block.constantNullBlock(2)
            )
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
            "      foo      |      bar      |15charwidename!|  null_field1  |superduperwidename!!!|      baz      |"
                + "          date          |  null_field2  ",
            result[0]
        );
        assertEquals(
            "---------------+---------------+---------------+---------------+---------------------+---------------+"
                + "------------------------+---------------",
            result[1]
        );
        assertEquals(
            "15charwidedata!|1              |6.888          |null           |12.0                 |rabbit         |"
                + "1953-09-02T00:00:00.000Z|null           ",
            result[2]
        );
        assertEquals(
            "dog            |2              |123124.888     |null           |9912.0               |goat           |"
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
            List.of(
                new Page(
                    BytesRefBlock.newBlockBuilder(2).appendBytesRef(new BytesRef("doggie")).appendBytesRef(new BytesRef("dog")).build(),
                    new LongArrayVector(new long[] { 4, 2 }, 2).asBlock(),
                    new DoubleArrayVector(new double[] { 1, 123124.888 }, 2).asBlock(),
                    Block.constantNullBlock(2),
                    new DoubleArrayVector(new double[] { 77.0, 9912.0 }, 2).asBlock(),
                    BytesRefBlock.newBlockBuilder(2).appendBytesRef(new BytesRef("wombat")).appendBytesRef(new BytesRef("goat")).build(),
                    new LongArrayVector(
                        new long[] {
                            UTC_DATE_TIME_FORMATTER.parseMillis("1955-01-21T01:02:03.342Z"),
                            UTC_DATE_TIME_FORMATTER.parseMillis("2231-12-31T23:59:59.999Z") },
                        2
                    ).asBlock(),
                    Block.constantNullBlock(2)
                )
            ),
            randomBoolean()
        );

        String[] result = new TextFormatter(response).format(false).split("\n");
        assertThat(result, arrayWithSize(2));
        assertEquals(
            "doggie         |4              |1.0            |null           |77.0                 |wombat         |"
                + "1955-01-21T01:02:03.342Z|null           ",
            result[0]
        );
        assertEquals(
            "dog            |2              |123124.888     |null           |9912.0               |goat           |"
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
