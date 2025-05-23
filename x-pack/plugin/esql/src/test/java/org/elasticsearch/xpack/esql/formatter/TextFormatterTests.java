/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.formatter;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BytesRefArray;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.action.ColumnInfoImpl;
import org.elasticsearch.xpack.esql.action.EsqlExecutionInfo;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;

import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.rest.RestResponseUtils.getTextBodyContent;
import static org.elasticsearch.xpack.esql.core.util.DateUtils.UTC_DATE_TIME_FORMATTER;
import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.CARTESIAN;
import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.GEO;
import static org.hamcrest.Matchers.arrayWithSize;

public class TextFormatterTests extends ESTestCase {

    static BlockFactory blockFactory = TestBlockFactory.getNonBreakingInstance();

    private final List<ColumnInfoImpl> columns = Arrays.asList(
        new ColumnInfoImpl("foo", "keyword", null),
        new ColumnInfoImpl("bar", "long", null),
        new ColumnInfoImpl("15charwidename!", "double", null),
        new ColumnInfoImpl("null_field1", "integer", null),
        new ColumnInfoImpl("superduperwidename!!!", "double", null),
        new ColumnInfoImpl("baz", "keyword", null),
        new ColumnInfoImpl("date", "date", null),
        new ColumnInfoImpl("location", "geo_point", null),
        new ColumnInfoImpl("location2", "cartesian_point", null),
        new ColumnInfoImpl("null_field2", "keyword", null)
    );

    private static final BytesRefArray geoPoints = new BytesRefArray(2, BigArrays.NON_RECYCLING_INSTANCE);
    static {
        geoPoints.append(GEO.asWkb(new Point(12, 56)));
        geoPoints.append(GEO.asWkb(new Point(-97, 26)));
    }

    EsqlQueryResponse esqlResponse = new EsqlQueryResponse(
        columns,
        List.of(
            new Page(
                blockFactory.newBytesRefBlockBuilder(2)
                    .appendBytesRef(new BytesRef("15charwidedata!"))
                    .appendBytesRef(new BytesRef("dog"))
                    .build(),
                blockFactory.newLongArrayVector(new long[] { 1, 2 }, 2).asBlock(),
                blockFactory.newDoubleArrayVector(new double[] { 6.888, 123124.888 }, 2).asBlock(),
                blockFactory.newConstantNullBlock(2),
                blockFactory.newDoubleArrayVector(new double[] { 12, 9912 }, 2).asBlock(),
                blockFactory.newBytesRefBlockBuilder(2).appendBytesRef(new BytesRef("rabbit")).appendBytesRef(new BytesRef("goat")).build(),
                blockFactory.newLongArrayVector(
                    new long[] {
                        UTC_DATE_TIME_FORMATTER.parseMillis("1953-09-02T00:00:00.000Z"),
                        UTC_DATE_TIME_FORMATTER.parseMillis("2000-03-15T21:34:37.443Z") },
                    2
                ).asBlock(),
                blockFactory.newBytesRefArrayVector(geoPoints, 2).asBlock(),
                blockFactory.newBytesRefBlockBuilder(2)
                    .appendBytesRef(CARTESIAN.asWkb(new Point(1234, 5678)))
                    .appendBytesRef(CARTESIAN.asWkb(new Point(-9753, 2611)))
                    .build(),
                blockFactory.newConstantNullBlock(2)
            )
        ),
        0,
        0,
        null,
        randomBoolean(),
        randomBoolean(),
        new EsqlExecutionInfo(randomBoolean())
    );

    /**
     * Tests for {@link TextFormatter#format} with header, values
     * of exactly the minimum column size, column names of exactly
     * the minimum column size, column headers longer than the
     * minimum column size, and values longer than the minimum
     * column size.
     */
    public void testFormatWithHeader() {
        String[] result = getTextBodyContent(new TextFormatter(esqlResponse, true, false).format()).split("\n");
        assertThat(result, arrayWithSize(4));
        assertEquals(
            "      foo      |      bar      |15charwidename!|  null_field1  |superduperwidename!!!|      baz      |"
                + "          date          |     location     |      location2       |  null_field2  ",
            result[0]
        );
        assertEquals(
            "---------------+---------------+---------------+---------------+---------------------+---------------+-------"
                + "-----------------+------------------+----------------------+---------------",
            result[1]
        );
        assertEquals(
            "15charwidedata!|1              |6.888          |null           |12.0                 |rabbit         |"
                + "1953-09-02T00:00:00.000Z|POINT (12.0 56.0) |POINT (1234.0 5678.0) |null           ",
            result[2]
        );
        assertEquals(
            "dog            |2              |123124.888     |null           |9912.0               |goat           |"
                + "2000-03-15T21:34:37.443Z|POINT (-97.0 26.0)|POINT (-9753.0 2611.0)|null           ",
            result[3]
        );
    }

    /**
     * Tests for {@link TextFormatter#format} with drop_null_columns and
     * truncation of long columns.
     */
    public void testFormatWithDropNullColumns() {
        String[] result = getTextBodyContent(new TextFormatter(esqlResponse, true, true).format()).split("\n");
        assertThat(result, arrayWithSize(4));
        assertEquals(
            "      foo      |      bar      |15charwidename!|superduperwidename!!!|      baz      |"
                + "          date          |     location     |      location2       ",
            result[0]
        );
        assertEquals(
            "---------------+---------------+---------------+---------------------+---------------+-------"
                + "-----------------+------------------+----------------------",
            result[1]
        );
        assertEquals(
            "15charwidedata!|1              |6.888          |12.0                 |rabbit         |"
                + "1953-09-02T00:00:00.000Z|POINT (12.0 56.0) |POINT (1234.0 5678.0) ",
            result[2]
        );
        assertEquals(
            "dog            |2              |123124.888     |9912.0               |goat           |"
                + "2000-03-15T21:34:37.443Z|POINT (-97.0 26.0)|POINT (-9753.0 2611.0)",
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
                    blockFactory.newBytesRefBlockBuilder(2)
                        .appendBytesRef(new BytesRef("doggie"))
                        .appendBytesRef(new BytesRef("dog"))
                        .build(),
                    blockFactory.newLongArrayVector(new long[] { 4, 2 }, 2).asBlock(),
                    blockFactory.newDoubleArrayVector(new double[] { 1, 123124.888 }, 2).asBlock(),
                    blockFactory.newConstantNullBlock(2),
                    blockFactory.newDoubleArrayVector(new double[] { 77.0, 9912.0 }, 2).asBlock(),
                    blockFactory.newBytesRefBlockBuilder(2)
                        .appendBytesRef(new BytesRef("wombat"))
                        .appendBytesRef(new BytesRef("goat"))
                        .build(),
                    blockFactory.newLongArrayVector(
                        new long[] {
                            UTC_DATE_TIME_FORMATTER.parseMillis("1955-01-21T01:02:03.342Z"),
                            UTC_DATE_TIME_FORMATTER.parseMillis("2231-12-31T23:59:59.999Z") },
                        2
                    ).asBlock(),
                    blockFactory.newBytesRefArrayVector(geoPoints, 2).asBlock(),
                    blockFactory.newBytesRefBlockBuilder(2)
                        .appendBytesRef(CARTESIAN.asWkb(new Point(1234, 5678)))
                        .appendBytesRef(CARTESIAN.asWkb(new Point(-9753, 2611)))
                        .build(),
                    blockFactory.newConstantNullBlock(2)
                )
            ),
            0,
            0,
            null,
            randomBoolean(),
            randomBoolean(),
            new EsqlExecutionInfo(randomBoolean())
        );

        String[] result = getTextBodyContent(new TextFormatter(response, false, false).format()).split("\n");
        assertThat(result, arrayWithSize(2));
        assertEquals(
            "doggie         |4              |1.0            |null           |77.0                 |wombat         |"
                + "1955-01-21T01:02:03.342Z|POINT (12.0 56.0) |POINT (1234.0 5678.0) |null           ",
            result[0]
        );
        assertEquals(
            "dog            |2              |123124.888     |null           |9912.0               |goat           |"
                + "2231-12-31T23:59:59.999Z|POINT (-97.0 26.0)|POINT (-9753.0 2611.0)|null           ",
            result[1]
        );
    }

    public void testVeryLongPadding() {
        final var smallFieldContent = "is twenty characters";
        final var largeFieldContent = "a".repeat(between(smallFieldContent.length(), 200));
        final var paddingLength = largeFieldContent.length() - smallFieldContent.length();
        assertEquals(
            Strings.format("""
                is twenty characters%s
                aaaaaaaaaaaaaaaaaaaa%s
                """, " ".repeat(paddingLength), "a".repeat(paddingLength)),
            getTextBodyContent(
                new TextFormatter(
                    new EsqlQueryResponse(
                        List.of(new ColumnInfoImpl("foo", "keyword", null)),
                        List.of(
                            new Page(
                                blockFactory.newBytesRefBlockBuilder(2)
                                    .appendBytesRef(new BytesRef(smallFieldContent))
                                    .appendBytesRef(new BytesRef(largeFieldContent))
                                    .build()
                            )
                        ),
                        0,
                        0,
                        null,
                        randomBoolean(),
                        randomBoolean(),
                        new EsqlExecutionInfo(randomBoolean())
                    ),
                    false,
                    false
                ).format()
            )
        );
    }
}
