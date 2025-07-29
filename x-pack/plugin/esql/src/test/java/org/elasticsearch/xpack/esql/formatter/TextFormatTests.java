/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.formatter;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BytesRefArray;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.esql.action.ColumnInfoImpl;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;
import org.elasticsearch.xpack.esql.core.util.StringUtils;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.elasticsearch.rest.RestResponseUtils.getTextBodyContent;
import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.CARTESIAN;
import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.GEO;
import static org.elasticsearch.xpack.esql.formatter.TextFormat.CSV;
import static org.elasticsearch.xpack.esql.formatter.TextFormat.PLAIN_TEXT;
import static org.elasticsearch.xpack.esql.formatter.TextFormat.TSV;

public class TextFormatTests extends ESTestCase {

    static final BlockFactory blockFactory = TestBlockFactory.getNonBreakingInstance();

    public void testCsvContentType() {
        assertEquals("text/csv; charset=utf-8; header=present", CSV.contentType(req()));
    }

    public void testCsvContentTypeWithoutHeader() {
        assertEquals("text/csv; charset=utf-8; header=absent", CSV.contentType(reqWithParam("header", "absent")));
    }

    public void testTsvContentType() {
        assertEquals("text/tab-separated-values; charset=utf-8", TSV.contentType(req()));
    }

    public void testCsvEscaping() {
        assertEscapedCorrectly("string", CSV, CSV.delimiter(), "string");
        assertEscapedCorrectly("", CSV, CSV.delimiter(), "");
        assertEscapedCorrectly("\"", CSV, CSV.delimiter(), "\"\"\"\"");
        assertEscapedCorrectly("\",\"", CSV, CSV.delimiter(), "\"\"\",\"\"\"");
        assertEscapedCorrectly("\"quo\"ted\"", CSV, CSV.delimiter(), "\"\"\"quo\"\"ted\"\"\"");
        assertEscapedCorrectly("one;two", CSV, ';', "\"one;two\"");
        assertEscapedCorrectly("one\ntwo", CSV, CSV.delimiter(), "\"one\ntwo\"");
        assertEscapedCorrectly("one\rtwo", CSV, CSV.delimiter(), "\"one\rtwo\"");

        final String inputString = randomStringForEscaping();
        final String expectedResult;
        if (inputString.contains(",") || inputString.contains("\n") || inputString.contains("\r") || inputString.contains("\"")) {
            expectedResult = "\"" + inputString.replaceAll("\"", "\"\"") + "\"";
        } else {
            expectedResult = inputString;
        }
        assertEscapedCorrectly(inputString, CSV, ',', expectedResult);
    }

    public void testTsvEscaping() {
        assertEscapedCorrectly("string", TSV, null, "string");
        assertEscapedCorrectly("", TSV, null, "");
        assertEscapedCorrectly("\"", TSV, null, "\"");
        assertEscapedCorrectly("\t", TSV, null, "\\t");
        assertEscapedCorrectly("\n\"\t", TSV, null, "\\n\"\\t");

        final var inputString = randomStringForEscaping();
        final var expectedResult = new StringBuilder();
        for (int i = 0; i < inputString.length(); i++) {
            final var c = inputString.charAt(i);
            switch (c) {
                case '\n' -> expectedResult.append("\\n");
                case '\t' -> expectedResult.append("\\t");
                default -> expectedResult.append(c);
            }
        }
        assertEscapedCorrectly(inputString, TSV, null, expectedResult.toString());
    }

    private static String randomStringForEscaping() {
        return String.join("", randomList(20, () -> randomFrom("a", "b", ",", ";", "\n", "\r", "\t", "\"")));
    }

    private static void assertEscapedCorrectly(String inputString, TextFormat format, Character delimiter, String expectedString) {
        try (var writer = new StringWriter()) {
            format.writeEscaped(inputString, delimiter, writer);
            writer.flush();
            assertEquals(expectedString, writer.toString());
        } catch (IOException e) {
            throw new AssertionError("impossible", e);
        }
    }

    public void testCsvFormatWithEmptyData() {
        String text = format(CSV, req(), emptyData());
        assertEquals("name\r\n", text);
    }

    public void testTsvFormatWithEmptyData() {
        String text = format(TSV, req(), emptyData());
        assertEquals("name\n", text);
    }

    public void testCsvFormatWithRegularData() {
        String text = format(CSV, req(), regularData());
        assertEquals("""
            string,number,location,location2,null_field\r
            Along The River Bank,708,POINT (12.0 56.0),POINT (1234.0 5678.0),\r
            Mind Train,280,POINT (-97.0 26.0),POINT (-9753.0 2611.0),\r
            """, text);
    }

    public void testCsvFormatNoHeaderWithRegularData() {
        String text = format(CSV, reqWithParam("header", "absent"), regularData());
        assertEquals("""
            Along The River Bank,708,POINT (12.0 56.0),POINT (1234.0 5678.0),\r
            Mind Train,280,POINT (-97.0 26.0),POINT (-9753.0 2611.0),\r
            """, text);
    }

    public void testCsvFormatWithCustomDelimiterRegularData() {
        Set<Character> forbidden = Set.of('"', '\r', '\n', '\t');
        Character delim = randomValueOtherThanMany(forbidden::contains, () -> randomAlphaOfLength(1).charAt(0));
        String text = format(CSV, reqWithParam("delimiter", String.valueOf(delim)), regularData());
        List<String> terms = Arrays.asList(
            "string",
            "number",
            "location",
            "location2",
            "null_field",
            "Along The River Bank",
            "708",
            "POINT (12.0 56.0)",
            "POINT (1234.0 5678.0)",
            "",
            "Mind Train",
            "280",
            "POINT (-97.0 26.0)",
            "POINT (-9753.0 2611.0)",
            ""
        );
        List<String> expectedTerms = terms.stream()
            .map(x -> x.contains(String.valueOf(delim)) ? '"' + x + '"' : x)
            .collect(Collectors.toList());
        StringBuilder sb = new StringBuilder();
        do {
            sb.append(expectedTerms.remove(0));
            sb.append(delim);
            sb.append(expectedTerms.remove(0));
            sb.append(delim);
            sb.append(expectedTerms.remove(0));
            sb.append(delim);
            sb.append(expectedTerms.remove(0));
            sb.append(delim);
            sb.append(expectedTerms.remove(0));
            sb.append("\r\n");
        } while (expectedTerms.size() > 0);
        assertEquals(sb.toString(), text);
    }

    public void testTsvFormatWithRegularData() {
        String text = format(TSV, req(), regularData());
        assertEquals("""
            string\tnumber\tlocation\tlocation2\tnull_field
            Along The River Bank\t708\tPOINT (12.0 56.0)\tPOINT (1234.0 5678.0)\t
            Mind Train\t280\tPOINT (-97.0 26.0)\tPOINT (-9753.0 2611.0)\t
            """, text);
    }

    public void testCsvFormatWithEscapedData() {
        String text = format(CSV, req(), escapedData());
        assertEquals("""
            first,""\"special""\"\r
            normal,""\"quo""ted"",
            "\r
            commas,"a,b,c,
            ,d,e,\t
            "\r
            """, text);
    }

    public void testCsvFormatWithCustomDelimiterEscapedData() {
        String text = format(CSV, reqWithParam("delimiter", "\\"), escapedData());
        assertEquals("""
            first\\""\"special""\"\r
            normal\\""\"quo""ted"",
            "\r
            commas\\"a,b,c,
            ,d,e,\t
            "\r
            """, text);
    }

    public void testTsvFormatWithEscapedData() {
        String text = format(TSV, req(), escapedData());
        assertEquals("""
            first\t"special"
            normal\t"quo"ted",\\n
            commas\ta,b,c,\\n,d,e,\\t\\n
            """, text);
    }

    public void testInvalidCsvDelims() {
        List<String> invalid = Arrays.asList("\"", "\r", "\n", "\t", "", "ab");

        for (String c : invalid) {
            Exception e = expectThrows(IllegalArgumentException.class, () -> format(CSV, reqWithParam("delimiter", c), emptyData()));
            String msg;
            if (c.length() == 1) {
                msg = c.equals("\t")
                    ? "illegal delimiter [TAB] specified as delimiter for the [csv] format; choose the [tsv] format instead"
                    : "illegal reserved character specified as delimiter [" + c + "]";
            } else {
                msg = "invalid " + (c.length() > 0 ? "multi-character" : "empty") + " delimiter [" + c + "]";
            }
            assertEquals(msg, e.getMessage());
        }
    }

    public void testPlainTextEmptyCursorWithColumns() {
        assertEquals("""
                 name     \s
            ---------------
            """, format(PLAIN_TEXT, req(), emptyData()));
    }

    public void testPlainTextEmptyCursorWithoutColumns() {
        assertEquals(
            StringUtils.EMPTY,
            getTextBodyContent(PLAIN_TEXT.format(req(), new EsqlQueryResponse(emptyList(), emptyList(), null, false, false, null)))
        );
    }

    public void testCsvFormatWithDropNullColumns() {
        String text = format(CSV, reqWithParam("drop_null_columns", "true"), regularData());
        assertEquals("""
            string,number,location,location2\r
            Along The River Bank,708,POINT (12.0 56.0),POINT (1234.0 5678.0)\r
            Mind Train,280,POINT (-97.0 26.0),POINT (-9753.0 2611.0)\r
            """, text);
    }

    public void testTsvFormatWithDropNullColumns() {
        String text = format(TSV, reqWithParam("drop_null_columns", "true"), regularData());
        assertEquals("""
            string\tnumber\tlocation\tlocation2
            Along The River Bank\t708\tPOINT (12.0 56.0)\tPOINT (1234.0 5678.0)
            Mind Train\t280\tPOINT (-97.0 26.0)\tPOINT (-9753.0 2611.0)
            """, text);
    }

    private static EsqlQueryResponse emptyData() {
        return new EsqlQueryResponse(singletonList(new ColumnInfoImpl("name", "keyword")), emptyList(), null, false, false, null);
    }

    private static EsqlQueryResponse regularData() {
        BlockFactory blockFactory = TestBlockFactory.getNonBreakingInstance();
        // headers
        List<ColumnInfoImpl> headers = asList(
            new ColumnInfoImpl("string", "keyword"),
            new ColumnInfoImpl("number", "integer"),
            new ColumnInfoImpl("location", "geo_point"),
            new ColumnInfoImpl("location2", "cartesian_point"),
            new ColumnInfoImpl("null_field", "keyword")
        );

        BytesRefArray geoPoints = new BytesRefArray(2, BigArrays.NON_RECYCLING_INSTANCE);
        geoPoints.append(GEO.asWkb(new Point(12, 56)));
        geoPoints.append(GEO.asWkb(new Point(-97, 26)));
        // values
        List<Page> values = List.of(
            new Page(
                blockFactory.newBytesRefBlockBuilder(2)
                    .appendBytesRef(new BytesRef("Along The River Bank"))
                    .appendBytesRef(new BytesRef("Mind Train"))
                    .build(),
                blockFactory.newIntArrayVector(new int[] { 11 * 60 + 48, 4 * 60 + 40 }, 2).asBlock(),
                blockFactory.newBytesRefArrayVector(geoPoints, 2).asBlock(),
                blockFactory.newBytesRefBlockBuilder(2)
                    .appendBytesRef(CARTESIAN.asWkb(new Point(1234, 5678)))
                    .appendBytesRef(CARTESIAN.asWkb(new Point(-9753, 2611)))
                    .build(),
                blockFactory.newConstantNullBlock(2)
            )
        );

        return new EsqlQueryResponse(headers, values, null, false, false, null);
    }

    private static EsqlQueryResponse escapedData() {
        // headers
        List<ColumnInfoImpl> headers = asList(new ColumnInfoImpl("first", "keyword"), new ColumnInfoImpl("\"special\"", "keyword"));

        // values
        List<Page> values = List.of(
            new Page(
                blockFactory.newBytesRefBlockBuilder(2)
                    .appendBytesRef(new BytesRef("normal"))
                    .appendBytesRef(new BytesRef("commas"))
                    .build(),
                blockFactory.newBytesRefBlockBuilder(2)
                    .appendBytesRef(new BytesRef("\"quo\"ted\",\n"))
                    .appendBytesRef(new BytesRef("a,b,c,\n,d,e,\t\n"))
                    .build()
            )
        );

        return new EsqlQueryResponse(headers, values, null, false, false, null);
    }

    private static RestRequest req() {
        return new FakeRestRequest();
    }

    private static RestRequest reqWithParam(String paramName, String paramVal) {
        return new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withParams(singletonMap(paramName, paramVal)).build();
    }

    private String format(TextFormat format, RestRequest request, EsqlQueryResponse response) {
        return getTextBodyContent(format.format(request, response));
    }
}
