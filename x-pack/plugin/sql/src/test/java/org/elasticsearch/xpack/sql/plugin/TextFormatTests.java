/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.plugin;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.xpack.sql.action.SqlQueryResponse;
import org.elasticsearch.xpack.sql.expression.literal.geo.GeoShape;
import org.elasticsearch.xpack.sql.proto.ColumnInfo;
import org.elasticsearch.xpack.sql.proto.Mode;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.elasticsearch.xpack.sql.plugin.TextFormat.CSV;
import static org.elasticsearch.xpack.sql.plugin.TextFormat.TSV;
import static org.elasticsearch.xpack.sql.proto.SqlVersion.DATE_NANOS_SUPPORT_VERSION;
import static org.elasticsearch.xpack.sql.type.SqlDataTypes.fromJava;

public class TextFormatTests extends ESTestCase {

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
        assertEquals("string", CSV.maybeEscape("string", CSV.delimiter()));
        assertEquals("", CSV.maybeEscape("", CSV.delimiter()));
        assertEquals("\"\"\"\"", CSV.maybeEscape("\"", CSV.delimiter()));
        assertEquals("\"\"\",\"\"\"", CSV.maybeEscape("\",\"", CSV.delimiter()));
        assertEquals("\"\"\"quo\"\"ted\"\"\"", CSV.maybeEscape("\"quo\"ted\"", CSV.delimiter()));
        assertEquals("\"one;two\"", CSV.maybeEscape("one;two", ';'));
    }

    public void testTsvEscaping() {
        assertEquals("string", TSV.maybeEscape("string", null));
        assertEquals("", TSV.maybeEscape("", null));
        assertEquals("\"", TSV.maybeEscape("\"", null));
        assertEquals("\\t", TSV.maybeEscape("\t", null));
        assertEquals("\\n\"\\t", TSV.maybeEscape("\n\"\t", null));
    }

    public void testCsvFormatWithEmptyData() {
        String text = CSV.format(req(), emptyData());
        assertEquals("name\r\n", text);
    }

    public void testTsvFormatWithEmptyData() {
        String text = TSV.format(req(), emptyData());
        assertEquals("name\n", text);
    }

    public void testCsvFormatWithRegularData() {
        String text = CSV.format(req(), regularData());
        assertEquals("string,number\r\n" +
                "Along The River Bank,708\r\n" +
                "Mind Train,280\r\n",
            text);
    }

    public void testCsvFormatNoHeaderWithRegularData() {
        String text = CSV.format(reqWithParam("header", "absent"), regularData());
        assertEquals("Along The River Bank,708\r\n" +
                "Mind Train,280\r\n",
            text);
    }

    public void testCsvFormatWithCustomDelimiterRegularData() {
        Set<Character> forbidden = Set.of('"', '\r', '\n', '\t');
        Character delim = randomValueOtherThanMany(forbidden::contains, () -> randomAlphaOfLength(1).charAt(0));
        String text = CSV.format(reqWithParam("delimiter", String.valueOf(delim)), regularData());
        List<String> terms = Arrays.asList("string", "number", "Along The River Bank", "708", "Mind Train", "280");
        List<String> expectedTerms = terms.stream()
            .map(x -> x.contains(String.valueOf(delim)) ? '"' + x + '"' : x)
            .collect(Collectors.toList());
        StringBuffer sb = new StringBuffer();
        do {
            sb.append(expectedTerms.remove(0));
            sb.append(delim);
            sb.append(expectedTerms.remove(0));
            sb.append("\r\n");
        } while (expectedTerms.size() > 0);
        assertEquals(sb.toString(), text);
    }

    public void testTsvFormatWithRegularData() {
        String text = TSV.format(req(), regularData());
        assertEquals("string\tnumber\n" +
                "Along The River Bank\t708\n" +
                "Mind Train\t280\n",
                text);
    }

    public void testCsvFormatWithEscapedData() {
        String text = CSV.format(req(), escapedData());
        assertEquals("first,\"\"\"special\"\"\"\r\n" +
                "normal,\"\"\"quo\"\"ted\"\",\n\"\r\n" +
                "commas,\"a,b,c,\n,d,e,\t\n\"\r\n"
            , text);
    }

    public void testCsvFormatWithCustomDelimiterEscapedData() {
        String text = CSV.format(reqWithParam("delimiter", "\\"), escapedData());
        assertEquals("first\\\"\"\"special\"\"\"\r\n" +
                "normal\\\"\"\"quo\"\"ted\"\",\n\"\r\n" +
                "commas\\\"a,b,c,\n,d,e,\t\n\"\r\n"
                , text);
    }

    public void testTsvFormatWithEscapedData() {
        String text = TSV.format(req(), escapedData());
        assertEquals("first\t\"special\"\n" +
                "normal\t\"quo\"ted\",\\n\n" +
                "commas\ta,b,c,\\n,d,e,\\t\\n\n"
                , text);
    }

    public void testInvalidCsvDelims() {
        List<String> invalid = Arrays.asList("\"", "\r", "\n", "\t", "", "ab");

        for (String c: invalid) {
            Exception e = expectThrows(IllegalArgumentException.class,
                () -> CSV.format(reqWithParam("delimiter", c), emptyData()));
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

    public void testCsvMultiValueEmpty() {
        String text = CSV.format(req(), withData(singletonList(singletonList(emptyList()))));
        assertEquals("null_column\r\n[]\r\n", text);
    }

    public void testTsvMultiValueEmpty() {
        String text = TSV.format(req(), withData(singletonList(singletonList(emptyList()))));
        assertEquals("null_column\n[]\n", text);
    }

    public void testCsvMultiValueNull() {
        String text = CSV.format(req(), withData(singletonList(singletonList(singletonList(null)))));
        assertEquals("null_column\r\n[NULL]\r\n", text);
    }

    public void testTsvMultiValueNull() {
        String text = TSV.format(req(), withData(singletonList(singletonList(asList(null, null)))));
        assertEquals("null_column\n[NULL,NULL]\n", text);
    }

    public void testCsvMultiValueKeywords() {
        String text = CSV.format(req(), withData(singletonList(singletonList(asList("one", "two", "one, two")))));
        assertEquals("keyword_column\r\n\"[\"\"one\"\",\"\"two\"\",\"\"one, two\"\"]\"\r\n", text);
    }

    public void testTsvMultiValueKeywords() {
        String text = TSV.format(req(), withData(singletonList(singletonList(asList("one", "two", "one, two")))));
        assertEquals("keyword_column\n[\"one\",\"two\",\"one, two\"]\n", text);
    }

    public void testCsvMultiValueKeywordWithQuote() {
        String text = CSV.format(req(), withData(singletonList(singletonList(asList("one", "two", "one\"two")))));
        assertEquals("keyword_column\r\n\"[\"\"one\"\",\"\"two\"\",\"\"one\\\"\"two\"\"]\"\r\n", text);
    }

    public void testTsvMultiValueKeywordWithQuote() {
        String text = TSV.format(req(), withData(singletonList(singletonList(asList("one", "two", "one\"two")))));
        assertEquals("keyword_column\n[\"one\",\"two\",\"one\\\"two\"]\n", text);
    }

    public void testTsvMultiValueKeywordWithTab() {
        String text = TSV.format(req(), withData(singletonList(singletonList(asList("one", "two", "one\ttwo")))));
        assertEquals("keyword_column\n[\"one\",\"two\",\"one\\ttwo\"]\n", text);
    }

    public void testCsvMultiValueKeywordTwoColumns() {
        String text = CSV.format(req(), withData(singletonList(asList(asList("one", "two", "three"), asList("4", "5")))));
        assertEquals("keyword_column,keyword_column\r\n\"[\"\"one\"\",\"\"two\"\",\"\"three\"\"]\",\"[\"\"4\"\",\"\"5\"\"]\"\r\n", text);
    }

    public void testTsvMultiValueKeywordTwoColumns() {
        String text = TSV.format(req(), withData(singletonList(asList(asList("one", "two", "three"), asList("4", "5")))));
        assertEquals("keyword_column\tkeyword_column\n[\"one\",\"two\",\"three\"]\t[\"4\",\"5\"]\n", text);
    }

    public void testCsvMultiValueBooleans() {
        String text = CSV.format(req(), withData(singletonList(singletonList(asList(true, false, true)))));
        assertEquals("boolean_column\r\n\"[true,false,true]\"\r\n", text);
    }

    public void testTsvMultiValueBooleans() {
        String text = TSV.format(req(), withData(singletonList(singletonList(asList(true, false, true)))));
        assertEquals("boolean_column\n[true,false,true]\n", text);
    }

    public void testCsvMultiValueIntegers() {
        String text = CSV.format(req(), withData(singletonList(asList(
            asList((byte) 1, (byte) 2), asList((short) 3, (short) 4), asList(5, 6), asList(7L, 8L)
        ))));
        assertEquals("byte_column,short_column,integer_column,long_column\r\n\"[1,2]\",\"[3,4]\",\"[5,6]\",\"[7,8]\"\r\n", text);
    }

    public void testTsvMultiValueIntegers() {
        String text = TSV.format(req(), withData(singletonList(asList(
            asList((byte) 1, (byte) 2), asList((short) 3, (short) 4), asList(5, 6), asList(7L, 8L)
        ))));
        assertEquals("byte_column\tshort_column\tinteger_column\tlong_column\n[1,2]\t[3,4]\t[5,6]\t[7,8]\n", text);
    }

    public void testCsvMultiValueFloatingPoints() {
        String text = CSV.format(req(), withData(singletonList(asList(asList(1.1f, 2.2f), asList(3.3d, 4.4d)))));
        assertEquals("float_column,double_column\r\n\"[1.1,2.2]\",\"[3.3,4.4]\"\r\n", text);
    }

    public void testTsvMultiValueFloatingPoints() {
        String text = TSV.format(req(), withData(singletonList(asList(asList(1.1f, 2.2f), asList(3.3d, 4.4d)))));
        assertEquals("float_column\tdouble_column\n[1.1,2.2]\t[3.3,4.4]\n", text);
    }

    public void testCsvMultiValueDates() {
        String date1 = "2020-02-02T02:02:02.222+03:00";
        String date2 = "1969-01-23T23:34:56.123456789+13:30";
        String text = CSV.format(req(), withData(singletonList(singletonList(
            asList(ZonedDateTime.parse(date1), ZonedDateTime.parse(date2))
        ))));
        assertEquals("datetime_column\r\n\"[" + date1 + "," + date2 + "]\"\r\n", text);
    }

    public void testTsvMultiValueDates() {
        String date1 = "2020-02-02T02:02:02.222+03:00";
        String date2 = "1969-01-23T23:34:56.123456789+13:30";
        String text = TSV.format(req(), withData(singletonList(singletonList(
            asList(ZonedDateTime.parse(date1), ZonedDateTime.parse(date2))
        ))));
        assertEquals("datetime_column\n[" + date1 + "," + date2 + "]\n", text);
    }

    public void testCsvMultiValueGeoPoints() {
        GeoShape point1 = new GeoShape(12.34, 56.78);
        double lat = randomDouble(), lon = randomDouble();
        GeoShape point2 = new GeoShape(lat, lon);
        String text = CSV.format(req(), withData(singletonList(singletonList(asList(point1, point2)))));
        assertEquals("geo_shape_column\r\n\"[POINT (12.34 56.78),POINT (" + lat + " " + lon + ")]\"\r\n", text);
    }

    public void testTsvMultiValueGeoPoints() {
        GeoShape point1 = new GeoShape(12.34, 56.78);
        double lat = randomDouble(), lon = randomDouble();
        GeoShape point2 = new GeoShape(lat, lon);
        String text = TSV.format(req(), withData(singletonList(singletonList(asList(point1, point2)))));
        assertEquals("geo_shape_column\n[POINT (12.34 56.78),POINT (" + lat + " " + lon + ")]\n", text);
    }

    public void testCsvMultiValueSingletons() {
        String text = CSV.format(req(), withData(singletonList(asList(emptyList(), singletonList(false), singletonList("string"),
            singletonList(1), singletonList(2.), singletonList(new GeoShape(12.34, 56.78))))));
        assertEquals("null_column,boolean_column,keyword_column,integer_column,double_column,geo_shape_column\r\n" +
            "[],[false],\"[\"\"string\"\"]\",[1],[2.0],[POINT (12.34 56.78)]\r\n", text);
    }

    public void testCsvMultiValueWithDelimiter() {
        String text = CSV.format(reqWithParam("delimiter", String.valueOf("|")),
            withData(singletonList(asList(emptyList(), asList(null, null), asList(false, true), asList("string", "strung"),
            asList(1, 2), asList(3.3, 4.), asList(new GeoShape(12.34, 56.78), new GeoShape(90, 10))))));
        assertEquals("null_column|null_column|boolean_column|keyword_column|integer_column|double_column|geo_shape_column\r\n" +
            "[]|[NULL,NULL]|[false,true]|\"[\"\"string\"\",\"\"strung\"\"]\"|[1,2]|[3.3,4.0]|[POINT (12.34 56.78),POINT (90.0 10.0)]\r\n",
            text);
    }

    public void testTsvMultiValueSingletons() {
        String text = TSV.format(req(), withData(singletonList(asList(emptyList(), singletonList(false), singletonList("string"),
            singletonList(1), singletonList(2.), singletonList(new GeoShape(12.34, 56.78))))));
        assertEquals("null_column\tboolean_column\tkeyword_column\tinteger_column\tdouble_column\tgeo_shape_column\n" +
            "[]\t[false]\t[\"string\"]\t[1]\t[2.0]\t[POINT (12.34 56.78)]\n", text);
    }

    private static SqlQueryResponse emptyData() {
        return new SqlQueryResponse(
            null,
            Mode.JDBC,
            DATE_NANOS_SUPPORT_VERSION,
            false,
            singletonList(new ColumnInfo("index", "name", "keyword")),
            emptyList()
        );
    }

    private static SqlQueryResponse withData(List<List<Object>> rows) {
        List<ColumnInfo> headers = new ArrayList<>();
        if (rows.isEmpty() == false) {
            // headers
            for (Object o : rows.get(0)) {
                if (o instanceof Collection) {
                    Collection<?> col = (Collection<?>) o;
                    o = col.isEmpty() ? null : col.toArray()[0];
                }

                String typeName = fromJava(o).typeName();
                headers.add(new ColumnInfo("index", typeName + "_column", typeName + "_array"));
            }
        }

        return new SqlQueryResponse(null, Mode.JDBC, DATE_NANOS_SUPPORT_VERSION, false, headers, rows);
    }

    private static SqlQueryResponse regularData() {
        // headers
        List<ColumnInfo> headers = new ArrayList<>();
        headers.add(new ColumnInfo("index", "string", "keyword"));
        headers.add(new ColumnInfo("index", "number", "integer"));

        // values
        List<List<Object>> values = new ArrayList<>();
        values.add(asList("Along The River Bank", 11 * 60 + 48));
        values.add(asList("Mind Train", 4 * 60 + 40));

        return new SqlQueryResponse(null, Mode.JDBC, DATE_NANOS_SUPPORT_VERSION, false, headers, values);
    }

    private static SqlQueryResponse escapedData() {
        // headers
        List<ColumnInfo> headers = new ArrayList<>();
        headers.add(new ColumnInfo("index", "first", "keyword"));
        headers.add(new ColumnInfo("index", "\"special\"", "keyword"));

        // values
        List<List<Object>> values = new ArrayList<>();
        values.add(asList("normal", "\"quo\"ted\",\n"));
        values.add(asList("commas", "a,b,c,\n,d,e,\t\n"));

        return new SqlQueryResponse(null, Mode.JDBC, DATE_NANOS_SUPPORT_VERSION, false, headers, values);
    }

    private static RestRequest req() {
        return new FakeRestRequest();
    }

    private static RestRequest reqWithParam(String paramName, String paramVal) {
        return new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withParams(singletonMap(paramName, paramVal)).build();
    }
}
