/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.plugin;

import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.sql.action.SqlQueryResponse;
import org.elasticsearch.xpack.sql.proto.ColumnInfo;
import org.elasticsearch.xpack.sql.proto.Mode;
import org.elasticsearch.xpack.sql.proto.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.elasticsearch.xpack.sql.plugin.TextFormat.CSV;
import static org.elasticsearch.xpack.sql.plugin.TextFormat.PLAIN_TEXT;
import static org.elasticsearch.xpack.sql.plugin.TextFormat.TSV;
import static org.elasticsearch.xpack.sql.proto.VersionCompatibility.INTRODUCING_DATE_NANOS;
import static org.elasticsearch.xpack.sql.proto.formatter.SimpleFormatter.FormatOption.TEXT;

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
            string,number\r
            Along The River Bank,708\r
            Mind Train,280\r
            """, text);
    }

    public void testCsvFormatNoHeaderWithRegularData() {
        String text = format(CSV, reqWithParam("header", "absent"), regularData());
        assertEquals("""
            Along The River Bank,708\r
            Mind Train,280\r
            """, text);
    }

    public void testCsvFormatWithCustomDelimiterRegularData() {
        Set<Character> forbidden = Set.of('"', '\r', '\n', '\t');
        Character delim = randomValueOtherThanMany(forbidden::contains, () -> randomAlphaOfLength(1).charAt(0));
        String text = format(CSV, reqWithParam("delimiter", String.valueOf(delim)), regularData());
        List<String> terms = Arrays.asList("string", "number", "Along The River Bank", "708", "Mind Train", "280");
        List<String> expectedTerms = terms.stream()
            .map(x -> x.contains(String.valueOf(delim)) ? '"' + x + '"' : x)
            .collect(Collectors.toList());
        StringBuilder sb = new StringBuilder();
        do {
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
            string\tnumber
            Along The River Bank\t708
            Mind Train\t280
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
            PLAIN_TEXT.format(
                req(),
                new BasicFormatter(emptyList(), emptyList(), TEXT),
                new SqlQueryResponse(StringUtils.EMPTY, Mode.JDBC, INTRODUCING_DATE_NANOS, false, null, emptyList())
            ).v1()
        );
    }

    private static SqlQueryResponse emptyData() {
        return new SqlQueryResponse(
            StringUtils.EMPTY,
            Mode.JDBC,
            INTRODUCING_DATE_NANOS,
            false,
            singletonList(new ColumnInfo("org/elasticsearch/xpack/sql/index", "name", "keyword")),
            emptyList()
        );
    }

    private static SqlQueryResponse regularData() {
        // headers
        List<ColumnInfo> headers = new ArrayList<>();
        headers.add(new ColumnInfo("org/elasticsearch/xpack/sql/index", "string", "keyword"));
        headers.add(new ColumnInfo("org/elasticsearch/xpack/sql/index", "number", "integer"));

        // values
        List<List<Object>> values = new ArrayList<>();
        values.add(asList("Along The River Bank", 11 * 60 + 48));
        values.add(asList("Mind Train", 4 * 60 + 40));

        return new SqlQueryResponse(null, Mode.JDBC, INTRODUCING_DATE_NANOS, false, headers, values);
    }

    private static SqlQueryResponse escapedData() {
        // headers
        List<ColumnInfo> headers = new ArrayList<>();
        headers.add(new ColumnInfo("org/elasticsearch/xpack/sql/index", "first", "keyword"));
        headers.add(new ColumnInfo("org/elasticsearch/xpack/sql/index", "\"special\"", "keyword"));

        // values
        List<List<Object>> values = new ArrayList<>();
        values.add(asList("normal", "\"quo\"ted\",\n"));
        values.add(asList("commas", "a,b,c,\n,d,e,\t\n"));

        return new SqlQueryResponse(null, Mode.JDBC, INTRODUCING_DATE_NANOS, false, headers, values);
    }

    private static RestRequest req() {
        return new FakeRestRequest();
    }

    private static RestRequest reqWithParam(String paramName, String paramVal) {
        return new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withParams(singletonMap(paramName, paramVal)).build();
    }

    private String format(TextFormat format, RestRequest request, SqlQueryResponse response) {
        return format.format(request, null, response).v1();
    }
}
