/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.formatter;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.IntArrayVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.esql.action.ColumnInfo;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;
import org.elasticsearch.xpack.ql.util.StringUtils;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.elasticsearch.xpack.esql.formatter.TextFormat.CSV;
import static org.elasticsearch.xpack.esql.formatter.TextFormat.PLAIN_TEXT;
import static org.elasticsearch.xpack.esql.formatter.TextFormat.TSV;

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
        assertEquals(StringUtils.EMPTY, PLAIN_TEXT.format(req(), new EsqlQueryResponse(emptyList(), emptyList(), false)));
    }

    private static EsqlQueryResponse emptyData() {
        return new EsqlQueryResponse(singletonList(new ColumnInfo("name", "keyword")), emptyList(), false);
    }

    private static EsqlQueryResponse regularData() {
        // headers
        List<ColumnInfo> headers = asList(new ColumnInfo("string", "keyword"), new ColumnInfo("number", "integer"));

        // values
        List<Page> values = List.of(
            new Page(
                BytesRefBlock.newBlockBuilder(2)
                    .appendBytesRef(new BytesRef("Along The River Bank"))
                    .appendBytesRef(new BytesRef("Mind Train"))
                    .build(),
                new IntArrayVector(new int[] { 11 * 60 + 48, 4 * 60 + 40 }, 2).asBlock()
            )
        );

        return new EsqlQueryResponse(headers, values, false);
    }

    private static EsqlQueryResponse escapedData() {
        // headers
        List<ColumnInfo> headers = asList(new ColumnInfo("first", "keyword"), new ColumnInfo("\"special\"", "keyword"));

        // values
        List<Page> values = List.of(
            new Page(
                BytesRefBlock.newBlockBuilder(2).appendBytesRef(new BytesRef("normal")).appendBytesRef(new BytesRef("commas")).build(),
                BytesRefBlock.newBlockBuilder(2)
                    .appendBytesRef(new BytesRef("\"quo\"ted\",\n"))
                    .appendBytesRef(new BytesRef("a,b,c,\n,d,e,\t\n"))
                    .build()
            )
        );

        return new EsqlQueryResponse(headers, values, false);
    }

    private static RestRequest req() {
        return new FakeRestRequest();
    }

    private static RestRequest reqWithParam(String paramName, String paramVal) {
        return new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withParams(singletonMap(paramName, paramVal)).build();
    }

    private String format(TextFormat format, RestRequest request, EsqlQueryResponse response) {
        return format.format(request, response);
    }
}
