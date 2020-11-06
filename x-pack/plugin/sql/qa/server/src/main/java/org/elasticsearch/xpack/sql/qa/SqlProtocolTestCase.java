/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.qa;

import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.cbor.CborXContent;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.common.xcontent.smile.SmileXContent;
import org.elasticsearch.common.xcontent.yaml.YamlXContent;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.sql.proto.Mode;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.xpack.sql.proto.Mode.CLI;
import static org.elasticsearch.xpack.sql.proto.Protocol.SQL_QUERY_REST_ENDPOINT;
import static org.elasticsearch.xpack.sql.proto.RequestInfo.CLIENT_IDS;
import static org.elasticsearch.xpack.sql.qa.rest.RestSqlTestCase.query;

public abstract class SqlProtocolTestCase extends ESRestTestCase {

    public void testNulls() throws IOException {
        assertQuery("SELECT NULL", "NULL", "null", null, 0);
    }

    public void testBooleanType() throws IOException {
        assertQuery("SELECT TRUE", "TRUE", "boolean", true, 1);
        assertQuery("SELECT FALSE", "FALSE", "boolean", false, 1);
    }

    public void testNumericTypes() throws IOException {
        assertQuery("SELECT CAST(3 AS TINYINT)", "CAST(3 AS TINYINT)", "byte", 3, 5);
        assertQuery("SELECT CAST(-123 AS TINYINT)", "CAST(-123 AS TINYINT)", "byte", -123, 5);
        assertQuery("SELECT CAST(5 AS SMALLINT)", "CAST(5 AS SMALLINT)", "short", 5, 6);
        assertQuery("SELECT CAST(-25 AS SMALLINT)", "CAST(-25 AS SMALLINT)", "short", -25, 6);
        assertQuery("SELECT 123", "123", "integer", 123, 11);
        assertQuery("SELECT -2123", "-2123", "integer", -2123, 11);
        assertQuery("SELECT 1234567890123", "1234567890123", "long", 1234567890123L, 20);
        assertQuery("SELECT -1234567890123", "-1234567890123", "long", -1234567890123L, 20);
        assertQuery("SELECT 1234567890123.34", "1234567890123.34", "double", 1234567890123.34, 25);
        assertQuery("SELECT -1234567890123.34", "-1234567890123.34", "double", -1234567890123.34, 25);
        assertQuery("SELECT CAST(1234.34 AS REAL)", "CAST(1234.34 AS REAL)", "float", 1234.34f, 15);
        assertQuery("SELECT CAST(-1234.34 AS REAL)", "CAST(-1234.34 AS REAL)", "float", -1234.34f, 15);
        assertQuery("SELECT CAST(1234567890123.34 AS FLOAT)", "CAST(1234567890123.34 AS FLOAT)", "double", 1234567890123.34, 25);
        assertQuery("SELECT CAST(-1234567890123.34 AS FLOAT)", "CAST(-1234567890123.34 AS FLOAT)", "double", -1234567890123.34, 25);
    }

    public void testTextualType() throws IOException {
        assertQuery("SELECT 'abc123'", "'abc123'", "keyword", "abc123", 32766);
    }

    public void testDateTimes() throws IOException {
        assertQuery(
            "SELECT CAST('2019-01-14T12:29:25.000Z' AS DATETIME)",
            "CAST('2019-01-14T12:29:25.000Z' AS DATETIME)",
            "datetime",
            "2019-01-14T12:29:25.000Z",
            29
        );
        assertQuery(
            "SELECT CAST(-26853765751000 AS DATETIME)",
            "CAST(-26853765751000 AS DATETIME)",
            "datetime",
            "1119-01-15T12:37:29.000Z",
            29
        );
        assertQuery(
            "SELECT CAST(CAST('-26853765751000' AS BIGINT) AS DATETIME)",
            "CAST(CAST('-26853765751000' AS BIGINT) AS DATETIME)",
            "datetime",
            "1119-01-15T12:37:29.000Z",
            29
        );

        assertQuery("SELECT CAST('2019-01-14' AS DATE)", "CAST('2019-01-14' AS DATE)", "date", "2019-01-14T00:00:00.000Z", 29);
        assertQuery("SELECT CAST(-26853765751000 AS DATE)", "CAST(-26853765751000 AS DATE)", "date", "1119-01-15T00:00:00.000Z", 29);

        assertQuery("SELECT CAST('12:29:25.123Z' AS TIME)", "CAST('12:29:25.123Z' AS TIME)", "time", "12:29:25.123Z", 18);
        assertQuery(
            "SELECT CAST('12:29:25.123456789+05:00' AS TIME)",
            "CAST('12:29:25.123456789+05:00' AS TIME)",
            "time",
            "12:29:25.123+05:00",
            18
        );
        assertQuery("SELECT CAST(-26853765751000 AS TIME)", "CAST(-26853765751000 AS TIME)", "time", "12:37:29.000Z", 18);
    }

    public void testIPs() throws IOException {
        assertQuery("SELECT CAST('12.13.14.15' AS IP)", "CAST('12.13.14.15' AS IP)", "ip", "12.13.14.15", 45);
        assertQuery(
            "SELECT CAST('2001:0db8:0000:0000:0000:ff00:0042:8329' AS IP)",
            "CAST('2001:0db8:0000:0000:0000:ff00:0042:8329' AS IP)",
            "ip",
            "2001:0db8:0000:0000:0000:ff00:0042:8329",
            45
        );
    }

    public void testDateTimeIntervals() throws IOException {
        assertQuery("SELECT INTERVAL '326' YEAR", "INTERVAL '326' YEAR", "interval_year", "P326Y", "+326-0", 7);
        assertQuery("SELECT INTERVAL '50' MONTH", "INTERVAL '50' MONTH", "interval_month", "P50M", "+0-50", 7);
        assertQuery("SELECT INTERVAL '520' DAY", "INTERVAL '520' DAY", "interval_day", "PT12480H", "+520 00:00:00", 23);
        assertQuery("SELECT INTERVAL '163' HOUR", "INTERVAL '163' HOUR", "interval_hour", "PT163H", "+6 19:00:00", 23);
        assertQuery("SELECT INTERVAL '163' MINUTE", "INTERVAL '163' MINUTE", "interval_minute", "PT2H43M", "+0 02:43:00", 23);
        assertQuery("SELECT INTERVAL '223.16' SECOND", "INTERVAL '223.16' SECOND", "interval_second", "PT3M43.16S", "+0 00:03:43.16", 23);
        assertQuery(
            "SELECT INTERVAL '163-11' YEAR TO MONTH",
            "INTERVAL '163-11' YEAR TO MONTH",
            "interval_year_to_month",
            "P163Y11M",
            "+163-11",
            7
        );
        assertQuery(
            "SELECT INTERVAL '163 12' DAY TO HOUR",
            "INTERVAL '163 12' DAY TO HOUR",
            "interval_day_to_hour",
            "PT3924H",
            "+163 12:00:00",
            23
        );
        assertQuery(
            "SELECT INTERVAL '163 12:39' DAY TO MINUTE",
            "INTERVAL '163 12:39' DAY TO MINUTE",
            "interval_day_to_minute",
            "PT3924H39M",
            "+163 12:39:00",
            23
        );
        assertQuery(
            "SELECT INTERVAL '163 12:39:59.163' DAY TO SECOND",
            "INTERVAL '163 12:39:59.163' DAY TO SECOND",
            "interval_day_to_second",
            "PT3924H39M59.163S",
            "+163 12:39:59.163",
            23
        );
        assertQuery(
            "SELECT INTERVAL -'163 23:39:56.23' DAY TO SECOND",
            "INTERVAL -'163 23:39:56.23' DAY TO SECOND",
            "interval_day_to_second",
            "PT-3935H-39M-56.23S",
            "-163 23:39:56.23",
            23
        );
        assertQuery(
            "SELECT INTERVAL '163:39' HOUR TO MINUTE",
            "INTERVAL '163:39' HOUR TO MINUTE",
            "interval_hour_to_minute",
            "PT163H39M",
            "+6 19:39:00",
            23
        );
        assertQuery(
            "SELECT INTERVAL '163:39:59.163' HOUR TO SECOND",
            "INTERVAL '163:39:59.163' HOUR TO SECOND",
            "interval_hour_to_second",
            "PT163H39M59.163S",
            "+6 19:39:59.163",
            23
        );
        assertQuery(
            "SELECT INTERVAL '163:59.163' MINUTE TO SECOND",
            "INTERVAL '163:59.163' MINUTE TO SECOND",
            "interval_minute_to_second",
            "PT2H43M59.163S",
            "+0 02:43:59.163",
            23
        );
    }

    /**
     * Method that tests that a binary response (CBOR) will return either Float or Double, depending on the SQL data type, for floating
     * point numbers, while JSON will always return Double for floating point numbers.
     */
    public void testFloatingPointNumbersReturnTypes() throws IOException {
        Request request = new Request("POST", SQL_QUERY_REST_ENDPOINT);
        for (Mode mode : Mode.values()) {
            assertFloatingPointNumbersReturnTypes(request, mode);
        }
    }

    @SuppressWarnings({ "unchecked" })
    private void assertFloatingPointNumbersReturnTypes(Request request, Mode mode) throws IOException {
        String requestContent = query(
            "SELECT "
                + "CAST(1234.34 AS REAL) AS float_positive,"
                + "CAST(-1234.34 AS REAL) AS float_negative,"
                + "1234567890123.34 AS double_positive,"
                + "-1234567890123.34 AS double_negative"
        ).mode(mode).toString();
        request.setEntity(new StringEntity(requestContent, ContentType.APPLICATION_JSON));

        Map<String, Object> map;
        boolean isBinaryResponse = mode != Mode.PLAIN;
        Response response = client().performRequest(request);
        if (isBinaryResponse) {
            map = XContentHelper.convertToMap(CborXContent.cborXContent, response.getEntity().getContent(), false);
        } else {
            map = XContentHelper.convertToMap(JsonXContent.jsonXContent, response.getEntity().getContent(), false);
        }

        List<Object> columns = (ArrayList<Object>) map.get("columns");
        assertEquals(4, columns.size());
        List<Object> rows = (ArrayList<Object>) map.get("rows");
        assertEquals(1, rows.size());
        List<Object> row = (ArrayList<Object>) rows.get(0);
        assertEquals(4, row.size());

        if (isBinaryResponse) {
            assertTrue(row.get(0) instanceof Float);
            assertEquals(row.get(0), 1234.34f);
            assertTrue(row.get(1) instanceof Float);
            assertEquals(row.get(1), -1234.34f);
        } else {
            assertTrue(row.get(0) instanceof Double);
            assertEquals(row.get(0), 1234.34d);
            assertTrue(row.get(1) instanceof Double);
            assertEquals(row.get(1), -1234.34d);
        }
        assertTrue(row.get(2) instanceof Double);
        assertEquals(row.get(2), 1234567890123.34d);
        assertTrue(row.get(3) instanceof Double);
        assertEquals(row.get(3), -1234567890123.34d);
    }

    private void assertQuery(String sql, String columnName, String columnType, Object columnValue, int displaySize) throws IOException {
        assertQuery(sql, columnName, columnType, columnValue, null, displaySize);
    }

    private void assertQuery(String sql, String columnName, String columnType, Object columnValue, Object cliColumnValue, int displaySize)
        throws IOException {
        for (Mode mode : Mode.values()) {
            boolean isCliCheck = mode == CLI && cliColumnValue != null;
            assertQuery(sql, columnName, columnType, isCliCheck ? cliColumnValue : columnValue, displaySize, mode);
        }
    }

    @SuppressWarnings({ "unchecked" })
    private void assertQuery(String sql, String columnName, String columnType, Object columnValue, int displaySize, Mode mode)
        throws IOException {
        boolean columnar = randomBoolean();
        Map<String, Object> response = runSql(mode, sql, columnar);
        List<Object> columns = (ArrayList<Object>) response.get("columns");
        assertEquals(1, columns.size());

        Map<String, Object> column = (HashMap<String, Object>) columns.get(0);
        assertEquals(columnName, column.get("name"));
        assertEquals(columnType, column.get("type"));
        if (Mode.isDriver(mode)) {
            assertEquals(3, column.size());
            assertEquals(displaySize, column.get("display_size"));
        } else {
            assertEquals(2, column.size());
        }

        List<Object> rows = (ArrayList<Object>) response.get(columnar ? "values" : "rows");
        assertEquals(1, rows.size());
        List<Object> row = (ArrayList<Object>) rows.get(0);
        assertEquals(1, row.size());

        // from xcontent we can get float or double, depending on the conversion
        // method of the specific xcontent format implementation
        if (columnValue instanceof Float && row.get(0) instanceof Double) {
            assertEquals(columnValue, (float) ((Number) row.get(0)).doubleValue());
        } else {
            assertEquals(columnValue, row.get(0));
        }
    }

    private Map<String, Object> runSql(Mode mode, String sql, boolean columnar) throws IOException {
        Request request = new Request("POST", SQL_QUERY_REST_ENDPOINT);
        String requestContent = query(sql).mode(mode).toString();
        String format = randomFrom(XContentType.values()).name().toLowerCase(Locale.ROOT);

        // add a client_id to the request
        if (randomBoolean()) {
            String clientId = randomFrom(randomFrom(CLIENT_IDS), randomAlphaOfLengthBetween(10, 20));
            requestContent = new StringBuilder(requestContent).insert(requestContent.length() - 1, ",\"client_id\":\"" + clientId + "\"")
                .toString();
        }
        if (randomBoolean()) {
            request.addParameter("error_trace", "true");
        }
        if (randomBoolean()) {
            request.addParameter("pretty", "true");
        }
        if (!"json".equals(format) || randomBoolean()) {
            // since we default to JSON if a format is not specified, randomize setting it or not, explicitly;
            // for any other format, just set the format explicitly
            request.addParameter("format", format);
        }
        if (randomBoolean()) {
            // randomly use the Accept header for the response format
            RequestOptions.Builder options = request.getOptions().toBuilder();
            options.addHeader("Accept", randomFrom("*/*", "application/" + format));
            request.setOptions(options);
        }
        if ((false == columnar && randomBoolean()) || columnar) {
            // randomly set the "columnar" parameter, either "true" (non-default) or explicit "false" (the default anyway)
            requestContent = new StringBuilder(requestContent).insert(requestContent.length() - 1, ",\"columnar\":" + columnar).toString();
        }

        // randomize binary response enforcement for drivers (ODBC/JDBC) and CLI
        boolean binaryCommunication = randomBoolean();
        if (randomBoolean()) {
            // set it explicitly or leave the default (null) as is
            requestContent = new StringBuilder(requestContent).insert(
                requestContent.length() - 1,
                ",\"binary_format\":" + binaryCommunication
            ).toString();
            binaryCommunication = Mode.isDedicatedClient(mode) && binaryCommunication;
        } else {
            binaryCommunication = Mode.isDedicatedClient(mode);
        }

        // send the query either as body or as request parameter
        if (randomBoolean()) {
            request.setEntity(new StringEntity(requestContent, ContentType.APPLICATION_JSON));
        } else {
            request.setEntity(null);
            request.addParameter("source", requestContent);
            request.addParameter("source_content_type", ContentType.APPLICATION_JSON.getMimeType());
            RequestOptions.Builder options = request.getOptions().toBuilder();
            options.addHeader("Content-Type", "application/json");
            request.setOptions(options);
        }

        Response response = client().performRequest(request);
        try (InputStream content = response.getEntity().getContent()) {
            if (binaryCommunication) {
                return XContentHelper.convertToMap(CborXContent.cborXContent, content, false);
            }
            switch (format) {
                case "cbor": {
                    return XContentHelper.convertToMap(CborXContent.cborXContent, content, false);
                }
                case "yaml": {
                    return XContentHelper.convertToMap(YamlXContent.yamlXContent, content, false);
                }
                case "smile": {
                    return XContentHelper.convertToMap(SmileXContent.smileXContent, content, false);
                }
                default:
                    return XContentHelper.convertToMap(JsonXContent.jsonXContent, content, false);
            }
        }
    }
}
