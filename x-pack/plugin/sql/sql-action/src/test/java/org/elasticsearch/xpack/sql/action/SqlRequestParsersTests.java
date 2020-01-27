/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.action;

import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParseException;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.sql.proto.Mode;
import org.elasticsearch.xpack.sql.proto.SqlTypedParamValue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.hamcrest.CoreMatchers.containsString;

public class SqlRequestParsersTests extends ESTestCase {

    public void testUnknownFieldParsingErrors() throws IOException {
        assertParsingErrorMessage("{\"key\" : \"value\"}", "unknown field [key]", SqlClearCursorRequest::fromXContent);
        assertParsingErrorMessage("{\"key\" : \"value\"}", "unknown field [key]", SqlQueryRequest::fromXContent);
        assertParsingErrorMessage("{\"key\" : \"value\"}", "unknown field [key]", SqlTranslateRequest::fromXContent);
    }
    
    public void testUnknownModeFieldParsingErrors() throws IOException {
        assertParsingErrorMessageReason("{\"cursor\":\"foo\",\"mode\" : \"value\"}",
                "No enum constant org.elasticsearch.xpack.sql.proto.Mode.VALUE", SqlClearCursorRequest::fromXContent);
        assertParsingErrorMessageReason("{\"cursor\":\"foo\",\"mode\" : \"value\"}",
                "No enum constant org.elasticsearch.xpack.sql.proto.Mode.VALUE", SqlQueryRequest::fromXContent);
        assertParsingErrorMessageReason("{\"mode\" : \"value\"}",
                "No enum constant org.elasticsearch.xpack.sql.proto.Mode.VALUE", SqlTranslateRequest::fromXContent);
    }
    
    public void testClearCursorRequestParser() throws IOException {
        assertParsingErrorMessage("{\"mode\" : \"jdbc\"}", "Required [cursor]", SqlClearCursorRequest::fromXContent);
        assertParsingErrorMessage("{\"cursor\" : \"whatever\", \"fetch_size\":123}", "unknown field [fetch_size]",
                SqlClearCursorRequest::fromXContent);
        Mode randomMode = randomFrom(Mode.values());
        
        SqlClearCursorRequest request = generateRequest("{\"cursor\" : \"whatever\", \"mode\" : \""
                + randomMode.toString() + "\", \"client_id\" : \"bla\"}",
                SqlClearCursorRequest::fromXContent);
        assertNull(request.clientId());
        assertEquals(randomMode, request.mode());
        assertEquals("whatever", request.getCursor());
        
        randomMode = randomFrom(Mode.values());
        request = generateRequest("{\"cursor\" : \"whatever\", \"mode\" : \""
                + randomMode.toString() + "\", \"client_id\" : \"bla\"}",
                SqlClearCursorRequest::fromXContent);
        assertNull(request.clientId());
        assertEquals(randomMode, request.mode());
        assertEquals("whatever", request.getCursor());
        
        request = generateRequest("{\"cursor\" : \"whatever\"}", SqlClearCursorRequest::fromXContent);
        assertNull(request.clientId());
        assertEquals(Mode.PLAIN, request.mode());
        assertEquals("whatever", request.getCursor());
        
        request = generateRequest("{\"cursor\" : \"whatever\", \"client_id\" : \"CLI\"}",
                SqlClearCursorRequest::fromXContent);
        assertNull(request.clientId());
        assertEquals(Mode.PLAIN, request.mode());
        assertEquals("whatever", request.getCursor());
        
        request = generateRequest("{\"cursor\" : \"whatever\", \"client_id\" : \"cANVAs\"}",
                SqlClearCursorRequest::fromXContent);
        assertEquals("canvas", request.clientId());
        assertEquals(Mode.PLAIN, request.mode());
        assertEquals("whatever", request.getCursor());
    }
    
    public void testTranslateRequestParser() throws IOException {
        assertParsingErrorMessage("{\"qquery\" : \"select * from bla\"}", "unknown field [qquery]", SqlTranslateRequest::fromXContent);

        SqlTranslateRequest request = generateRequest("{\"query\" : \"select * from foo\"}", SqlTranslateRequest::fromXContent);
        assertEquals("select * from foo", request.query());
        assertEquals(Mode.PLAIN, request.mode());
        
        Mode randomMode = randomFrom(Mode.values());
        request = generateRequest("{\"query\" : \"whatever\", \"client_id\" : \"foo\", \"mode\":\""
                + randomMode.toString() + "\"}",
                SqlTranslateRequest::fromXContent);
        assertNull(request.clientId());
        assertEquals(randomMode, request.mode());
    }
    
    public void testQueryRequestParser() throws IOException {
        assertParsingErrorMessage("{\"mode\" : 123}", "mode doesn't support values of type: VALUE_NUMBER", SqlQueryRequest::fromXContent);
        assertParsingErrorMessage("{\"cursor\" : \"whatever\", \"fetch_size\":\"abc\"}", "failed to parse field [fetch_size]",
                SqlQueryRequest::fromXContent);
        assertParsingErrorMessage("{\"client_id\":123}", "client_id doesn't support values of type: VALUE_NUMBER",
                SqlQueryRequest::fromXContent);
        assertParsingErrorMessage("{\"params\":[{\"value\":123}]}", "failed to parse field [params]", SqlQueryRequest::fromXContent);
        assertParsingErrorMessage("{\"time_zone\":12}", "time_zone doesn't support values of type: VALUE_NUMBER",
                SqlQueryRequest::fromXContent);
        
        Mode randomMode = randomFrom(Mode.values());
        String params;
        List<SqlTypedParamValue> list = new ArrayList<>(1);
        
        if (Mode.isDriver(randomMode)) {
            params = "{\"value\":123, \"type\":\"whatever\"}";
            list.add(new SqlTypedParamValue("whatever", 123, true));
        } else {
            params = "123";
            list.add(new SqlTypedParamValue("integer", 123, false));
        }
        
        SqlQueryRequest request = generateRequest("{\"cursor\" : \"whatever\", \"mode\" : \""
                + randomMode.toString() + "\", \"client_id\" : \"bla\","
                + "\"query\":\"select\","
                + "\"params\":[" + params + "],"
                + " \"time_zone\":\"UTC\","
                + "\"request_timeout\":\"5s\",\"page_timeout\":\"10s\"}", SqlQueryRequest::fromXContent);
        assertNull(request.clientId());
        assertEquals(randomMode, request.mode());
        assertEquals("whatever", request.cursor());
        assertEquals("select", request.query());

        assertEquals(list, request.params());
        assertEquals("UTC", request.zoneId().getId());
        assertEquals(TimeValue.parseTimeValue("5s", "request_timeout"), request.requestTimeout());
        assertEquals(TimeValue.parseTimeValue("10s", "page_timeout"), request.pageTimeout());
    }
    
    public void testParamsSuccessfulParsingInDriverMode() throws IOException {
        Mode driverMode = randomValueOtherThanMany((m) -> Mode.isDriver(m) == false, () -> randomFrom(Mode.values()));
        String json = "{" + 
                      "    \"params\":[{\"type\":\"integer\",\"value\":35000},"
                      + "              {\"type\":\"date\",\"value\":\"1960-01-01\"},"
                      + "              {\"type\":\"boolean\",\"value\":false},"
                      + "              {\"type\":\"keyword\",\"value\":\"foo\"}]," + 
                      "    \"mode\": \"" + driverMode.toString() + "\"" + 
                      "}";
        SqlQueryRequest request = generateRequest(json, SqlQueryRequest::fromXContent);
        List<SqlTypedParamValue> params = request.params();
        assertEquals(4, params.size());
        
        assertEquals(35000, params.get(0).value);
        assertEquals("integer", params.get(0).type);
        assertTrue(params.get(0).hasExplicitType());
        
        assertEquals("1960-01-01", params.get(1).value);
        assertEquals("date", params.get(1).type);
        assertTrue(params.get(1).hasExplicitType());
        
        assertEquals(false, params.get(2).value);
        assertEquals("boolean", params.get(2).type);
        assertTrue(params.get(2).hasExplicitType());
        
        assertEquals("foo", params.get(3).value);
        assertEquals("keyword", params.get(3).type);
        assertTrue(params.get(3).hasExplicitType());
    }
    
    public void testParamsSuccessfulParsingInNonDriverMode() throws IOException {
        Mode nonDriverMode = randomValueOtherThanMany(Mode::isDriver, () -> randomFrom(Mode.values()));
        String json = "{" + 
                      "    \"params\":[35000,\"1960-01-01\",false,\"foo\"]," + 
                      "    \"mode\": \"" + nonDriverMode.toString() + "\"" + 
                      "}";
        SqlQueryRequest request = generateRequest(json, SqlQueryRequest::fromXContent);
        List<SqlTypedParamValue> params = request.params();
        assertEquals(4, params.size());
        
        assertEquals(35000, params.get(0).value);
        assertEquals("integer", params.get(0).type);
        assertFalse(params.get(0).hasExplicitType());
        
        assertEquals("1960-01-01", params.get(1).value);
        assertEquals("keyword", params.get(1).type);
        assertFalse(params.get(1).hasExplicitType());
        
        assertEquals(false, params.get(2).value);
        assertEquals("boolean", params.get(2).type);
        assertFalse(params.get(2).hasExplicitType());
        
        assertEquals("foo", params.get(3).value);
        assertEquals("keyword", params.get(3).type);
        assertFalse(params.get(3).hasExplicitType());
    }
    
    public void testParamsParsingFailure_QueryRequest_NonDriver() throws IOException {
        Mode m = randomValueOtherThanMany(Mode::isDriver, () -> randomFrom(Mode.values()));
        assertXContentParsingErrorMessage("{\"params\":[{\"whatever\":35000},\"1960-01-01\",false,\"foo\"],\"mode\": \""
                + m.toString() + "\"}",
                "[sql/query] failed to parse field [params]",
                SqlQueryRequest::fromXContent);
        assertXContentParsingErrorMessage("{\"params\":[350.123,\"1960-01-01\",{\"foobar\":false},\"foo\"],\"mode\": \"}"
                + m.toString() + "\"}",
                "[sql/query] failed to parse field [params]",
                SqlQueryRequest::fromXContent);
        assertXContentParsingErrorMessage("{\"mode\": \"" + m.toString() + "\",\"params\":[350.123,\"1960-01-01\",false,"
                + "{\"type\":\"keyword\",\"value\":\"foo\"}]}",
                "[params] must be an array where each entry is a single field (no objects supported)",
                SqlQueryRequest::fromXContent);
    }
    
    public void testParamsParsingFailure_TranslateRequest_NonDriver() throws IOException {
        Mode m = randomValueOtherThanMany(Mode::isDriver, () -> randomFrom(Mode.values()));
        assertXContentParsingErrorMessage("{\"params\":[{\"whatever\":35000},\"1960-01-01\",false,\"foo\"],\"mode\": \""
                + m.toString() + "\"}",
                "[sql/query] failed to parse field [params]",
                SqlTranslateRequest::fromXContent);
        assertXContentParsingErrorMessage("{\"params\":[350.123,\"1960-01-01\",{\"foobar\":false},\"foo\"],\"mode\": \"}"
                + m.toString() + "\"}",
                "[sql/query] failed to parse field [params]",
                SqlTranslateRequest::fromXContent);
        assertXContentParsingErrorMessage("{\"mode\": \"" + m.toString() + "\",\"params\":[350.123,\"1960-01-01\",false,"
                + "{\"type\":\"keyword\",\"value\":\"foo\"}]}",
                "[params] must be an array where each entry is a single field (no objects supported)",
                SqlTranslateRequest::fromXContent);
    }
    
    public void testParamsParsingFailure_Driver() throws IOException {
        Mode m = randomValueOtherThanMany((t) -> Mode.isDriver(t) == false, () -> randomFrom(Mode.values()));
        assertXContentParsingErrorMessage("{\"params\":[35000,{\"value\":\"1960-01-01\",\"type\":\"date\"},{\"value\":\"foo\","
                + "\"type\":\"keyword\"}],\"mode\": \"" + m.toString() + "\"}",
                "[params] must be an array where each entry is an object with a value/type pair",
                SqlQueryRequest::fromXContent);
        assertXContentParsingErrorMessage("{\"params\":[{\"value\":10,\"type\":\"integer\"},{\"value\":\"1960-01-01\",\"type\":\"date\"},"
                + "false,\"foo\"],\"mode\": \"" + m.toString() + "\"}",
                "[params] must be an array where each entry is an object with a value/type pair",
                SqlQueryRequest::fromXContent);
        assertXContentParsingErrorMessage("{\"mode\": \"" + m.toString() + "\",\"params\":[{\"value\":10,\"type\":\"integer\"},"
                + "{\"value\":\"1960-01-01\",\"type\":\"date\"},{\"foo\":\"bar\"}]}",
                "[sql/query] failed to parse field [params]",
                SqlQueryRequest::fromXContent);
    }
    
    private <R extends AbstractSqlRequest> R generateRequest(String json, Function<XContentParser, R> fromXContent)
            throws IOException {
        XContentParser parser = parser(json);
        return fromXContent.apply(parser);
    }
    
    private void assertParsingErrorMessage(String json, String errorMessage, Consumer<XContentParser> consumer) throws IOException {
        XContentParser parser = parser(json);
        Exception e = expectThrows(IllegalArgumentException.class, () -> consumer.accept(parser));
        assertThat(e.getMessage(), containsString(errorMessage));
    }
    
    private void assertParsingErrorMessageReason(String json, String errorMessage, Consumer<XContentParser> consumer) throws IOException {
        XContentParser parser = parser(json);
        Exception e = expectThrows(IllegalArgumentException.class, () -> consumer.accept(parser));
        assertThat(e.getCause().getMessage(), containsString(errorMessage));
    }
    
    private void assertXContentParsingErrorMessage(String json, String errorMessage, Consumer<XContentParser> consumer) throws IOException {
        XContentParser parser = parser(json);
        Exception e = expectThrows(XContentParseException.class, () -> consumer.accept(parser));
        assertThat(e.getMessage(), containsString(errorMessage));
    }
    
    private XContentParser parser(String content) throws IOException {
        XContentType xContentType = XContentType.JSON;
        return xContentType.xContent().createParser(
                NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, content);
    }
}
