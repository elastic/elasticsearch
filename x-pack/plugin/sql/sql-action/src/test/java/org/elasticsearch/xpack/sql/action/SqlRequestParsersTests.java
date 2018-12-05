/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.action;

import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.sql.proto.SqlTypedParamValue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.hamcrest.Matchers.containsString;

public class SqlRequestParsersTests extends ESTestCase {

    public void testUnknownFieldParsingErrors() throws IOException {
        assertParsingErrorMessage("{\"key\" : \"value\"}", "unknown field [key]", SqlClearCursorRequest::fromXContent);
        assertParsingErrorMessage("{\"key\" : \"value\"}", "unknown field [key]", SqlQueryRequest::fromXContent);
        assertParsingErrorMessage("{\"key\" : \"value\"}", "unknown field [key]", SqlTranslateRequest::fromXContent);
    }
    
    public void testClearCursorRequestParser() throws IOException {
        assertParsingErrorMessage("{\"mode\" : \"jdbc\"}", "Required [cursor]", SqlClearCursorRequest::fromXContent);
        assertParsingErrorMessage("{\"cursor\" : \"whatever\", \"fetch_size\":123}", "unknown field [fetch_size]",
                SqlClearCursorRequest::fromXContent);
        
        SqlClearCursorRequest request = generateRequest("{\"cursor\" : \"whatever\", \"mode\" : \"jdbc\", \"client_id\" : \"bla\"}",
                SqlClearCursorRequest::fromXContent);
        assertNull(request.clientId());
        assertEquals("jdbc", request.mode().toString());
        assertEquals("whatever", request.getCursor());
        
        request = generateRequest("{\"cursor\" : \"whatever\", \"mode\" : \"some foo mode\", \"client_id\" : \"bla\"}",
                SqlClearCursorRequest::fromXContent);
        assertNull(request.clientId());
        assertEquals("plain", request.mode().toString());
        assertEquals("whatever", request.getCursor());
        
        request = generateRequest("{\"cursor\" : \"whatever\"}", SqlClearCursorRequest::fromXContent);
        assertNull(request.clientId());
        assertEquals("plain", request.mode().toString());
        assertEquals("whatever", request.getCursor());
        
        request = generateRequest("{\"cursor\" : \"whatever\", \"client_id\" : \"CLI\"}",
                SqlClearCursorRequest::fromXContent);
        assertEquals("cli", request.clientId());
        assertEquals("plain", request.mode().toString());
        assertEquals("whatever", request.getCursor());
        
        request = generateRequest("{\"cursor\" : \"whatever\", \"client_id\" : \"cANVAs\"}",
                SqlClearCursorRequest::fromXContent);
        assertEquals("canvas", request.clientId());
        assertEquals("plain", request.mode().toString());
        assertEquals("whatever", request.getCursor());
    }
    
    public void testTranslateRequestParser() throws IOException {
        assertParsingErrorMessage("{\"qquery\" : \"select * from bla\"}", "unknown field [qquery]", SqlTranslateRequest::fromXContent);

        SqlTranslateRequest request = generateRequest("{\"query\" : \"select * from foo\"}", SqlTranslateRequest::fromXContent);
        assertEquals("select * from foo", request.query());
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
        
        SqlQueryRequest request = generateRequest("{\"cursor\" : \"whatever\", \"mode\" : \"jdbc\", \"client_id\" : \"bla\","
                + "\"query\":\"select\",\"params\":[{\"value\":123, \"type\":\"whatever\"}], \"time_zone\":\"UTC\","
                + "\"request_timeout\":\"5s\",\"page_timeout\":\"10s\"}", SqlQueryRequest::fromXContent);
        assertNull(request.clientId());
        assertEquals("jdbc", request.mode().toString());
        assertEquals("whatever", request.cursor());
        assertEquals("select", request.query());
        
        List<SqlTypedParamValue> list = new ArrayList<SqlTypedParamValue>(1);
        list.add(new SqlTypedParamValue("whatever", 123));
        assertEquals(list, request.params());
        assertEquals("UTC", request.timeZone().getID());
        assertEquals(TimeValue.parseTimeValue("5s", "request_timeout"), request.requestTimeout());
        assertEquals(TimeValue.parseTimeValue("10s", "page_timeout"), request.pageTimeout());
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
    
    private XContentParser parser(String content) throws IOException {
        XContentType xContentType = XContentType.JSON;
        return xContentType.xContent().createParser(
                NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, content);
    }
}
