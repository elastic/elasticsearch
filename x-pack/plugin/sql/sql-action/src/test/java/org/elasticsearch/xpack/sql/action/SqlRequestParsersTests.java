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

import static org.hamcrest.Matchers.containsString;

public class SqlRequestParsersTests extends ESTestCase {

    public void testUnknownFieldParsingErrors() throws IOException {
        XContentParser parser = parser("{\"key\" : \"value\"}");
        Exception e = expectThrows(IllegalArgumentException.class, () -> SqlClearCursorRequest.fromXContent(parser));
        assertThat(e.getMessage(), containsString("unknown field [key]"));
        
        XContentParser parser2 = parser("{\"key\" : \"value\"}");
        e = expectThrows(IllegalArgumentException.class, () -> SqlQueryRequest.fromXContent(parser2));
        assertThat(e.getMessage(), containsString("unknown field [key]"));
        
        XContentParser parser3 = parser("{\"key\" : \"value\"}");
        e = expectThrows(IllegalArgumentException.class, () -> SqlTranslateRequest.fromXContent(parser3));
        assertThat(e.getMessage(), containsString("unknown field [key]"));
    }
    
    public void testClearCursorRequestParser() throws IOException {
        XContentParser parser = parser("{\"mode\" : \"jdbc\"}");
        Exception e = expectThrows(IllegalArgumentException.class, () -> SqlClearCursorRequest.fromXContent(parser));
        assertThat(e.getMessage(), containsString("Required [cursor]"));
        
        XContentParser parser2 = parser("{\"cursor\" : \"whatever\", \"fetch_size\":123}");
        e = expectThrows(IllegalArgumentException.class, () -> SqlClearCursorRequest.fromXContent(parser2));
        assertThat(e.getMessage(), containsString("unknown field [fetch_size]"));
        
        XContentParser parser3 = parser("{\"cursor\" : \"whatever\", \"mode\" : \"jdbc\", \"client.id\" : \"bla\"}");
        SqlClearCursorRequest request = SqlClearCursorRequest.fromXContent(parser3);
        assertEquals("bla", request.clientId());
        assertEquals("jdbc", request.mode().toString());
        assertEquals("whatever", request.getCursor());
        
        XContentParser parser4 = parser("{\"cursor\" : \"whatever\", \"mode\" : \"some foo mode\", \"client.id\" : \"bla\"}");
        request = SqlClearCursorRequest.fromXContent(parser4);
        assertEquals("bla", request.clientId());
        assertEquals("plain", request.mode().toString());
        assertEquals("whatever", request.getCursor());
    }
    
    public void testTranslateRequestParser() throws IOException {
        XContentParser parser = parser("{\"qquery\" : \"select * from bla\"}");
        Exception e = expectThrows(IllegalArgumentException.class, () -> SqlTranslateRequest.fromXContent(parser));
        assertThat(e.getMessage(), containsString("unknown field [qquery]"));

        XContentParser parser3 = parser("{\"query\" : \"select * from foo\"}");
        SqlTranslateRequest request = SqlTranslateRequest.fromXContent(parser3);
        assertEquals("select * from foo", request.query());
    }
    
    public void testQueryRequestParser() throws IOException {
        XContentParser parser = parser("{\"mode\" : 123}");
        Exception e = expectThrows(IllegalArgumentException.class, () -> SqlQueryRequest.fromXContent(parser));
        assertThat(e.getMessage(), containsString("mode doesn't support values of type: VALUE_NUMBER"));
        
        XContentParser parser2 = parser("{\"cursor\" : \"whatever\", \"fetch_size\":\"abc\"}");
        e = expectThrows(IllegalArgumentException.class, () -> SqlQueryRequest.fromXContent(parser2));
        assertThat(e.getMessage(), containsString("failed to parse field [fetch_size]"));
        
        XContentParser parser3 = parser("{\"client.id\":123}");
        e = expectThrows(IllegalArgumentException.class, () -> SqlQueryRequest.fromXContent(parser3));
        assertThat(e.getMessage(), containsString("client.id doesn't support values of type: VALUE_NUMBER"));
        
        XContentParser parser4 = parser("{\"params\":[{\"value\":123}]}");
        e = expectThrows(IllegalArgumentException.class, () -> SqlQueryRequest.fromXContent(parser4));
        assertThat(e.getMessage(), containsString("failed to parse field [params]"));
        
        XContentParser parser5 = parser("{\"time_zone\":12}");
        e = expectThrows(IllegalArgumentException.class, () -> SqlQueryRequest.fromXContent(parser5));
        assertThat(e.getMessage(), containsString("time_zone doesn't support values of type: VALUE_NUMBER"));
        
        XContentParser parser6 = parser("{\"cursor\" : \"whatever\", \"mode\" : \"jdbc\", \"client.id\" : \"bla\", \"query\":\"select\","
                + "\"params\":[{\"value\":123, \"type\":\"whatever\"}], \"time_zone\":\"UTC\", \"request_timeout\":\"5s\","
                + "\"page_timeout\":\"10s\"}");
        SqlQueryRequest request = SqlQueryRequest.fromXContent(parser6);
        assertEquals("bla", request.clientId());
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
    
    private XContentParser parser(String content) throws IOException {
        XContentType xContentType = XContentType.JSON;
        return xContentType.xContent().createParser(
                NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, content);
    }
}
