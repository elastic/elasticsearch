/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.exporter.http;

import org.apache.http.HttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContent;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.mock.orig.Mockito;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests {@link HttpExportBulkResponseListener}.
 */
public class HttpExportBulkResponseListenerTests extends ESTestCase {

    public void testOnSuccess() throws IOException {
        final Response response = mock(Response.class);
        final StringEntity entity = new StringEntity("{\"took\":5,\"errors\":false}", ContentType.APPLICATION_JSON);

        when(response.getEntity()).thenReturn(entity);

        // doesn't explode
        new WarningsHttpExporterBulkResponseListener().onSuccess(response);
    }

    public void testOnSuccessParsing() throws IOException {
        // {"took": 4, "errors": false, ...
        final Response response = mock(Response.class);
        final XContent xContent = mock(XContent.class);
        final XContentParser parser = mock(XContentParser.class);
        final HttpEntity entity = mock(HttpEntity.class);
        final InputStream stream = mock(InputStream.class);

        when(response.getEntity()).thenReturn(entity);
        when(entity.getContent()).thenReturn(stream);
        when(xContent.createParser(Mockito.any(NamedXContentRegistry.class),
                Mockito.any(DeprecationHandler.class), Mockito.eq(stream))).thenReturn(parser);

        // {, "took", 4, "errors", false
        when(parser.nextToken()).thenReturn(Token.START_OBJECT,
                                            Token.FIELD_NAME, Token.VALUE_NUMBER,
                                            Token.FIELD_NAME, Token.VALUE_BOOLEAN);
        when(parser.currentName()).thenReturn("took", "errors");
        when(parser.booleanValue()).thenReturn(false);

        new HttpExportBulkResponseListener(xContent).onSuccess(response);

        verify(parser, times(5)).nextToken();
        verify(parser, times(2)).currentName();
        verify(parser).booleanValue();
    }

    public void testOnSuccessWithInnerErrors() {
        final String[] expectedErrors = new String[] { randomAlphaOfLengthBetween(4, 10), randomAlphaOfLengthBetween(5, 9) };
        final AtomicInteger counter = new AtomicInteger(0);
        final Response response = mock(Response.class);
        final StringEntity entity = new StringEntity(
                "{\"took\":4,\"errors\":true,\"items\":[" +
                        "{\"index\":{\"_index\":\".monitoring-data-2\",\"_type\":\"node\",\"_id\":\"123\"}}," +
                        "{\"index\":{\"_index\":\".monitoring-data-2\",\"_type\":\"node\",\"_id\":\"456\"," +
                            "\"error\":\"" + expectedErrors[0] + "\"}}," +
                        "{\"index\":{\"_index\":\".monitoring-data-2\",\"_type\":\"node\",\"_id\":\"789\"}}," +
                        "{\"index\":{\"_index\":\".monitoring-data-2\",\"_type\":\"node\",\"_id\":\"012\"," +
                            "\"error\":\"" + expectedErrors[1] + "\"}}" +
                "]}",
                ContentType.APPLICATION_JSON);

        when(response.getEntity()).thenReturn(entity);

        // doesn't explode
        new WarningsHttpExporterBulkResponseListener() {
            @Override
            void onItemError(final String text) {
                assertEquals(expectedErrors[counter.getAndIncrement()], text);
            }
        }.onSuccess(response);

        assertEquals(expectedErrors.length, counter.get());
    }

    public void testOnSuccessParsingWithInnerErrors() throws IOException {
        // {"took": 4, "errors": true, "items": [ { "index": { "_index": "ignored", "_type": "ignored", "_id": "ignored" },
        //                                        { "index": { "_index": "ignored", "_type": "ignored", "_id": "ignored", "error": "blah" }
        //                                      ]...
        final Response response = mock(Response.class);
        final XContent xContent = mock(XContent.class);
        final XContentParser parser = mock(XContentParser.class);
        final HttpEntity entity = mock(HttpEntity.class);
        final InputStream stream = mock(InputStream.class);

        when(response.getEntity()).thenReturn(entity);
        when(entity.getContent()).thenReturn(stream);
        when(xContent.createParser(Mockito.any(NamedXContentRegistry.class),
                Mockito.any(DeprecationHandler.class), Mockito.eq(stream))).thenReturn(parser);

        // tag::disable-formatting
        // {, "took", 4, "errors", false
        when(parser.nextToken()).thenReturn(               // nextToken, currentName
            Token.START_OBJECT,                            // 1
            Token.FIELD_NAME, Token.VALUE_NUMBER,          // 3, 1
            Token.FIELD_NAME, Token.VALUE_BOOLEAN,         // 5, 2
            Token.FIELD_NAME, Token.START_ARRAY,           // 7, 3
                // no error:
                Token.START_OBJECT,                        // 8
                    Token.FIELD_NAME, Token.START_OBJECT,  // 10, 4
                    Token.FIELD_NAME, Token.VALUE_STRING,  // 12, 5
                    Token.FIELD_NAME, Token.VALUE_STRING,  // 14, 6
                    Token.FIELD_NAME, Token.VALUE_STRING,  // 16, 7
                Token.END_OBJECT,                          // 17
                Token.START_OBJECT,                        // 18
                    Token.FIELD_NAME, Token.START_OBJECT,  // 20, 8
                    Token.FIELD_NAME, Token.VALUE_STRING,  // 22, 9
                    Token.FIELD_NAME, Token.VALUE_STRING,  // 24, 10
                    Token.FIELD_NAME, Token.VALUE_STRING,  // 26, 11
                    Token.FIELD_NAME, Token.VALUE_STRING,  // 28, 12 ("error")
                Token.END_OBJECT,                          // 29
            Token.END_ARRAY);                              // 30
        // end::disable-formatting
        when(parser.currentName()).thenReturn("took", "errors", "items",
                                              "index", "_index", "_type", "_id",
                                              "index", "_index", "_type", "_id", "error");
        // there were errors; so go diving for the error
        when(parser.booleanValue()).thenReturn(true);
        when(parser.text()).thenReturn("this is the error");

        new HttpExportBulkResponseListener(xContent).onSuccess(response);

        verify(parser, times(30)).nextToken();
        verify(parser, times(12)).currentName();
        verify(parser).booleanValue();
        verify(parser).text();
    }

    public void testOnSuccessMalformed() {
        final AtomicInteger counter = new AtomicInteger(0);
        final Response response = mock(Response.class);

        if (randomBoolean()) {
            // malformed JSON
            when(response.getEntity()).thenReturn(new StringEntity("{", ContentType.APPLICATION_JSON));
        }

        new WarningsHttpExporterBulkResponseListener() {
            @Override
            void onError(final String msg, final Throwable cause) {
                counter.getAndIncrement();
            }
        }.onSuccess(response);

        assertEquals(1, counter.get());
    }

    public void testOnFailure() {
        final Exception exception = randomBoolean() ? new Exception() : new RuntimeException();

        new WarningsHttpExporterBulkResponseListener() {
            @Override
            void onError(final String msg, final Throwable cause) {
                assertSame(exception, cause);
            }
        }.onFailure(exception);
    }

    private static class WarningsHttpExporterBulkResponseListener extends HttpExportBulkResponseListener {

        WarningsHttpExporterBulkResponseListener() {
            super(XContentType.JSON.xContent());
        }

        @Override
        void onItemError(final String msg) {
            fail("There should be no errors within the response!");
        }

        @Override
        void onError(final String msg, final Throwable cause) {
            super.onError(msg, cause); // let it log the exception so you can check the output

            fail("There should be no errors!");
        }

    }

}
