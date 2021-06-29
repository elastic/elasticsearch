/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import org.apache.http.client.methods.HttpPost;
import org.elasticsearch.client.textstructure.FindStructureRequest;
import org.elasticsearch.client.textstructure.FindStructureRequestTests;
import org.elasticsearch.client.textstructure.structurefinder.TextStructure;
import org.elasticsearch.common.Strings;
import org.elasticsearch.test.ESTestCase;

public class TextStructureRequestConvertersTests extends ESTestCase {

    public void testFindFileStructure() throws Exception {

        String sample = randomAlphaOfLength(randomIntBetween(1000, 2000));
        FindStructureRequest findStructureRequest = FindStructureRequestTests.createTestRequestWithoutSample();
        findStructureRequest.setSample(sample.getBytes(StandardCharsets.UTF_8));
        Request request = TextStructureRequestConverters.findFileStructure(findStructureRequest);

        assertEquals(HttpPost.METHOD_NAME, request.getMethod());
        assertEquals("/_text_structure/find_structure", request.getEndpoint());
        if (findStructureRequest.getLinesToSample() != null) {
            assertEquals(findStructureRequest.getLinesToSample(), Integer.valueOf(request.getParameters().get("lines_to_sample")));
        } else {
            assertNull(request.getParameters().get("lines_to_sample"));
        }
        if (findStructureRequest.getTimeout() != null) {
            assertEquals(findStructureRequest.getTimeout().toString(), request.getParameters().get("timeout"));
        } else {
            assertNull(request.getParameters().get("timeout"));
        }
        if (findStructureRequest.getCharset() != null) {
            assertEquals(findStructureRequest.getCharset(), request.getParameters().get("charset"));
        } else {
            assertNull(request.getParameters().get("charset"));
        }
        if (findStructureRequest.getFormat() != null) {
            assertEquals(findStructureRequest.getFormat(), TextStructure.Format.fromString(request.getParameters().get("format")));
        } else {
            assertNull(request.getParameters().get("format"));
        }
        if (findStructureRequest.getColumnNames() != null) {
            assertEquals(findStructureRequest.getColumnNames(),
                Arrays.asList(Strings.splitStringByCommaToArray(request.getParameters().get("column_names"))));
        } else {
            assertNull(request.getParameters().get("column_names"));
        }
        if (findStructureRequest.getHasHeaderRow() != null) {
            assertEquals(findStructureRequest.getHasHeaderRow(), Boolean.valueOf(request.getParameters().get("has_header_row")));
        } else {
            assertNull(request.getParameters().get("has_header_row"));
        }
        if (findStructureRequest.getDelimiter() != null) {
            assertEquals(findStructureRequest.getDelimiter().toString(), request.getParameters().get("delimiter"));
        } else {
            assertNull(request.getParameters().get("delimiter"));
        }
        if (findStructureRequest.getQuote() != null) {
            assertEquals(findStructureRequest.getQuote().toString(), request.getParameters().get("quote"));
        } else {
            assertNull(request.getParameters().get("quote"));
        }
        if (findStructureRequest.getShouldTrimFields() != null) {
            assertEquals(findStructureRequest.getShouldTrimFields(),
                Boolean.valueOf(request.getParameters().get("should_trim_fields")));
        } else {
            assertNull(request.getParameters().get("should_trim_fields"));
        }
        if (findStructureRequest.getGrokPattern() != null) {
            assertEquals(findStructureRequest.getGrokPattern(), request.getParameters().get("grok_pattern"));
        } else {
            assertNull(request.getParameters().get("grok_pattern"));
        }
        if (findStructureRequest.getTimestampFormat() != null) {
            assertEquals(findStructureRequest.getTimestampFormat(), request.getParameters().get("timestamp_format"));
        } else {
            assertNull(request.getParameters().get("timestamp_format"));
        }
        if (findStructureRequest.getTimestampField() != null) {
            assertEquals(findStructureRequest.getTimestampField(), request.getParameters().get("timestamp_field"));
        } else {
            assertNull(request.getParameters().get("timestamp_field"));
        }
        if (findStructureRequest.getExplain() != null) {
            assertEquals(findStructureRequest.getExplain(), Boolean.valueOf(request.getParameters().get("explain")));
        } else {
            assertNull(request.getParameters().get("explain"));
        }
        assertEquals(sample, requestEntityToString(request));
    }

    private static String requestEntityToString(Request request) throws Exception {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        request.getEntity().writeTo(bos);
        return bos.toString("UTF-8");
    }
}
