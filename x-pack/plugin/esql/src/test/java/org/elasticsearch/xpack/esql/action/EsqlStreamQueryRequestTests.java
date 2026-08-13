/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Locale;

import static org.hamcrest.Matchers.containsString;

public class EsqlStreamQueryRequestTests extends ESTestCase {

    public void testValidateRejectsNullPageSize() {
        EsqlStreamQueryRequest req = EsqlStreamQueryRequest.from(
            EsqlQueryRequest.syncEsqlQueryRequest("FROM idx"),
            ActionListener.noop(),
            false
        );
        ActionRequestValidationException e = req.validate();
        assertNotNull("validate() must return a non-null exception when page_size is missing", e);
        assertThat(e.getMessage(), containsString("page_size"));
    }

    public void testValidateRejectsZeroPageSize() {
        EsqlQueryRequest base = EsqlQueryRequest.syncEsqlQueryRequest("FROM idx");
        base.pageSize(0);
        EsqlStreamQueryRequest req = EsqlStreamQueryRequest.from(base, ActionListener.noop(), false);
        ActionRequestValidationException e = req.validate();
        assertNotNull("validate() must return a non-null exception when page_size is 0", e);
        assertThat(e.getMessage(), containsString("page_size"));
    }

    public void testValidateRejectsNegativePageSize() {
        EsqlQueryRequest base = EsqlQueryRequest.syncEsqlQueryRequest("FROM idx");
        base.pageSize(-1);
        EsqlStreamQueryRequest req = EsqlStreamQueryRequest.from(base, ActionListener.noop(), false);
        ActionRequestValidationException e = req.validate();
        assertNotNull("validate() must return a non-null exception when page_size is negative", e);
        assertThat(e.getMessage(), containsString("page_size"));
    }

    public void testValidateAcceptsPositivePageSize() {
        EsqlQueryRequest base = EsqlQueryRequest.syncEsqlQueryRequest("FROM idx");
        base.pageSize(randomIntBetween(1, 10_000));
        EsqlStreamQueryRequest req = EsqlStreamQueryRequest.from(base, ActionListener.noop(), false);
        ActionRequestValidationException e = req.validate();
        assertNull("validate() must return null for a valid page_size", e);
    }

    public void testDropNullColumnsStoredOnRequest() {
        EsqlQueryRequest base = EsqlQueryRequest.syncEsqlQueryRequest("FROM idx");
        assertFalse(EsqlStreamQueryRequest.from(base, ActionListener.noop(), false).dropNullColumns());
        assertTrue(EsqlStreamQueryRequest.from(base, ActionListener.noop(), true).dropNullColumns());
    }

    public void testParseStreamAllSupportedFields() throws IOException {
        Locale locale = randomLocale(random());
        String json = String.format(Locale.ROOT, """
            {
                "query": "FROM idx",
                "filter": {"term": {"field": "value"}},
                "accept_pragma_risks": true,
                "pragma": {},
                "params": [1],
                "locale": "%s",
                "tables": {"t": {"c": {"keyword": ["v"]}}},
                "page_size": 100
            }
            """, locale.toLanguageTag());

        EsqlQueryRequest request = EsqlQueryRequestTests.parseEsqlQueryRequest(json, RequestXContent::parseStream);

        assertEquals("FROM idx", request.query());
        assertNotNull(request.filter());
        assertNotNull(request.pragmas());
        assertEquals(1, request.params().size());
        assertEquals(locale, request.locale());
        assertNotNull(request.tables());
        assertFalse(request.tables().isEmpty());
        assertEquals(100, (int) request.pageSize());
    }

    public void testParseStreamRejectsUnknownFields() {
        String unknownFieldName = "unknown_field";
        String json = "{\"query\": \"FROM idx\", \"" + unknownFieldName + "\": true}";

        Exception e = expectThrows(
            IllegalArgumentException.class,
            () -> EsqlQueryRequestTests.parseEsqlQueryRequest(json, RequestXContent::parseStream)
        );
        assertThat(e.getMessage(), containsString(unknownFieldName));
    }
}
