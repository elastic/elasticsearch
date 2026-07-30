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

public class EsqlStreamQueryRequestTests extends ESTestCase {

    public void testValidateRejectsNullPageSize() {
        EsqlStreamQueryRequest req = EsqlStreamQueryRequest.from(EsqlQueryRequest.syncEsqlQueryRequest("FROM idx"), ActionListener.noop());
        ActionRequestValidationException e = req.validate();
        assertNotNull("validate() must return a non-null exception when page_size is missing", e);
        assertThat(e.getMessage(), org.hamcrest.Matchers.containsString("page_size"));
    }

    public void testValidateRejectsZeroPageSize() {
        EsqlQueryRequest base = EsqlQueryRequest.syncEsqlQueryRequest("FROM idx");
        base.pageSize(0);
        EsqlStreamQueryRequest req = EsqlStreamQueryRequest.from(base, ActionListener.noop());
        ActionRequestValidationException e = req.validate();
        assertNotNull("validate() must return a non-null exception when page_size is 0", e);
        assertThat(e.getMessage(), org.hamcrest.Matchers.containsString("page_size"));
    }

    public void testValidateRejectsNegativePageSize() {
        EsqlQueryRequest base = EsqlQueryRequest.syncEsqlQueryRequest("FROM idx");
        base.pageSize(-1);
        EsqlStreamQueryRequest req = EsqlStreamQueryRequest.from(base, ActionListener.noop());
        ActionRequestValidationException e = req.validate();
        assertNotNull("validate() must return a non-null exception when page_size is negative", e);
        assertThat(e.getMessage(), org.hamcrest.Matchers.containsString("page_size"));
    }

    public void testValidateAcceptsPositivePageSize() {
        EsqlQueryRequest base = EsqlQueryRequest.syncEsqlQueryRequest("FROM idx");
        base.pageSize(randomIntBetween(1, 10_000));
        EsqlStreamQueryRequest req = EsqlStreamQueryRequest.from(base, ActionListener.noop());
        ActionRequestValidationException e = req.validate();
        assertNull("validate() must return null for a valid page_size", e);
    }
}
