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

import static org.hamcrest.Matchers.containsString;

public class EsqlStreamQueryRequestTests extends ESTestCase {

    public void testValidateRejectsZeroBatchSize() {
        EsqlStreamQueryRequest req = EsqlStreamQueryRequest.from(
            EsqlQueryRequest.syncEsqlQueryRequest("FROM idx"),
            ActionListener.noop(),
            false,
            0
        );
        ActionRequestValidationException e = req.validate();
        assertNotNull("validate() must return a non-null exception when batch_size is 0", e);
        assertThat(e.getMessage(), containsString("batch_size"));
    }

    public void testValidateRejectsNegativeBatchSize() {
        EsqlStreamQueryRequest req = EsqlStreamQueryRequest.from(
            EsqlQueryRequest.syncEsqlQueryRequest("FROM idx"),
            ActionListener.noop(),
            false,
            -1
        );
        ActionRequestValidationException e = req.validate();
        assertNotNull("validate() must return a non-null exception when batch_size is negative", e);
        assertThat(e.getMessage(), containsString("batch_size"));
    }

    public void testValidateAcceptsPositiveBatchSize() {
        EsqlStreamQueryRequest req = EsqlStreamQueryRequest.from(
            EsqlQueryRequest.syncEsqlQueryRequest("FROM idx"),
            ActionListener.noop(),
            false,
            randomIntBetween(1, 1000)
        );
        ActionRequestValidationException e = req.validate();
        assertNull("validate() must return null for a valid batch_size", e);
    }

    public void testDropNullColumnsStoredOnRequest() {
        EsqlQueryRequest base = EsqlQueryRequest.syncEsqlQueryRequest("FROM idx");
        assertFalse(EsqlStreamQueryRequest.from(base, ActionListener.noop(), false, 100).dropNullColumns());
        assertTrue(EsqlStreamQueryRequest.from(base, ActionListener.noop(), true, 100).dropNullColumns());
    }

    public void testBatchSizeStoredOnRequest() {
        EsqlQueryRequest base = EsqlQueryRequest.syncEsqlQueryRequest("FROM idx");
        int batchSize = randomIntBetween(1, 1000);
        assertEquals(batchSize, EsqlStreamQueryRequest.from(base, ActionListener.noop(), false, batchSize).batchSize());
    }
}
