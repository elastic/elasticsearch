/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.test.ESTestCase;

public class EsqlStreamQueryRequestTests extends ESTestCase {

    public void testDropNullColumnsStoredOnRequest() {
        EsqlQueryRequest base = EsqlQueryRequest.syncEsqlQueryRequest("FROM idx");
        assertFalse(new EsqlStreamQueryRequest(base, ActionListener.noop(), false, 100).dropNullColumns());
        assertTrue(new EsqlStreamQueryRequest(base, ActionListener.noop(), true, 100).dropNullColumns());
    }

    public void testBatchSizeStoredOnRequest() {
        EsqlQueryRequest base = EsqlQueryRequest.syncEsqlQueryRequest("FROM idx");
        int batchSize = randomIntBetween(1, 1000);
        assertEquals(batchSize, new EsqlStreamQueryRequest(base, ActionListener.noop(), false, batchSize).batchSize());
    }
}
