/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.test.ESTestCase;

public class RuntimeExceptionHandlerTests extends ESTestCase {

    public void testContinueOnErrorForField() {
        RuntimeExceptionHandler h = new RuntimeExceptionHandler();
        String fieldname = randomAlphaOfLength(5);
        h.continueOnErrorForField(fieldname);
        h.handleError(new RuntimeException("test"), fieldname);

        // other field names should rethrow the error
        expectThrows(RuntimeException.class, () -> h.handleError(new RuntimeException("test"), fieldname + "x"));
    }
}
