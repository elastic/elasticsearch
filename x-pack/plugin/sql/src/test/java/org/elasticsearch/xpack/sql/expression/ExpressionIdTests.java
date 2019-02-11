/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression;

import org.elasticsearch.test.ESTestCase;
import java.util.concurrent.atomic.AtomicLong;

public class ExpressionIdTests extends ESTestCase {
    /**
     * Each {@link ExpressionId} should be unique. Technically
     * you can roll the {@link AtomicLong} that backs them but
     * that is not going to happen within a single query.
     */
    public void testUnique() {
        assertNotEquals(new ExpressionId(), new ExpressionId());
    }
}
