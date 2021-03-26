/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ql.expression;

import org.elasticsearch.test.ESTestCase;

import java.util.concurrent.atomic.AtomicLong;

public class ExpressionIdTests extends ESTestCase {
    /**
     * Each {@link NameId} should be unique. Technically
     * you can roll the {@link AtomicLong} that backs them but
     * that is not going to happen within a single query.
     */
    public void testUnique() {
        assertNotEquals(new NameId(), new NameId());
    }
}
