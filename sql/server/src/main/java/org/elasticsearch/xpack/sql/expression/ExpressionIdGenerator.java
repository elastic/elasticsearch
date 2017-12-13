/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

//TODO: this class is thread-safe but used across multiple sessions might cause the id to roll over and potentially generate an already assigned id
// making this session scope would simplify things
// (which also begs the question on whether thread-safety is needed than)

// TODO: hook this into SqlSession#SessionContext
public class ExpressionIdGenerator {

    private static final AtomicInteger GLOBAL_ID = new AtomicInteger();
    private static final String JVM_ID = "@" + UUID.randomUUID().toString();

    public static final ExpressionId EMPTY = new ExpressionId(-1, "@<empty>");

    public static ExpressionId newId() {
        return new ExpressionId(GLOBAL_ID.getAndIncrement(), JVM_ID);
    }
}
