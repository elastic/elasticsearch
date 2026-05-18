/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.eql.execution.assembler;

import org.elasticsearch.xpack.eql.execution.search.QueryRequest;

public class Criterion<Q extends QueryRequest> {

    private final int keySize;

    public Criterion(int keySize) {
        this.keySize = keySize;
    }

    public int keySize() {
        return keySize;
    }
}
