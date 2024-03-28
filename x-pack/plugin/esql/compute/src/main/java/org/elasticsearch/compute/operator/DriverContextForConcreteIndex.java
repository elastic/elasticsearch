/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;

/**
 * A driver context that includes an index name. To be used for union types when functions need to differentiate between different indices.
 */
public final class DriverContextForConcreteIndex extends DriverContext {
    private final String indexName;

    public DriverContextForConcreteIndex(BigArrays bigArrays, BlockFactory blockFactory, String indexName) {
        super(bigArrays, blockFactory);
        this.indexName = indexName;
    }

    public String indexName() {
        return indexName;
    }
}
