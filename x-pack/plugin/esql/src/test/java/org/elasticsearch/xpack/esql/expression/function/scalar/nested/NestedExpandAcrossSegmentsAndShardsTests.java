/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.expression.function.scalar.nested;

/**
 * Runs every {@link NestedExpandOperatorTests} case with parents spread over several segments and shards, so the
 * children of one page come from different segments and shards.
 */
public class NestedExpandAcrossSegmentsAndShardsTests extends NestedExpandOperatorTests {
    @Override
    protected int parentsPerSegment() {
        return 7;
    }

    @Override
    protected int shardCount() {
        return 3;
    }
}
