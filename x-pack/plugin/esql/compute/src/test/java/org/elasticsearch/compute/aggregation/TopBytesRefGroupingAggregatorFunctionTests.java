/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.xpack.esql.core.type.DataType;

public class TopBytesRefGroupingAggregatorFunctionTests extends AbstractTopBytesRefGroupingAggregatorFunctionTests {
    @Override
    protected BytesRef randomValue() {
        return new BytesRef(randomAlphaOfLength(6));
    }

    @Override
    protected final AggregatorFunctionSupplier aggregatorFunction() {
        return new TopBytesRefAggregatorFunctionSupplier(LIMIT, true);
    }

    @Override
    protected DataType acceptedDataType() {
        return DataType.KEYWORD;
    }

    @Override
    protected String expectedDescriptionOfAggregator() {
        return "top of bytes";
    }
}
