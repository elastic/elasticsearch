/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.Page;

import java.util.List;

interface TopNProcessor {
    RowFiller rowFiller(List<ElementType> elementTypes, List<TopNEncoder> encoders, List<TopNOperator.SortOrder> sortOrders, Page page);

    Row row(CircuitBreaker breaker, List<TopNOperator.SortOrder> sortOrders, int spareKeysPreAllocSize, int spareValuesPreAllocSize);

    TopNQueue queue(CircuitBreaker breaker, int topCount);
}
