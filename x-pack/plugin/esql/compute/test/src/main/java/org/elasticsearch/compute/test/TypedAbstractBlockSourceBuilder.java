/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.test;

import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.test.operator.blocksource.AbstractBlockSourceOperator;

import java.util.List;

public abstract class TypedAbstractBlockSourceBuilder extends AbstractBlockSourceOperator {
    protected TypedAbstractBlockSourceBuilder(BlockFactory blockFactory, int maxPagePositions) {
        super(blockFactory, maxPagePositions);
    }

    public abstract List<ElementType> elementTypes();
}
