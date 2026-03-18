/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data.arrow;

import org.apache.arrow.memory.ArrowBuf;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Vector;

/**
 * Constructor for a vector backed by Arrow buffers. See @{@link AbstractArrowBufVector} for additional details.
 */
@FunctionalInterface
public interface ArrowBufVectorConstructor<V extends Vector> {
    V create(ArrowBuf valueBuffer, int positionCount, BlockFactory blockFactory);
}
