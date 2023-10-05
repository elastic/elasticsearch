/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.StreamInput;

public class BlockStreamInput extends NamedWriteableAwareStreamInput {
    private final BlockFactory blockFactory;

    public BlockStreamInput(StreamInput delegate, BlockFactory blockFactory) {
        super(delegate, delegate.namedWriteableRegistry());
        this.blockFactory = blockFactory;
    }

    BlockFactory blockFactory() {
        return blockFactory;
    }
}
