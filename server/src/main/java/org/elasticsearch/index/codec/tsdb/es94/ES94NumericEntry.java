/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.es94;

import org.elasticsearch.index.codec.tsdb.AbstractTSDBDocValuesProducer;
import org.elasticsearch.index.codec.tsdb.pipeline.PipelineDescriptor;

/**
 * ES94 numeric entry carrying a {@link PipelineDescriptor} that describes
 * the encoding pipeline used for this field. The descriptor is read from
 * segment metadata and used to reconstruct the decoder at iteration time.
 */
final class ES94NumericEntry extends AbstractTSDBDocValuesProducer.NumericEntry implements PipelineDescriptorEntry {

    private PipelineDescriptor pipelineDescriptor;

    @Override
    public PipelineDescriptor pipelineDescriptor() {
        return pipelineDescriptor;
    }

    @Override
    public void pipelineDescriptor(PipelineDescriptor pipelineDescriptor) {
        this.pipelineDescriptor = pipelineDescriptor;
    }
}
