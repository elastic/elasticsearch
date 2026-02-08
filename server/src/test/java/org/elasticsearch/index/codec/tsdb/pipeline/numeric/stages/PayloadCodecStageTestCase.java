/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages;

import org.elasticsearch.index.codec.tsdb.pipeline.DecodingContext;
import org.elasticsearch.index.codec.tsdb.pipeline.EncodingContext;
import org.elasticsearch.index.codec.tsdb.pipeline.PipelineDescriptor;

public abstract class PayloadCodecStageTestCase extends CodecStageTestCase {

    protected EncodingContext createEncodingContext(int blockSize, byte stageId) {
        final PipelineDescriptor pipeline = new PipelineDescriptor(new byte[] { stageId }, blockSize);
        final EncodingContext context = new EncodingContext(blockSize, pipeline.pipelineLength());
        context.setCurrentPosition(0);
        return context;
    }

    protected DecodingContext createDecodingContext(int blockSize, byte stageId) {
        final PipelineDescriptor pipeline = new PipelineDescriptor(new byte[] { stageId }, blockSize);
        return new DecodingContext(blockSize, pipeline.stageIds());
    }
}
