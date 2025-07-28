/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.downsample;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;

/**
 * Status of the rollup indexer task
 */
public enum DownsampleShardIndexerStatus implements Writeable {
    INITIALIZED,
    STARTED,
    FAILED,
    COMPLETED,
    CANCELLED;

    public static DownsampleShardIndexerStatus readFromStream(final StreamInput in) throws IOException {
        return in.readEnum(DownsampleShardIndexerStatus.class);
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        out.writeEnum(this);
    }
}
