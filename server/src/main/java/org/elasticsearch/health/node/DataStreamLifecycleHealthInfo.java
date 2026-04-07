/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.health.node;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.List;

/**
 * Represents the data stream lifecycle information that would help shape the functionality's health.
 */
public record DataStreamLifecycleHealthInfo(List<DslErrorInfo> dslErrorsInfo, int totalErrorEntriesCount) implements Writeable {

    public static final DataStreamLifecycleHealthInfo NO_DSL_ERRORS = new DataStreamLifecycleHealthInfo(List.of(), 0);

    public DataStreamLifecycleHealthInfo(StreamInput in) throws IOException {
        this(in.readCollectionAsList(DslErrorInfo::new), in.readVInt());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeCollection(dslErrorsInfo);
        out.writeVInt(totalErrorEntriesCount);
    }
}
