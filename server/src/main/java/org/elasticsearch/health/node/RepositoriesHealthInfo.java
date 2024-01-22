/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.health.node;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.List;

/**
 * The repository health info for a node.
 */
public record RepositoriesHealthInfo(
    List<String> unknownRepositories,
    List<String> invalidRepositories
) implements Writeable {
    public RepositoriesHealthInfo(StreamInput in) throws IOException {
        this(in.readStringCollectionAsList(), in.readStringCollectionAsList());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeStringCollection(unknownRepositories);
        out.writeStringCollection(invalidRepositories);
    }
}
