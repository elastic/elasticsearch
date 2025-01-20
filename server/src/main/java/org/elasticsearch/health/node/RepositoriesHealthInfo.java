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
 * Health info regarding repository health for a node. It refers to issues that are local to a node such as the unknown and
 * invalid repositories.
 */
public record RepositoriesHealthInfo(List<String> unknownRepositories, List<String> invalidRepositories) implements Writeable {
    public RepositoriesHealthInfo(StreamInput in) throws IOException {
        this(in.readStringCollectionAsList(), in.readStringCollectionAsList());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeStringCollection(unknownRepositories);
        out.writeStringCollection(invalidRepositories);
    }
}
