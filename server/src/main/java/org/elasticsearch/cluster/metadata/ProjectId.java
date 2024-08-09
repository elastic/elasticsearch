/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;

public record ProjectId(String id) implements Writeable {

    public static final Reader<ProjectId> READER = ProjectId::new;

    public ProjectId(StreamInput in) throws IOException {
        this(in.readString());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(id);
    }
}
