/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.coordination;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.TimeValue;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public record JoinStatus(DiscoveryNode remoteNode, long term, String message, TimeValue age) implements Writeable {
    public JoinStatus(StreamInput in) throws IOException {
        this(new DiscoveryNode(in), in.readLong(), in.readString(), new TimeValue(in.readLong(), TimeUnit.valueOf(in.readString())));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        remoteNode.writeTo(out);
        out.writeLong(term);
        out.writeString(message);
        out.writeLong(age.duration());
        out.writeString(age.timeUnit().name());
    }
}
