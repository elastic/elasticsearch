/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.support.nodes;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.transport.TransportRequest;

import java.io.IOException;

// TODO: this class can be removed in master once 7.x is bumped to 7.4.0
public abstract class BaseNodeRequest extends TransportRequest {

    public BaseNodeRequest() {}

    public BaseNodeRequest(StreamInput in) throws IOException {
        super(in);
        if (in.getVersion().before(Version.V_7_3_0)) {
            in.readString(); // previously nodeId
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        if (out.getVersion().before(Version.V_7_3_0)) {
            out.writeString(""); // previously nodeId
        }
    }
}
