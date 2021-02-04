/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.snapshots.features;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

public class ResetFeatureStateResponse extends ActionResponse implements ToXContentObject {

    public ResetFeatureStateResponse() {
        // TODO[wrb]
    }

    public ResetFeatureStateResponse(StreamInput in) throws IOException {
        super(in);
        // TODO[wrb]
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        // TODO[wrb]
        return null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        // TODO[wrb]
    }
}
