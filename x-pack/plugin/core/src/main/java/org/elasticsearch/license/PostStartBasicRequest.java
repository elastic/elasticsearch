/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class PostStartBasicRequest extends AcknowledgedRequest<PostStartBasicRequest> {

    private boolean acknowledge = false;

    public PostStartBasicRequest() {}

    public PostStartBasicRequest(StreamInput in) throws IOException {
        super(in);
        acknowledge = in.readBoolean();
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    public PostStartBasicRequest acknowledge(boolean acknowledge) {
        this.acknowledge = acknowledge;
        return this;
    }

    public boolean isAcknowledged() {
        return acknowledge;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(acknowledge);
    }
}
