/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.action;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.protocol.xpack.XPackInfoResponse.FeatureSetsInfo.FeatureSet;

import java.io.IOException;

public class XPackInfoFeatureResponse extends ActionResponse {

    private FeatureSet info;

    public XPackInfoFeatureResponse() {
        // empty, for readFrom
    }

    public XPackInfoFeatureResponse(FeatureSet info) {
        this.info = info;
    }

    public FeatureSet getInfo() {
        return info;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        info.writeTo(out);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        info = new FeatureSet(in);
    }
}
