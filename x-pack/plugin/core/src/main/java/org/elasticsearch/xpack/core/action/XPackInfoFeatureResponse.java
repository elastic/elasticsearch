/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.action;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.protocol.xpack.XPackInfoResponse.FeatureSetsInfo.FeatureSet;

import java.io.IOException;

public class XPackInfoFeatureResponse extends ActionResponse {

    private final FeatureSet info;

    public XPackInfoFeatureResponse(FeatureSet info) {
        this.info = info;
    }

    public FeatureSet getInfo() {
        return info;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        info.writeTo(out);
    }

}
