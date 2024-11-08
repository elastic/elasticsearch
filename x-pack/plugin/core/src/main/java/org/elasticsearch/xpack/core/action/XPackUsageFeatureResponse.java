/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.action;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.core.XPackFeatureUsage;

import java.io.IOException;

public class XPackUsageFeatureResponse extends ActionResponse {

    private final XPackFeatureUsage usage;

    public XPackUsageFeatureResponse(StreamInput in) throws IOException {
        super(in);
        usage = in.readNamedWriteable(XPackFeatureUsage.class);
    }

    public XPackUsageFeatureResponse(XPackFeatureUsage usage) {
        this.usage = usage;
    }

    public XPackFeatureUsage getUsage() {
        return usage;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(usage);
    }

}
