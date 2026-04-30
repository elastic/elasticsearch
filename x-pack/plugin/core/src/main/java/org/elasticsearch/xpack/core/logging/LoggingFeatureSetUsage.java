/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.logging;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.XPackFeatureUsage;
import org.elasticsearch.xpack.core.XPackField;

import java.io.IOException;

public class LoggingFeatureSetUsage extends XPackFeatureUsage {

    private final boolean queryLogEnabled;
    private final boolean esqlLogEnabled;

    public LoggingFeatureSetUsage(boolean queryLogEnabled, boolean esqlLogEnabled) {
        super(XPackField.LOGGING, true, true);
        this.queryLogEnabled = queryLogEnabled;
        this.esqlLogEnabled = esqlLogEnabled;
    }

    public LoggingFeatureSetUsage(StreamInput input) throws IOException {
        super(input);
        queryLogEnabled = input.readBoolean();
        esqlLogEnabled = input.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(queryLogEnabled);
        out.writeBoolean(esqlLogEnabled);
    }

    @Override
    protected void innerXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("querylog", queryLogEnabled);
        builder.field("esql", esqlLogEnabled);
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersion.zero();
    }
}
