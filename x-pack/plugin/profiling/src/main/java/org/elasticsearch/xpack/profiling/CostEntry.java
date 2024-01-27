/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.profiling;

import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;

final class CostEntry implements ToXContentObject {
    final double costFactor;

    CostEntry(double costFactor) {
        this.costFactor = costFactor;
    }

    public static CostEntry fromSource(Map<String, Object> source) {
        return new CostEntry((Double) source.get("cost_factor"));
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("cost_factor", this.costFactor);
        builder.endObject();
        return builder;
    }
}
