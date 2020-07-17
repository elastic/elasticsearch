/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.eql;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.XPackFeatureSet;
import org.elasticsearch.xpack.core.XPackField;

import java.io.IOException;
import java.util.Map;

public class EqlFeatureSetUsage extends XPackFeatureSet.Usage {

    private final Map<String, Object> stats;

    public EqlFeatureSetUsage(StreamInput in) throws IOException {
        super(in);
        stats = in.readMap();
    }

    public EqlFeatureSetUsage(boolean available, boolean enabled, Map<String, Object> stats) {
        super(XPackField.EQL, available, enabled);
        this.stats = stats;
    }

    public Map<String, Object> stats() {
        return stats;
    }

    @Override
    protected void innerXContent(XContentBuilder builder, Params params) throws IOException {
        super.innerXContent(builder, params);
        if (enabled) {
            for (Map.Entry<String, Object> entry : stats.entrySet()) {
                builder.field(entry.getKey(), entry.getValue());
            }
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeMap(stats);
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return Version.V_7_9_0;
    }

}
