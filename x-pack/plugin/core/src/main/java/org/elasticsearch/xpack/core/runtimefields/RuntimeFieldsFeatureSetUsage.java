/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.runtimefields;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.stats.RuntimeFieldStats;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.core.XPackFeatureSet;

import java.io.IOException;
import java.util.Collections;

public class RuntimeFieldsFeatureSetUsage extends XPackFeatureSet.Usage {

    public RuntimeFieldsFeatureSetUsage(StreamInput input) throws IOException {
        super(input);
        input.readList(RuntimeFieldStats::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeList(Collections.emptyList());
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return Version.V_7_11_0;
    }
}
