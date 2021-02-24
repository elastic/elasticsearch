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

/**
 * The telemetry info for runtime fields used to be part of the xpack usage API in 7.11 and 7.12,
 * but it was made part of the cluster stats API starting from 7.13.
 * This class is here for backwards compatibility: if we are talking to 7.11 or 7.12, we need to read/write the runtime fields stats.
 * If we are in a mixed cluster between 8.x and 7.last, this class will not be used as there won't be a bound RuntimeFieldsFeatureSet.
 */
public class RuntimeFieldsFeatureSetUsage extends XPackFeatureSet.Usage {

    static final Version MINIMAL_SUPPORTED_VERSION = Version.V_7_11_0;

    public RuntimeFieldsFeatureSetUsage(StreamInput in) throws IOException {
        super(in.getVersion().before(Version.V_7_13_0) ? in.readString() : "runtime_fields",
            in.getVersion().before(Version.V_7_13_0) ? in.readBoolean() : true,
            in.getVersion().before(Version.V_7_13_0) ? in.readBoolean() : true);
        if (in.getVersion().before(Version.V_7_13_0)) {
            in.readList(RuntimeFieldStats::new);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getVersion().before(Version.V_7_13_0)) {
            super.writeTo(out);
            out.writeList(Collections.emptyList());
        }
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return MINIMAL_SUPPORTED_VERSION;
    }
}
