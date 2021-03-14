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
 * The telemetry info for runtime fields used to be part of the x-pack usage API in 7.11 and 7.12. It was moved to the
 * cluster stats API starting from 7.13.
 * The x-pack runtime fields plugin only exists for 7.11 and 7.12, hence RuntimeFieldsFeatureSet is only bound on such versions.
 * In a mixed cluster scenario between different 7.x nodes, when xpack/usage gets called on a 7.11 or 7.12 node that have a bound
 * RuntimeFieldsFeatureSet, this class will be serialized, hence it needs to be registered as a known
 * {@link org.elasticsearch.common.io.stream.NamedWriteable} until the last minor version of the 7.x series.
 * If we are in a mixed cluster between 8.x and 7.last, this class will not be used as there won't be a bound RuntimeFieldsFeatureSet
 * in any of the nodes.
 *
 * @deprecated This class exists for backwards compatibility with 7.11 and 7.12 only and should not be used for other purposes.
 */
@Deprecated
public class RuntimeFieldsFeatureSetUsage extends XPackFeatureSet.Usage {

    static final Version MINIMAL_SUPPORTED_VERSION = Version.V_7_11_0;

    public RuntimeFieldsFeatureSetUsage(StreamInput in) throws IOException {
        super(in);
        in.readList(RuntimeFieldStats::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeList(Collections.emptyList());
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return MINIMAL_SUPPORTED_VERSION;
    }
}
