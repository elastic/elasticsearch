/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.spatial;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.core.XPackFeatureSet;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.spatial.action.SpatialStatsAction;

import java.io.IOException;
import java.util.Objects;

public class SpatialFeatureSetUsage extends XPackFeatureSet.Usage {

    private final SpatialStatsAction.Response statsResponse;

    public SpatialFeatureSetUsage(SpatialStatsAction.Response statsResponse) {
        super(XPackField.SPATIAL, true, true);
        this.statsResponse = statsResponse;
    }

    public SpatialFeatureSetUsage(StreamInput input) throws IOException {
        super(input);
        if (input.getTransportVersion().onOrAfter(TransportVersion.V_7_11_0)) {
            this.statsResponse = new SpatialStatsAction.Response(input);
        } else {
            this.statsResponse = null;
        }
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersion.V_7_4_0;
    }

    SpatialStatsAction.Response statsResponse() {
        return statsResponse;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        if (out.getTransportVersion().onOrAfter(TransportVersion.V_7_11_0)) {
            this.statsResponse.writeTo(out);
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(available, enabled, statsResponse);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        SpatialFeatureSetUsage other = (SpatialFeatureSetUsage) obj;
        return Objects.equals(statsResponse, other.statsResponse);
    }
}
