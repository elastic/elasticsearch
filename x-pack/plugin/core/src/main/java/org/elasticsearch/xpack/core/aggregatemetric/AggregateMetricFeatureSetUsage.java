/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.aggregatemetric;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.xpack.core.XPackFeatureSet;
import org.elasticsearch.xpack.core.XPackField;

import java.io.IOException;
import java.util.Objects;

public class AggregateMetricFeatureSetUsage extends XPackFeatureSet.Usage {

    public AggregateMetricFeatureSetUsage(StreamInput input) throws IOException {
        super(input);
    }

    public AggregateMetricFeatureSetUsage(boolean available, boolean enabled) {
        super(XPackField.AGGREGATE_METRIC, available, enabled);
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return Version.V_7_11_0;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        AggregateMetricFeatureSetUsage other = (AggregateMetricFeatureSetUsage) obj;
        return Objects.equals(available, other.available) && Objects.equals(enabled, other.enabled);
    }

    @Override
    public int hashCode() {
        return Objects.hash(available, enabled);
    }
}
