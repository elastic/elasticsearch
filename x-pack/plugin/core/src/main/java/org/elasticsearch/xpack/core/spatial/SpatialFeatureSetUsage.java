/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.spatial;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.core.XPackFeatureSet;
import org.elasticsearch.xpack.core.XPackField;

import java.io.IOException;
import java.util.Objects;

public class SpatialFeatureSetUsage extends XPackFeatureSet.Usage {

    public SpatialFeatureSetUsage(boolean available, boolean enabled) {
        super(XPackField.SPATIAL, available, enabled);
    }

    public SpatialFeatureSetUsage(StreamInput input) throws IOException {
        super(input);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
    }

    @Override
    public int hashCode() {
        return Objects.hash(available, enabled);
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
        return Objects.equals(available, other.available) &&
            Objects.equals(enabled, other.enabled);
    }
}
