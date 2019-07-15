/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.geo;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.core.XPackFeatureSet;
import org.elasticsearch.xpack.core.XPackField;

import java.io.IOException;
import java.util.Objects;

public class GeoFeatureSetUsage extends XPackFeatureSet.Usage {

    public GeoFeatureSetUsage(boolean available, boolean enabled) {
        super(XPackField.GEO, available, enabled);
    }

    public GeoFeatureSetUsage(StreamInput input) throws IOException {
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
        GeoFeatureSetUsage other = (GeoFeatureSetUsage) obj;
        return Objects.equals(available, other.available) &&
            Objects.equals(enabled, other.enabled);
    }
}
