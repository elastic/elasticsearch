/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata.otlp.datapoint;

import io.opentelemetry.proto.resource.v1.Resource;

/**
 * Map key that groups resources sharing the resource TSID dimensions
 * (see {@link org.elasticsearch.xpack.oteldata.otlp.tsid.ResourceTsidFunnel}).
 */
final class ResourceEqualityWrapper extends AttributesEqualityWrapper {

    private final int hashCode;

    ResourceEqualityWrapper(Resource resource) {
        super(resource.getAttributesList());
        this.hashCode = computeHashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        return o instanceof ResourceEqualityWrapper that && hashCode == that.hashCode && attributesEqual(that);
    }

    @Override
    public int hashCode() {
        return hashCode;
    }
}
