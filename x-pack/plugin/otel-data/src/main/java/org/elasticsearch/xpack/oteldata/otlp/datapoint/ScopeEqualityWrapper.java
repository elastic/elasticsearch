/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata.otlp.datapoint;

import io.opentelemetry.proto.common.v1.InstrumentationScope;

/**
 * Map key that groups scopes sharing the scope TSID dimensions
 * (see {@link org.elasticsearch.xpack.oteldata.otlp.tsid.ScopeTsidFunnel}) within a resource.
 */
final class ScopeEqualityWrapper extends AttributesEqualityWrapper {

    private final String name;
    private final int hashCode;

    ScopeEqualityWrapper(InstrumentationScope scope) {
        super(scope.getAttributesList());
        this.name = scope.getName();
        this.hashCode = computeHashCode();
    }

    @Override
    protected int computeHashCode() {
        return 31 * name.hashCode() + super.computeHashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        return o instanceof ScopeEqualityWrapper that && hashCode == that.hashCode && name.equals(that.name) && attributesEqual(that);
    }

    @Override
    public int hashCode() {
        return hashCode;
    }
}
