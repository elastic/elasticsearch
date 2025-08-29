/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata.otlp.tsid;

import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.metrics.v1.ScopeMetrics;

import org.elasticsearch.cluster.routing.TsidBuilder;
import org.elasticsearch.cluster.routing.TsidBuilder.TsidFunnel;
import org.elasticsearch.xpack.oteldata.otlp.proto.BufferedByteStringAccessor;

import java.util.List;

public class ScopeTsidFunnel implements TsidFunnel<ScopeMetrics> {

    private final BufferedByteStringAccessor byteStringAccessor;

    public ScopeTsidFunnel(BufferedByteStringAccessor byteStringAccessor) {
        this.byteStringAccessor = byteStringAccessor;
    }

    public static TsidBuilder forScope(BufferedByteStringAccessor byteStringAccessor, ScopeMetrics scopeMetrics) {
        TsidBuilder tsidBuilder = new TsidBuilder(scopeMetrics.getScope().getAttributesCount() + 3);
        new ScopeTsidFunnel(byteStringAccessor).add(scopeMetrics, tsidBuilder);
        return tsidBuilder;
    }

    @Override
    public void add(ScopeMetrics scopeMetrics, TsidBuilder tsidBuilder) {
        List<KeyValue> resourceAttributes = scopeMetrics.getScope().getAttributesList();
        byteStringAccessor.addStringDimension(tsidBuilder, "scope.name", scopeMetrics.getScope().getNameBytes());
        tsidBuilder.add(resourceAttributes, AttributeListTsidFunnel.get(byteStringAccessor, "scope.attributes."));
    }
}
