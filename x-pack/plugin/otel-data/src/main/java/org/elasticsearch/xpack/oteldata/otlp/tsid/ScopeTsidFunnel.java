/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata.otlp.tsid;

import io.opentelemetry.proto.common.v1.InstrumentationScope;
import io.opentelemetry.proto.common.v1.KeyValue;

import org.elasticsearch.cluster.routing.TsidBuilder;
import org.elasticsearch.cluster.routing.TsidBuilder.TsidFunnel;
import org.elasticsearch.xpack.oteldata.otlp.proto.BufferedByteStringAccessor;

import java.util.List;

public class ScopeTsidFunnel implements TsidFunnel<InstrumentationScope> {

    private final BufferedByteStringAccessor byteStringAccessor;

    public ScopeTsidFunnel(BufferedByteStringAccessor byteStringAccessor) {
        this.byteStringAccessor = byteStringAccessor;
    }

    public static TsidBuilder forScope(
        BufferedByteStringAccessor byteStringAccessor,
        InstrumentationScope scope,
        int resourceTsidBuilderSize
    ) {
        // + 1 for scope.name
        TsidBuilder tsidBuilder = new TsidBuilder(scope.getAttributesCount() + 1 + resourceTsidBuilderSize);
        new ScopeTsidFunnel(byteStringAccessor).add(scope, tsidBuilder);
        return tsidBuilder;
    }

    @Override
    public void add(InstrumentationScope scope, TsidBuilder tsidBuilder) {
        List<KeyValue> scopeAttributes = scope.getAttributesList();
        byteStringAccessor.addStringDimension(tsidBuilder, "scope.name", scope.getNameBytes());
        tsidBuilder.add(scopeAttributes, AttributeListTsidFunnel.get(byteStringAccessor, "scope.attributes."));
    }
}
