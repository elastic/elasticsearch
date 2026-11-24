/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata.otlp.tsid;

import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.resource.v1.Resource;

import org.elasticsearch.cluster.routing.TsidBuilder;
import org.elasticsearch.cluster.routing.TsidBuilder.TsidFunnel;
import org.elasticsearch.xpack.oteldata.otlp.proto.BufferedByteStringAccessor;

import java.util.List;

public class ResourceTsidFunnel implements TsidFunnel<Resource> {

    private final BufferedByteStringAccessor byteStringAccessor;

    public ResourceTsidFunnel(BufferedByteStringAccessor byteStringAccessor) {
        this.byteStringAccessor = byteStringAccessor;
    }

    public static TsidBuilder forResource(BufferedByteStringAccessor byteStringAccessor, Resource resource) {
        TsidBuilder tsidBuilder = new TsidBuilder(resource.getAttributesCount());
        new ResourceTsidFunnel(byteStringAccessor).add(resource, tsidBuilder);
        return tsidBuilder;
    }

    @Override
    public void add(Resource resource, TsidBuilder tsidBuilder) {
        List<KeyValue> resourceAttributes = resource.getAttributesList();
        tsidBuilder.add(resourceAttributes, AttributeListTsidFunnel.get(byteStringAccessor, "resource.attributes."));
    }
}
