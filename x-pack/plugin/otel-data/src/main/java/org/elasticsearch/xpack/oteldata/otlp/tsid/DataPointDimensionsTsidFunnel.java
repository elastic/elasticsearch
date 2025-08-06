/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata.otlp.tsid;

import org.elasticsearch.cluster.routing.TsidBuilder;
import org.elasticsearch.cluster.routing.TsidBuilder.TsidFunnel;
import org.elasticsearch.xpack.oteldata.otlp.datapoint.DataPoint;
import org.elasticsearch.xpack.oteldata.otlp.proto.BufferedByteStringAccessor;

class DataPointDimensionsTsidFunnel implements TsidFunnel<DataPoint> {

    private final BufferedByteStringAccessor byteStringAccessor;

    private DataPointDimensionsTsidFunnel(BufferedByteStringAccessor byteStringAccessor) {
        this.byteStringAccessor = byteStringAccessor;
    }

    static DataPointDimensionsTsidFunnel get(BufferedByteStringAccessor byteStringAccessor) {
        return new DataPointDimensionsTsidFunnel(byteStringAccessor);
    }

    @Override
    public void add(DataPoint dataPoint, TsidBuilder tsidBuilder) {
        tsidBuilder.add(dataPoint.getAttributes(), AttributeListTsidFunnel.get(byteStringAccessor, "attributes."));
        tsidBuilder.addStringDimension("unit", dataPoint.getUnit());
    }
}
