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

public class DataPointTsidFunnel implements TsidFunnel<DataPoint> {

    // for "unit" and "_metric_names_hash" that will be added in
    // MetricDocumentBuilder once the data point group is complete
    private static final int EXTRA_DIMENSIONS_SIZE = 2;
    private final BufferedByteStringAccessor byteStringAccessor;

    private DataPointTsidFunnel(BufferedByteStringAccessor byteStringAccessor) {
        this.byteStringAccessor = byteStringAccessor;
    }

    public static TsidBuilder forDataPoint(BufferedByteStringAccessor byteStringAccessor, DataPoint dataPoint, int scopeTsidBuilderSize) {
        TsidBuilder tsidBuilder = new TsidBuilder(dataPoint.getAttributes().size() + scopeTsidBuilderSize + EXTRA_DIMENSIONS_SIZE);
        new DataPointTsidFunnel(byteStringAccessor).add(dataPoint, tsidBuilder);
        return tsidBuilder;
    }

    @Override
    public void add(DataPoint dataPoint, TsidBuilder tsidBuilder) {
        tsidBuilder.add(dataPoint.getAttributes(), AttributeListTsidFunnel.get(byteStringAccessor, "attributes."));
        tsidBuilder.addStringDimension("unit", dataPoint.getUnit());
    }
}
