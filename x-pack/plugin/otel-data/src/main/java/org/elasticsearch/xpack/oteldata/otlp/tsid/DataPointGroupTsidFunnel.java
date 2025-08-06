/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata.otlp.tsid;

import org.elasticsearch.cluster.routing.TsidBuilder;
import org.elasticsearch.xpack.oteldata.otlp.datapoint.DataPointGroupingContext;

public class DataPointGroupTsidFunnel implements TsidBuilder.TsidFunnel<DataPointGroupingContext.DataPointGroup> {

    private static final DataPointGroupTsidFunnel INSTANCE = new DataPointGroupTsidFunnel();

    public static TsidBuilder forDataPointGroup(DataPointGroupingContext.DataPointGroup value) {
        TsidBuilder tsidBuilder = new TsidBuilder(
            1 + value.dataPointGroupTsidBuilder().size() + value.resourceTsidBuilder().size() + value.scopeTsidBuilder().size()
        );
        INSTANCE.add(value, tsidBuilder);
        return tsidBuilder;
    }

    @Override
    public void add(DataPointGroupingContext.DataPointGroup value, TsidBuilder tsidBuilder) {
        tsidBuilder.addStringDimension("_metric_names_hash", value.getMetricNamesHash());
        tsidBuilder.addAll(value.dataPointGroupTsidBuilder());
        tsidBuilder.addAll(value.resourceTsidBuilder());
        tsidBuilder.addAll(value.scopeTsidBuilder());
    }
}
