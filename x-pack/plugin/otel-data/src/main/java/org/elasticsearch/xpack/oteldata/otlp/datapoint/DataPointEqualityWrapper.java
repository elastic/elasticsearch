/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata.otlp.datapoint;

import java.util.Objects;

/**
 * Map key that groups data points sharing a timestamp, start timestamp and data point TSID dimensions
 * (see {@link org.elasticsearch.xpack.oteldata.otlp.tsid.DataPointTsidFunnel}) within a scope.
 */
public final class DataPointEqualityWrapper extends AttributesEqualityWrapper {

    private final DataPoint dataPoint;
    private final int hashCode;

    DataPointEqualityWrapper(DataPoint dataPoint) {
        super(dataPoint.getAttributes());
        this.dataPoint = dataPoint;
        this.hashCode = computeHashCode();
    }

    @Override
    protected int computeHashCode() {
        int result = Long.hashCode(dataPoint.getTimestampUnixNano());
        result = 31 * result + Long.hashCode(dataPoint.getStartTimestampUnixNano());
        result = 31 * result + Objects.hashCode(dataPoint.getUnit());
        result = 31 * result + Objects.hashCode(dataPoint.getTemporality());
        return 31 * result + super.computeHashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        return o instanceof DataPointEqualityWrapper that
            && hashCode == that.hashCode
            && dataPoint.getTimestampUnixNano() == that.dataPoint.getTimestampUnixNano()
            && dataPoint.getStartTimestampUnixNano() == that.dataPoint.getStartTimestampUnixNano()
            && Objects.equals(dataPoint.getUnit(), that.dataPoint.getUnit())
            && dataPoint.getTemporality() == that.dataPoint.getTemporality()
            && attributesEqual(that);
    }

    @Override
    public int hashCode() {
        return hashCode;
    }
}
