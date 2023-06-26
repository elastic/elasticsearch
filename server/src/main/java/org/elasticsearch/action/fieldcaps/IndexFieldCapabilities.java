/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.fieldcaps;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.StringLiteralDeduplicator;
import org.elasticsearch.index.mapper.TimeSeriesParams;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

/**
 * Describes the capabilities of a field in a single index.
 */
public class IndexFieldCapabilities implements Writeable {

    private static final StringLiteralDeduplicator typeStringDeduplicator = new StringLiteralDeduplicator();

    private final String name;
    private final String type;
    private final boolean isMetadatafield;
    private final boolean isSearchable;
    private final boolean isAggregatable;
    private final boolean isDimension;
    private final TimeSeriesParams.MetricType metricType;
    private final Map<String, String> meta;

    /**
     * @param name The name of the field.
     * @param type The type associated with the field.
     * @param isSearchable Whether this field is indexed for search.
     * @param isAggregatable Whether this field can be aggregated on.
     * @param meta Metadata about the field.
     */
    IndexFieldCapabilities(
        String name,
        String type,
        boolean isMetadatafield,
        boolean isSearchable,
        boolean isAggregatable,
        boolean isDimension,
        TimeSeriesParams.MetricType metricType,
        Map<String, String> meta
    ) {
        this.name = name;
        this.type = type;
        this.isMetadatafield = isMetadatafield;
        this.isSearchable = isSearchable;
        this.isAggregatable = isAggregatable;
        this.isDimension = isDimension;
        this.metricType = metricType;
        this.meta = meta;
    }

    IndexFieldCapabilities(StreamInput in) throws IOException {
        this.name = in.readString();
        this.type = typeStringDeduplicator.deduplicate(in.readString());
        this.isMetadatafield = in.readBoolean();
        this.isSearchable = in.readBoolean();
        this.isAggregatable = in.readBoolean();
        if (in.getTransportVersion().onOrAfter(TransportVersion.V_8_0_0)) {
            this.isDimension = in.readBoolean();
            this.metricType = in.readOptionalEnum(TimeSeriesParams.MetricType.class);
        } else {
            this.isDimension = false;
            this.metricType = null;
        }
        this.meta = in.readMap(StreamInput::readString);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeString(type);
        out.writeBoolean(isMetadatafield);
        out.writeBoolean(isSearchable);
        out.writeBoolean(isAggregatable);
        if (out.getTransportVersion().onOrAfter(TransportVersion.V_8_0_0)) {
            out.writeBoolean(isDimension);
            out.writeOptionalEnum(metricType);
        }
        out.writeMap(meta, StreamOutput::writeString, StreamOutput::writeString);
    }

    public String getName() {
        return name;
    }

    public String getType() {
        return type;
    }

    public boolean isMetadatafield() {
        return isMetadatafield;
    }

    public boolean isAggregatable() {
        return isAggregatable;
    }

    public boolean isSearchable() {
        return isSearchable;
    }

    public boolean isDimension() {
        return isDimension;
    }

    public TimeSeriesParams.MetricType getMetricType() {
        return metricType;
    }

    public Map<String, String> meta() {
        return meta;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IndexFieldCapabilities that = (IndexFieldCapabilities) o;
        return isMetadatafield == that.isMetadatafield
            && isSearchable == that.isSearchable
            && isAggregatable == that.isAggregatable
            && isDimension == that.isDimension
            && Objects.equals(metricType, that.metricType)
            && Objects.equals(name, that.name)
            && Objects.equals(type, that.type)
            && Objects.equals(meta, that.meta);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, type, isMetadatafield, isSearchable, isAggregatable, isDimension, metricType, meta);
    }
}
