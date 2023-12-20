/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.fieldcaps;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.StringLiteralDeduplicator;
import org.elasticsearch.index.mapper.TimeSeriesParams;

import java.io.IOException;
import java.util.Map;

/**
 * Describes the capabilities of a field in a single index.
 * @param name           The name of the field.
 * @param type           The type associated with the field.
 * @param isSearchable   Whether this field is indexed for search.
 * @param isAggregatable Whether this field can be aggregated on.
 * @param meta           Metadata about the field.
 */

public record IndexFieldCapabilities(
    String name,
    String type,
    boolean isMetadatafield,
    boolean isSearchable,
    boolean isAggregatable,

    boolean hasValue,
    boolean isDimension,
    TimeSeriesParams.MetricType metricType,
    Map<String, String> meta
) implements Writeable {

    private static final StringLiteralDeduplicator typeStringDeduplicator = new StringLiteralDeduplicator();

    public static IndexFieldCapabilities readFrom(StreamInput in) throws IOException {
        String name = in.readString();
        String type = typeStringDeduplicator.deduplicate(in.readString());
        boolean isMetadatafield = in.readBoolean();
        boolean isSearchable = in.readBoolean();
        boolean isAggregatable = in.readBoolean();
        boolean hasValue = false;
        boolean isDimension;
        TimeSeriesParams.MetricType metricType;
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_0_0)) {
            isDimension = in.readBoolean();
            metricType = in.readOptionalEnum(TimeSeriesParams.MetricType.class);
        } else {
            isDimension = false;
            metricType = null;
        }
        if (in.getTransportVersion().onOrAfter(TransportVersions.FIELD_CAPS_FIELD_HAS_VALUE)) {
            hasValue = in.readBoolean();
        }
        return new IndexFieldCapabilities(
            name,
            type,
            isMetadatafield,
            isSearchable,
            isAggregatable,
            hasValue,
            isDimension,
            metricType,
            in.readImmutableMap(StreamInput::readString)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeString(type);
        out.writeBoolean(isMetadatafield);
        out.writeBoolean(isSearchable);
        out.writeBoolean(isAggregatable);
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_0_0)) {
            out.writeBoolean(isDimension);
            out.writeOptionalEnum(metricType);
        }
        out.writeMap(meta, StreamOutput::writeString);
        if (out.getTransportVersion().onOrAfter(TransportVersions.FIELD_CAPS_FIELD_HAS_VALUE)) {
            out.writeBoolean(hasValue);
        }
    }

}
