/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.fieldcaps;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.StringLiteralDeduplicator;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.TimeSeriesParams;

import java.io.IOException;
import java.util.Map;

/**
 * Describes the capabilities of a field in a single index.
 * @param name              The name of the field.
 * @param type              The type associated with the field.
 * @param isSearchable      Whether this field is indexed for search.
 * @param isAggregatable    Whether this field can be aggregated on.
 * @param inferenceId       The id of the inference endpoint that backs this field, or null if the field is not backed by inference.
 * @param searchInferenceId The id of the inference endpoint used at query time, or null if it matches {@code inferenceId} or the
 *                          field is not backed by inference.
 * @param meta              Metadata about the field.
 */

public record IndexFieldCapabilities(
    String name,
    String type,
    boolean isMetadatafield,
    boolean isSearchable,
    boolean isAggregatable,
    boolean isDimension,
    TimeSeriesParams.MetricType metricType,
    @Nullable String inferenceId,
    @Nullable String searchInferenceId,
    Map<String, String> meta
) implements Writeable {

    private static final StringLiteralDeduplicator typeStringDeduplicator = new StringLiteralDeduplicator();

    static final TransportVersion FIELD_CAPS_INFERENCE_INFO = TransportVersion.fromName("field_caps_inference_info");

    public IndexFieldCapabilities(
        String name,
        String type,
        boolean isMetadatafield,
        boolean isSearchable,
        boolean isAggregatable,
        boolean isDimension,
        TimeSeriesParams.MetricType metricType,
        Map<String, String> meta
    ) {
        this(name, type, isMetadatafield, isSearchable, isAggregatable, isDimension, metricType, null, null, meta);
    }

    public static IndexFieldCapabilities readFrom(StreamInput in) throws IOException {
        String name = in.readString();
        String type = typeStringDeduplicator.deduplicate(in.readString());
        boolean isMetadatafield = in.readBoolean();
        boolean isSearchable = in.readBoolean();
        boolean isAggregatable = in.readBoolean();
        boolean isDimension = in.readBoolean();
        TimeSeriesParams.MetricType metricType = in.readOptionalEnum(TimeSeriesParams.MetricType.class);
        String inferenceId = null;
        String searchInferenceId = null;
        if (in.getTransportVersion().supports(FIELD_CAPS_INFERENCE_INFO)) {
            inferenceId = in.readOptionalString();
            searchInferenceId = in.readOptionalString();
        }
        return new IndexFieldCapabilities(
            name,
            type,
            isMetadatafield,
            isSearchable,
            isAggregatable,
            isDimension,
            metricType,
            inferenceId,
            searchInferenceId,
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
        out.writeBoolean(isDimension);
        out.writeOptionalEnum(metricType);
        if (out.getTransportVersion().supports(FIELD_CAPS_INFERENCE_INFO)) {
            out.writeOptionalString(inferenceId);
            out.writeOptionalString(searchInferenceId);
        }
        out.writeMap(meta, StreamOutput::writeString);
    }

}
