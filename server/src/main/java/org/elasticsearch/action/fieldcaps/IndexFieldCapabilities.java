/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.fieldcaps;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.StringLiteralDeduplicator;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.TextFieldMapper;
import org.elasticsearch.index.mapper.TimeSeriesParams;

import java.io.IOException;
import java.util.Map;

/**
 * Describes the capabilities of a field in a single index.
 *
 * @param name           The name of the field.
 * @param type           The type associated with the field.
 * @param isSearchable   Whether this field is indexed for search.
 * @param isAggregatable Whether this field can be aggregated on.
 * @param isInference    Whether this field is an inference field.
 * @param meta           Metadata about the field.
 * @param indexAnalyzer  index analyzer name for a text field, or {@code null} when unknown or from an older node
 * @param indexAnalyzerPositionIncrementGap mapping {@code position_increment_gap} when {@code indexAnalyzer} is set;
 *                       the default otherwise, so a missing name does not affect equality
 * @param indexLocalAnalyzer {@code true} when a text field's analyzer name was withheld because it is defined under
 *                       {@code index.analysis}. Never set together with {@code indexAnalyzer}.
 */

public record IndexFieldCapabilities(
    String name,
    String type,
    boolean isMetadatafield,
    boolean isSearchable,
    boolean isAggregatable,
    boolean isInference,
    boolean isDimension,
    TimeSeriesParams.MetricType metricType,
    Map<String, String> meta,
    @Nullable String indexAnalyzer,
    int indexAnalyzerPositionIncrementGap,
    boolean indexLocalAnalyzer
) implements Writeable {

    public IndexFieldCapabilities {
        if (indexAnalyzer == null) {
            indexAnalyzerPositionIncrementGap = TextFieldMapper.Defaults.POSITION_INCREMENT_GAP;
        }
        assert indexAnalyzer == null || indexLocalAnalyzer == false : "a reported analyzer name cannot be index-local";
    }

    private static final StringLiteralDeduplicator typeStringDeduplicator = new StringLiteralDeduplicator();

    public static IndexFieldCapabilities readFrom(StreamInput in) throws IOException {
        String name = in.readString();
        String type = typeStringDeduplicator.deduplicate(in.readString());
        boolean isMetadatafield = in.readBoolean();
        boolean isSearchable = in.readBoolean();
        boolean isAggregatable = in.readBoolean();
        boolean isDimension = in.readBoolean();
        TimeSeriesParams.MetricType metricType = in.readOptionalEnum(TimeSeriesParams.MetricType.class);
        Map<String, String> meta = in.readImmutableMap(StreamInput::readString);
        boolean isInference = in.getTransportVersion().supports(FieldCapabilities.FIELD_CAPS_INFERENCE_FIELD) && in.readBoolean();
        String indexAnalyzer = null;
        int indexAnalyzerPositionIncrementGap = TextFieldMapper.Defaults.POSITION_INCREMENT_GAP;
        boolean indexLocalAnalyzer = false;
        if (in.getTransportVersion().supports(FieldCapabilities.FIELD_CAPS_INDEX_ANALYZER)) {
            indexAnalyzer = in.readOptionalString();
            if (indexAnalyzer != null) {
                indexAnalyzerPositionIncrementGap = in.readVInt();
            } else {
                indexLocalAnalyzer = in.readBoolean();
            }
        }
        return new IndexFieldCapabilities(
            name,
            type,
            isMetadatafield,
            isSearchable,
            isAggregatable,
            isInference,
            isDimension,
            metricType,
            meta,
            indexAnalyzer,
            indexAnalyzerPositionIncrementGap,
            indexLocalAnalyzer
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
        out.writeMap(meta, StreamOutput::writeString);
        if (out.getTransportVersion().supports(FieldCapabilities.FIELD_CAPS_INFERENCE_FIELD)) {
            out.writeBoolean(isInference);
        }
        if (out.getTransportVersion().supports(FieldCapabilities.FIELD_CAPS_INDEX_ANALYZER)) {
            out.writeOptionalString(indexAnalyzer);
            if (indexAnalyzer != null) {
                out.writeVInt(indexAnalyzerPositionIncrementGap);
            } else {
                out.writeBoolean(indexLocalAnalyzer);
            }
        }
    }

}
