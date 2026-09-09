/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata.otlp.docbuilder;

import io.opentelemetry.proto.metrics.v1.AggregationTemporality;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.hash.BufferedMurmur3Hasher;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.oteldata.otlp.datapoint.DataPointGroupingContext;
import org.elasticsearch.xpack.oteldata.otlp.datapoint.TargetIndex;
import org.elasticsearch.xpack.oteldata.otlp.proto.BufferedByteStringAccessor;

import java.io.IOException;

/**
 * Keeps the shared dimension fields and TSID construction identical for metric and exemplar documents.
 */
public abstract class OtelTsdbDocumentBuilder extends OTelDocumentBuilder {

    public static final String UNIT_FIELD = "unit";
    public static final String TEMPORALITY_FIELD = "temporality";

    protected final BufferedMurmur3Hasher hasher = new BufferedMurmur3Hasher(0);

    protected OtelTsdbDocumentBuilder(BufferedByteStringAccessor byteStringAccessor) {
        super(byteStringAccessor);
    }

    protected void buildDimensionFields(
        XContentBuilder builder,
        DataPointGroupingContext.DataPointGroup dataPointGroup,
        TargetIndex targetIndex,
        String metricNamesHash
    ) throws IOException {
        // Metric dimensions intentionally skip merging paired *.geo.location.lat/.lon into a [lon, lat] array:
        // The *.geo.location dynamic template doesn't apply because geo_point isn't a supported dimension type.
        // That would mean the merged value would land as a plain [lon, lat] array with no guaranteed element order.
        buildResource(dataPointGroup.resource(), dataPointGroup.resourceSchemaUrl(), builder);
        buildDataStream(builder, targetIndex);
        buildScope(builder, dataPointGroup.scope(), dataPointGroup.scopeSchemaUrl());
        buildAttributes(builder, dataPointGroup.dataPointAttributes(), 0);
        if (Strings.hasLength(dataPointGroup.unit())) {
            builder.field(UNIT_FIELD, dataPointGroup.unit());
        }
        String temporality = temporalityToString(dataPointGroup.temporality());
        if (temporality != null) {
            builder.field(TEMPORALITY_FIELD, temporality);
        }
        builder.field("_metric_names_hash", metricNamesHash);
    }

    protected BytesRef buildTsid(
        DataPointGroupingContext.DataPointGroup dataPointGroup,
        String metricNamesHash,
        IndexVersion indexVersion
    ) {
        return dataPointGroup.buildTsid(metricNamesHash, indexVersion);
    }

    /**
     * Converts an {@link AggregationTemporality} to the string value stored in the temporality dimension field.
     */
    public static @Nullable String temporalityToString(@Nullable AggregationTemporality temporality) {
        if (temporality == null) {
            return null;
        }
        return switch (temporality) {
            case AGGREGATION_TEMPORALITY_CUMULATIVE -> "cumulative";
            case AGGREGATION_TEMPORALITY_DELTA -> "delta";
            default -> null;
        };
    }
}
