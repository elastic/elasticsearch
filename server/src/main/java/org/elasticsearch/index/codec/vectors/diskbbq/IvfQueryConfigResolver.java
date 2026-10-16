/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.diskbbq;

import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.SegmentReader;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.core.Nullable;

import java.io.IOException;
import java.util.Objects;

/**
 * Resolves a single {@link IvfSegmentConfig} per leaf at query time: persisted calibration when available and valid,
 * otherwise mapping defaults, with query-time oversample override.
 */
public class IvfQueryConfigResolver {

    private final boolean autoCalibrate;
    private final boolean mappingUsePrecondition;
    private final int quantBits;
    private final float mappingRescoreOversample;
    private final Float queryOversample;

    public IvfQueryConfigResolver(
        boolean autoCalibrate,
        boolean mappingUsePrecondition,
        int quantBits,
        float mappingRescoreOversample,
        @Nullable Float queryOversample
    ) {
        this.autoCalibrate = autoCalibrate;
        this.mappingUsePrecondition = mappingUsePrecondition;
        this.quantBits = quantBits;
        this.mappingRescoreOversample = mappingRescoreOversample;
        this.queryOversample = queryOversample;
    }

    public static IvfQueryConfigResolver from(
        boolean autoCalibrate,
        boolean mappingUsePrecondition,
        int quantBits,
        float mappingRescoreOversample,
        @Nullable Float queryOversample
    ) {
        return new IvfQueryConfigResolver(autoCalibrate, mappingUsePrecondition, quantBits, mappingRescoreOversample, queryOversample);
    }

    public boolean isAutoCalibrate() {
        return autoCalibrate;
    }

    /**
     * The oversample that configuration alone asks for: the query-time override when there is one, otherwise
     * the mapping default.
     */
    public float declaredRescoreOversample() {
        return queryOversample != null ? queryOversample : mappingRescoreOversample;
    }

    public IvfSegmentConfig resolve(FieldInfo fieldInfo, LeafReader leafReader) throws IOException {
        SegmentCalibrationParameters persisted = readPersisted(fieldInfo, leafReader);
        IvfSegmentConfig raw = switch (persisted) {
            case null -> mappingDefaults(mappingUsePrecondition);
            case SegmentCalibrationParameters.Osq osq when autoCalibrate && osq.calibrated() -> new IvfSegmentConfig(
                CentroidIndexFormat.FLAT,
                new IvfSegmentConfig.OsqConfig(osq.encoding()),
                osq.precondition(),
                osq.oversample()
            );
            case SegmentCalibrationParameters.Osq osq -> mappingDefaults(osq.precondition());
        };
        return IvfSegmentConfig.withEffectiveRescoreOversample(raw, queryOversample, mappingRescoreOversample);
    }

    private IvfSegmentConfig mappingDefaults(boolean usePrecondition) {
        return new IvfSegmentConfig(
            CentroidIndexFormat.FLAT,
            new IvfSegmentConfig.OsqConfig(QuantEncoding.fromBits((byte) quantBits)),
            usePrecondition,
            Float.NaN
        );
    }

    /**
     * Reads what the segment itself recorded for the field, or {@code null} when the segment's reader
     * cannot report it.
     */
    @Nullable
    private static SegmentCalibrationParameters readPersisted(FieldInfo fieldInfo, LeafReader leafReader) {
        SegmentReader segmentReader = Lucene.tryUnwrapSegmentReader(leafReader);
        if (segmentReader == null) {
            return null;
        }
        KnnVectorsReader vectorsReader = segmentReader.getVectorReader();
        if (vectorsReader instanceof PerFieldKnnVectorsFormat.FieldsReader perField) {
            vectorsReader = perField.getFieldReader(fieldInfo.name);
        }
        if (vectorsReader instanceof CalibrationAwareReader calibrationAwareReader) {
            return calibrationAwareReader.getCalibrationParameters(fieldInfo);
        }
        return null;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        IvfQueryConfigResolver that = (IvfQueryConfigResolver) o;
        return autoCalibrate == that.autoCalibrate
            && mappingUsePrecondition == that.mappingUsePrecondition
            && quantBits == that.quantBits
            && mappingRescoreOversample == that.mappingRescoreOversample
            && Objects.equals(queryOversample, that.queryOversample);
    }

    @Override
    public int hashCode() {
        return Objects.hash(autoCalibrate, mappingUsePrecondition, quantBits, mappingRescoreOversample, queryOversample);
    }
}
