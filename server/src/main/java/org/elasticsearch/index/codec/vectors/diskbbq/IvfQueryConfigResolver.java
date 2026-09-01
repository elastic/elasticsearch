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
 * Resolves a single {@link IvfSegmentConfig} per leaf at query time: persisted calibration when
 * {@code auto_calibrate} is enabled, else mapping defaults, with query-time oversample override.
 * Preconditioning is the exception — it always follows the segment, see
 * {@link #mappingDefaults(FieldInfo, CalibrationAwareReader)}.
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

    public IvfSegmentConfig resolve(FieldInfo fieldInfo, LeafReader leafReader) throws IOException {
        CalibrationAwareReader reader = calibrationAwareReader(fieldInfo, leafReader);
        IvfSegmentConfig raw = autoCalibrate ? resolveCalibrated(fieldInfo, reader) : mappingDefaults(fieldInfo, reader);
        return IvfSegmentConfig.withEffectiveRescoreOversample(raw, queryOversample, mappingRescoreOversample);
    }

    /**
     * Mapping-driven config, except for preconditioning. Preconditioning is a physical property of the
     * segment — a preconditioned segment stores transformed vectors and can only be scored with a
     * transformed query — so it is read back from the segment whenever the reader exposes it, even with
     * {@code auto_calibrate} disabled. Otherwise segments that merge-calibration preconditioned while the
     * setting was on would be scored with an untransformed query once the mapping turns it off.
     */
    private IvfSegmentConfig mappingDefaults(FieldInfo fieldInfo, @Nullable CalibrationAwareReader reader) {
        return new IvfSegmentConfig(
            CentroidIndexFormat.FLAT,
            new IvfSegmentConfig.OsqConfig(QuantEncoding.fromBits((byte) quantBits)),
            reader == null ? mappingUsePrecondition : reader.shouldPrecondition(fieldInfo),
            Float.NaN
        );
    }

    private IvfSegmentConfig resolveCalibrated(FieldInfo fieldInfo, @Nullable CalibrationAwareReader reader) {
        if (reader == null) {
            return mappingDefaults(fieldInfo, null);
        }
        QuantEncoding quantEncoding = reader.getQuantEncoding(fieldInfo);
        if (quantEncoding == null) {
            return mappingDefaults(fieldInfo, reader);
        }
        return new IvfSegmentConfig(
            CentroidIndexFormat.FLAT,
            new IvfSegmentConfig.OsqConfig(quantEncoding),
            reader.shouldPrecondition(fieldInfo),
            reader.getOversampleFactor(fieldInfo)
        );
    }

    @Nullable
    private static CalibrationAwareReader calibrationAwareReader(FieldInfo fieldInfo, LeafReader leafReader) {
        SegmentReader segmentReader = Lucene.tryUnwrapSegmentReader(leafReader);
        if (segmentReader == null) {
            return null;
        }
        KnnVectorsReader vectorsReader = segmentReader.getVectorReader();
        if (vectorsReader instanceof PerFieldKnnVectorsFormat.FieldsReader perField) {
            vectorsReader = perField.getFieldReader(fieldInfo.name);
        }
        return vectorsReader instanceof CalibrationAwareReader calibrationAwareReader ? calibrationAwareReader : null;
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
