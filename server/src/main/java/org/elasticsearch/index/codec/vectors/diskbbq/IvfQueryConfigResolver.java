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
import org.elasticsearch.index.codec.vectors.diskbbq.next.ESNextDiskBBQVectorsFormat;

import java.io.IOException;
import java.util.Objects;

/**
 * Resolves a single {@link IvfSegmentConfig} per leaf at query time: persisted calibration when enabled,
 * else mapping defaults, with query-time oversample override.
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
        IvfSegmentConfig raw = autoCalibrate ? resolveCalibrated(fieldInfo, leafReader) : mappingDefaults();
        return IvfSegmentConfig.withEffectiveRescoreOversample(raw, queryOversample, mappingRescoreOversample);
    }

    private IvfSegmentConfig mappingDefaults() {
        return new IvfSegmentConfig(ESNextDiskBBQVectorsFormat.QuantEncoding.fromBits((byte) quantBits), mappingUsePrecondition, Float.NaN);
    }

    private IvfSegmentConfig resolveCalibrated(FieldInfo fieldInfo, LeafReader leafReader) throws IOException {
        SegmentReader segmentReader = Lucene.tryUnwrapSegmentReader(leafReader);
        if (segmentReader == null) {
            return mappingDefaults();
        }
        KnnVectorsReader vectorsReader = segmentReader.getVectorReader();
        if (vectorsReader instanceof PerFieldKnnVectorsFormat.FieldsReader perField) {
            vectorsReader = perField.getFieldReader(fieldInfo.name);
        }
        if (vectorsReader instanceof CalibrationAwareReader calibrationAwareReader) {
            ESNextDiskBBQVectorsFormat.QuantEncoding quantEncoding = calibrationAwareReader.getQuantEncoding(fieldInfo);
            if (quantEncoding == null) {
                return mappingDefaults();
            }
            float oversampleFactor = calibrationAwareReader.getOversampleFactor(fieldInfo);
            boolean precondition = calibrationAwareReader.shouldPrecondition(fieldInfo);
            return new IvfSegmentConfig(quantEncoding, precondition, oversampleFactor);
        }
        return mappingDefaults();
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
