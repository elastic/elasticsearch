/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.diskbbq;

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.LeafReader;

/**
 * Test-only {@link IvfQueryConfigResolver} that returns a fixed {@link IvfSegmentConfig} on every leaf.
 */
public class TestIvfQueryConfigResolver extends IvfQueryConfigResolver {

    private final IvfSegmentConfig config;

    public TestIvfQueryConfigResolver(
        CentroidIndexFormat centroidIndexFormat,
        QuantEncoding encoding,
        boolean usePrecondition,
        float rescoreOversample
    ) {
        this(centroidIndexFormat, encoding, usePrecondition, rescoreOversample, rescoreOversample, false);
    }

    /**
     * Variant where what configuration declares and what a segment resolves to differ, as they do under
     * auto-calibration: {@code declaredRescoreOversample()} reports {@code declaredOversample} while every leaf
     * resolves to {@code segmentOversample}.
     */
    public TestIvfQueryConfigResolver(
        CentroidIndexFormat centroidIndexFormat,
        QuantEncoding encoding,
        boolean usePrecondition,
        float declaredOversample,
        float segmentOversample,
        boolean autoCalibrate
    ) {
        super(autoCalibrate, false, 4, declaredOversample, null);
        this.config = IvfSegmentConfig.of(
            centroidIndexFormat,
            new IvfSegmentConfig.OsqConfig(encoding),
            usePrecondition,
            segmentOversample
        );
    }

    @Override
    public IvfSegmentConfig resolve(FieldInfo fieldInfo, LeafReader leafReader) {
        return config;
    }
}
