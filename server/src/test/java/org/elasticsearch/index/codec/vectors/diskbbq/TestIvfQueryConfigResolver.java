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
        super(false, false, 4, rescoreOversample, null);
        this.config = new IvfSegmentConfig(centroidIndexFormat, encoding, usePrecondition, rescoreOversample);
    }

    @Override
    public IvfSegmentConfig resolve(FieldInfo fieldInfo, LeafReader leafReader) {
        return config;
    }
}
