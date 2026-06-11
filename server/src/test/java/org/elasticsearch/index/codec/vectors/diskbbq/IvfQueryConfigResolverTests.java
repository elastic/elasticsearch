/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.diskbbq;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.store.Directory;
import org.elasticsearch.index.codec.vectors.diskbbq.next.ESNextDiskBBQVectorsFormat;
import org.elasticsearch.index.codec.vectors.diskbbq.next.ESNextRescoreOversampleTestFixture;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Random;

import static org.hamcrest.Matchers.equalTo;

/**
 * Query-time resolution of persisted merge calibration vs mapping defaults.
 */
public class IvfQueryConfigResolverTests extends ESTestCase {

    private static final int VPC = 128;

    public void testResolveUsesMappingDefaultsWhenAutoCalibrateDisabled() throws IOException {
        Random rnd = random();
        int vectorsPerSegment = 64;
        try (Directory dir = newDirectory()) {
            try (
                DirectoryReader reader = ESNextRescoreOversampleTestFixture.buildTwoCommitsTwoSegments(
                    dir,
                    rnd,
                    4,
                    vectorsPerSegment,
                    2f,
                    5f,
                    IvfMergeConfigResolver.useCodecDefault()
                )
            ) {
                LeafReader leaf = reader.leaves().getFirst().reader();
                FieldInfo fieldInfo = leaf.getFieldInfos().fieldInfo(ESNextRescoreOversampleTestFixture.FIELD_NAME);
                float mappingOversample = DenseVectorFieldMapper.DEFAULT_OVERSAMPLE;
                IvfQueryConfigResolver resolver = IvfQueryConfigResolver.from(false, true, 4, mappingOversample, null);
                IvfSegmentConfig resolved = resolver.resolve(fieldInfo, leaf);

                assertThat(resolved.quantEncoding(), equalTo(ESNextDiskBBQVectorsFormat.QuantEncoding.fromBits((byte) 4)));
                assertTrue(resolved.usePrecondition());
                assertThat(resolved.rescoreOversample(), equalTo(mappingOversample));
            }
        }
    }

    public void testQueryOversampleOverrideWins() throws IOException {
        Random rnd = random();
        int vectorsPerSegment = IvfAutoCalibration.MIN_VECTORS_FOR_CALIBRATION / 2 + 100;
        try (Directory dir = newDirectory()) {
            try (
                DirectoryReader reader = ESNextRescoreOversampleTestFixture.buildForceMergedWithDisagreeingFlushCalibration(
                    dir,
                    rnd,
                    8,
                    vectorsPerSegment,
                    VPC
                )
            ) {
                LeafReader leaf = reader.leaves().getFirst().reader();
                FieldInfo fieldInfo = leaf.getFieldInfos().fieldInfo(ESNextRescoreOversampleTestFixture.FIELD_NAME);
                IvfQueryConfigResolver resolver = IvfQueryConfigResolver.from(
                    true,
                    false,
                    4,
                    DenseVectorFieldMapper.DEFAULT_OVERSAMPLE,
                    7f
                );
                IvfSegmentConfig resolved = resolver.resolve(fieldInfo, leaf);
                assertThat(resolved.rescoreOversample(), equalTo(7f));
            }
        }
    }
}
