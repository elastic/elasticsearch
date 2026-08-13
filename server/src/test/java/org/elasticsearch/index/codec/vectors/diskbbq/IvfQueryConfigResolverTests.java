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
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.store.Directory;
import org.elasticsearch.index.codec.vectors.diskbbq.next.ESNextRescoreOversampleTestFixture;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

/**
 * Query-time resolution of persisted merge calibration vs mapping defaults, and interaction with
 * {@code bits}, {@code precondition}, and query-time oversample when {@code auto_calibrate} is enabled.
 */
public class IvfQueryConfigResolverTests extends ESTestCase {

    private static final int VPC = 128;
    private static final float MAPPING_OVERSAMPLE = 4f;
    private static final int MAPPING_BITS = 4;

    public void testResolveUsesMappingDefaultsWhenAutoCalibrateDisabled() throws IOException {
        int vectorsPerSegment = 64;
        try (Directory dir = newDirectory()) {
            try (
                DirectoryReader reader = ESNextRescoreOversampleTestFixture.buildTwoCommitsTwoSegments(
                    dir,
                    4,
                    vectorsPerSegment,
                    2f,
                    5f,
                    IvfMergeConfigResolver.useCodecDefault()
                )
            ) {
                LeafReader leaf = reader.leaves().getFirst().reader();
                FieldInfo fieldInfo = leaf.getFieldInfos().fieldInfo(ESNextRescoreOversampleTestFixture.FIELD_NAME);
                IvfQueryConfigResolver resolver = IvfQueryConfigResolver.from(false, true, MAPPING_BITS, MAPPING_OVERSAMPLE, null);
                IvfSegmentConfig resolved = resolver.resolve(fieldInfo, leaf);

                assertThat(resolved.quantEncoding(), equalTo(QuantEncoding.fromBits((byte) MAPPING_BITS)));
                assertTrue(resolved.usePrecondition());
                assertThat(resolved.rescoreOversample(), equalTo(MAPPING_OVERSAMPLE));
            }
        }
    }

    public void testResolveIgnoresPersistedOversampleWhenAutoCalibrateDisabled() throws IOException {
        float persistedOversample = 5f;
        try (Directory dir = newDirectory()) {
            try (
                DirectoryReader reader = ESNextRescoreOversampleTestFixture.buildTwoCommitsTwoSegments(
                    dir,
                    4,
                    64,
                    persistedOversample,
                    persistedOversample,
                    IvfMergeConfigResolver.useCodecDefault()
                )
            ) {
                LeafReader leaf = reader.leaves().getFirst().reader();
                FieldInfo fieldInfo = leaf.getFieldInfos().fieldInfo(ESNextRescoreOversampleTestFixture.FIELD_NAME);
                IvfQueryConfigResolver resolver = IvfQueryConfigResolver.from(false, false, 1, MAPPING_OVERSAMPLE, null);
                IvfSegmentConfig resolved = resolver.resolve(fieldInfo, leaf);

                assertThat(resolved.rescoreOversample(), equalTo(MAPPING_OVERSAMPLE));
            }
        }
    }

    public void testResolveIgnoresPersistedPreconditionWhenAutoCalibrateDisabled() throws IOException {
        try (Directory dir = newDirectory()) {
            try (
                DirectoryReader reader = ESNextRescoreOversampleTestFixture.buildTwoCommitsTwoSegments(
                    dir,
                    4,
                    64,
                    new IvfSegmentConfig(CentroidIndexFormat.FLAT, QuantEncoding.ONE_BIT_4BIT_QUERY, true, MAPPING_OVERSAMPLE),
                    new IvfSegmentConfig(CentroidIndexFormat.FLAT, QuantEncoding.ONE_BIT_4BIT_QUERY, true, MAPPING_OVERSAMPLE),
                    IvfMergeConfigResolver.useCodecDefault()
                )
            ) {
                LeafReader leaf = reader.leaves().getFirst().reader();
                assertTrue(ESNextRescoreOversampleTestFixture.persistedPreconditionOnLeaf(leaf));

                FieldInfo fieldInfo = leaf.getFieldInfos().fieldInfo(ESNextRescoreOversampleTestFixture.FIELD_NAME);
                IvfQueryConfigResolver resolver = IvfQueryConfigResolver.from(false, false, 1, MAPPING_OVERSAMPLE, null);
                IvfSegmentConfig resolved = resolver.resolve(fieldInfo, leaf);

                assertFalse(resolved.usePrecondition());
            }
        }
    }

    public void testResolveUsesPersistedOversamplePerSegmentWhenAutoCalibrateEnabled() throws IOException {
        float oversampleA = 1.25f;
        float oversampleB = 3f;
        try (Directory dir = newDirectory()) {
            try (
                DirectoryReader reader = ESNextRescoreOversampleTestFixture.buildTwoCommitsTwoSegments(
                    dir,
                    4,
                    64,
                    oversampleA,
                    oversampleB,
                    IvfMergeConfigResolver.useCodecDefault()
                )
            ) {
                IvfQueryConfigResolver resolver = IvfQueryConfigResolver.from(true, false, 1, MAPPING_OVERSAMPLE, null);
                assertThat(reader.leaves(), hasSize(2));

                LeafReaderContext leafA = reader.leaves().get(0);
                LeafReaderContext leafB = reader.leaves().get(1);
                FieldInfo fieldInfoA = leafA.reader().getFieldInfos().fieldInfo(ESNextRescoreOversampleTestFixture.FIELD_NAME);
                FieldInfo fieldInfoB = leafB.reader().getFieldInfos().fieldInfo(ESNextRescoreOversampleTestFixture.FIELD_NAME);

                assertThat(resolver.resolve(fieldInfoA, leafA.reader()).rescoreOversample(), equalTo(oversampleA));
                assertThat(resolver.resolve(fieldInfoB, leafB.reader()).rescoreOversample(), equalTo(oversampleB));
            }
        }
    }

    public void testResolveUsesPersistedPreconditionWhenAutoCalibrateEnabled() throws IOException {
        try (Directory dir = newDirectory()) {
            try (
                DirectoryReader reader = ESNextRescoreOversampleTestFixture.buildTwoCommitsTwoSegmentsPreconditioning(
                    dir,
                    4,
                    64,
                    true,
                    false,
                    IvfMergeConfigResolver.useCodecDefault()
                )
            ) {
                IvfQueryConfigResolver resolver = IvfQueryConfigResolver.from(true, false, 1, MAPPING_OVERSAMPLE, null);

                LeafReader preconditionedLeaf = reader.leaves().get(0).reader();
                LeafReader plainLeaf = reader.leaves().get(1).reader();
                FieldInfo preconditionedField = preconditionedLeaf.getFieldInfos().fieldInfo(ESNextRescoreOversampleTestFixture.FIELD_NAME);
                FieldInfo plainField = plainLeaf.getFieldInfos().fieldInfo(ESNextRescoreOversampleTestFixture.FIELD_NAME);

                assertTrue(resolver.resolve(preconditionedField, preconditionedLeaf).usePrecondition());
                assertFalse(resolver.resolve(plainField, plainLeaf).usePrecondition());
            }
        }
    }

    public void testResolveUsesPersistedQuantEncodingWhenAutoCalibrateEnabled() throws IOException {
        QuantEncoding persistedEncoding = QuantEncoding.TWO_BIT_4BIT_QUERY;
        try (Directory dir = newDirectory()) {
            try (
                DirectoryReader reader = ESNextRescoreOversampleTestFixture.buildTwoCommitsTwoSegments(
                    dir,
                    4,
                    64,
                    new IvfSegmentConfig(CentroidIndexFormat.FLAT, persistedEncoding, false, 2f),
                    new IvfSegmentConfig(CentroidIndexFormat.FLAT, persistedEncoding, false, 2f),
                    IvfMergeConfigResolver.useCodecDefault()
                )
            ) {
                LeafReader leaf = reader.leaves().getFirst().reader();
                FieldInfo fieldInfo = leaf.getFieldInfos().fieldInfo(ESNextRescoreOversampleTestFixture.FIELD_NAME);
                IvfQueryConfigResolver resolver = IvfQueryConfigResolver.from(true, false, MAPPING_BITS, MAPPING_OVERSAMPLE, null);
                IvfSegmentConfig resolved = resolver.resolve(fieldInfo, leaf);

                assertThat(resolved.quantEncoding(), equalTo(persistedEncoding));
            }
        }
    }

    public void testResolveUsesMappingBitsWhenAutoCalibrateDisabled() throws IOException {
        QuantEncoding persistedEncoding = QuantEncoding.TWO_BIT_4BIT_QUERY;
        try (Directory dir = newDirectory()) {
            try (
                DirectoryReader reader = ESNextRescoreOversampleTestFixture.buildTwoCommitsTwoSegments(
                    dir,
                    4,
                    64,
                    new IvfSegmentConfig(CentroidIndexFormat.FLAT, persistedEncoding, false, 2f),
                    new IvfSegmentConfig(CentroidIndexFormat.FLAT, persistedEncoding, false, 2f),
                    IvfMergeConfigResolver.useCodecDefault()
                )
            ) {
                LeafReader leaf = reader.leaves().getFirst().reader();
                FieldInfo fieldInfo = leaf.getFieldInfos().fieldInfo(ESNextRescoreOversampleTestFixture.FIELD_NAME);
                IvfQueryConfigResolver resolver = IvfQueryConfigResolver.from(false, false, MAPPING_BITS, MAPPING_OVERSAMPLE, null);
                IvfSegmentConfig resolved = resolver.resolve(fieldInfo, leaf);

                assertThat(resolved.quantEncoding(), equalTo(QuantEncoding.fromBits((byte) MAPPING_BITS)));
            }
        }
    }

    public void testResolveUsesMappingOversampleFallbackWhenPersistedNotFinite() throws IOException {
        try (Directory dir = newDirectory()) {
            try (DirectoryReader reader = ESNextRescoreOversampleTestFixture.buildTwoCommitsCodecDefaults(dir, 4, 64)) {
                LeafReader leaf = reader.leaves().getFirst().reader();
                assertFalse(Float.isFinite(ESNextRescoreOversampleTestFixture.persistedOversampleOnLeaf(leaf)));

                FieldInfo fieldInfo = leaf.getFieldInfos().fieldInfo(ESNextRescoreOversampleTestFixture.FIELD_NAME);
                IvfQueryConfigResolver resolver = IvfQueryConfigResolver.from(true, false, 1, MAPPING_OVERSAMPLE, null);
                IvfSegmentConfig resolved = resolver.resolve(fieldInfo, leaf);

                assertThat(resolved.rescoreOversample(), equalTo(MAPPING_OVERSAMPLE));
            }
        }
    }

    public void testQueryOversampleOverrideWinsOverCalibratedPersisted() throws IOException {
        int vectorsPerSegment = IvfAutoCalibration.MIN_VECTORS_FOR_CALIBRATION / 2 + 100;
        float queryOversample = 7f;
        try (Directory dir = newDirectory()) {
            try (
                DirectoryReader reader = ESNextRescoreOversampleTestFixture.buildForceMergedWithDisagreeingFlushCalibration(
                    dir,
                    8,
                    vectorsPerSegment,
                    VPC
                )
            ) {
                IvfQueryConfigResolver resolver = IvfQueryConfigResolver.from(
                    true,
                    false,
                    MAPPING_BITS,
                    MAPPING_OVERSAMPLE,
                    queryOversample
                );
                for (LeafReaderContext leafCtx : reader.leaves()) {
                    FieldInfo fieldInfo = leafCtx.reader().getFieldInfos().fieldInfo(ESNextRescoreOversampleTestFixture.FIELD_NAME);
                    assertThat(resolver.resolve(fieldInfo, leafCtx.reader()).rescoreOversample(), equalTo(queryOversample));
                }
            }
        }
    }

    public void testQueryOversampleOverrideWinsOverPerSegmentCalibratedValues() throws IOException {
        float oversampleA = 1.5f;
        float oversampleB = 2.5f;
        float queryOversample = 6f;
        try (Directory dir = newDirectory()) {
            try (
                DirectoryReader reader = ESNextRescoreOversampleTestFixture.buildTwoCommitsTwoSegments(
                    dir,
                    4,
                    64,
                    oversampleA,
                    oversampleB,
                    IvfMergeConfigResolver.useCodecDefault()
                )
            ) {
                IvfQueryConfigResolver resolver = IvfQueryConfigResolver.from(true, true, 1, MAPPING_OVERSAMPLE, queryOversample);
                for (LeafReaderContext leafCtx : reader.leaves()) {
                    FieldInfo fieldInfo = leafCtx.reader().getFieldInfos().fieldInfo(ESNextRescoreOversampleTestFixture.FIELD_NAME);
                    assertThat(resolver.resolve(fieldInfo, leafCtx.reader()).rescoreOversample(), equalTo(queryOversample));
                }
            }
        }
    }

    public void testEffectiveRescoreOversamplePriorityOrder() {
        assertThat(IvfSegmentConfig.effectiveRescoreOversample(2f, 7f, 3f), equalTo(7f));
        assertThat(IvfSegmentConfig.effectiveRescoreOversample(2f, null, 3f), equalTo(2f));
        assertThat(IvfSegmentConfig.effectiveRescoreOversample(Float.NaN, null, 3f), equalTo(3f));
    }
}
