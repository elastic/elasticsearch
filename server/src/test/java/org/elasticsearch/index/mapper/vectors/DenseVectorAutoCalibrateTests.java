/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.vectors;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.codec.vectors.diskbbq.IvfAutoCalibrationProfile;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.index.IndexVersionUtils;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.junit.BeforeClass;

import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;

public class DenseVectorAutoCalibrateTests extends ESTestCase {
    private static final String FIELD = "field";
    private static final Predicate<NodeFeature> FEATURE_ON = f -> f == DenseVectorAutoCalibrate.AUTO_CALIBRATE_PROFILES;
    private static final Predicate<NodeFeature> FEATURE_OFF = f -> false;
    private static final Predicate<NodeFeature> FEATURE_UNCHECKED = f -> {
        throw new AssertionError("feature [" + f + "] should not be checked");
    };

    private static final int OLD_INDEX_VERSION_COUNT = 20;
    private static final IndexVersion[] INDEX_VERSIONS = new IndexVersion[OLD_INDEX_VERSION_COUNT + 1];

    @BeforeClass
    public static void setupIndexVersions() {
        INDEX_VERSIONS[0] = IndexVersion.current(); // Always test with the current index version
        for (int i = 0; i < OLD_INDEX_VERSION_COUNT; i++) {
            INDEX_VERSIONS[i + 1] = IndexVersionUtils.randomVersion();
        }
    }

    public void testParseNull() {
        for (IndexVersion version : INDEX_VERSIONS) {
            assertSame(
                versionMessage(version),
                DenseVectorAutoCalibrate.DEFAULT,
                DenseVectorAutoCalibrate.parse(null, version, FEATURE_UNCHECKED, FIELD)
            );
        }
    }

    public void testParseBooleans() {
        for (IndexVersion version : INDEX_VERSIONS) {
            for (Object node : List.of(Boolean.TRUE, Boolean.FALSE, "true", "false")) {
                boolean expectedEnabled = Booleans.parseBoolean(node.toString());
                IvfAutoCalibrationProfile expectedProfile = expectedEnabled
                    ? DenseVectorAutoCalibrate.defaultEnabledProfile(version)
                    : IvfAutoCalibrationProfile.DISABLED;

                DenseVectorAutoCalibrate autoCalibrate = DenseVectorAutoCalibrate.parse(node, version, FEATURE_UNCHECKED, FIELD);
                assertEquals(versionMessage(version), expectedEnabled, autoCalibrate.enabled());
                assertEquals(versionMessage(version), expectedProfile, autoCalibrate.profile());
                assertSame(versionMessage(version), node, autoCalibrate.originalValue());
            }
        }
    }

    public void testParseTrueDefaultProfile() {
        IndexVersion isoSizingVersion = IndexVersions.DISK_BBQ_AUTO_CALIBRATE_DEFAULT_ISO_SIZING;
        IndexVersion previousVersion = IndexVersionUtils.getPreviousVersion(isoSizingVersion);
        for (IndexVersion version : List.of(previousVersion, IndexVersionUtils.randomVersionBetween(null, previousVersion))) {
            assertEquals(IvfAutoCalibrationProfile.QUALITY, DenseVectorAutoCalibrate.parse(true, version, FEATURE_ON, FIELD).profile());
        }
        for (IndexVersion version : List.of(isoSizingVersion, IndexVersion.current())) {
            assertEquals(IvfAutoCalibrationProfile.ISO_SIZING, DenseVectorAutoCalibrate.parse(true, version, FEATURE_ON, FIELD).profile());
        }
    }

    public void testParseProfileNames() {
        for (IndexVersion version : INDEX_VERSIONS) {
            for (IvfAutoCalibrationProfile profile : IvfAutoCalibrationProfile.values()) {
                String name = profile.toString();
                DenseVectorAutoCalibrate autoCalibrate = DenseVectorAutoCalibrate.parse(name, version, FEATURE_ON, FIELD);
                assertEquals(versionMessage(version), profile != IvfAutoCalibrationProfile.DISABLED, autoCalibrate.enabled());
                assertEquals(versionMessage(version), profile, autoCalibrate.profile());
                assertSame(versionMessage(version), name, autoCalibrate.originalValue());

                IllegalArgumentException e = expectThrows(
                    IllegalArgumentException.class,
                    versionMessage(version),
                    () -> DenseVectorAutoCalibrate.parse(name, version, FEATURE_OFF, FIELD)
                );
                assertEquals(versionMessage(version), booleanOnlyMessage(), e.getMessage());
            }
        }
    }

    public void testParseInvalidValue() {
        for (IndexVersion version : INDEX_VERSIONS) {
            for (Object node : List.of("bogus", "", "TRUE", "Quality", "ISO_SIZING", 1)) {
                IllegalArgumentException e = expectThrows(
                    IllegalArgumentException.class,
                    versionMessage(version),
                    () -> DenseVectorAutoCalibrate.parse(node, version, FEATURE_ON, FIELD)
                );
                assertEquals(
                    versionMessage(version),
                    "'auto_calibrate' must be a boolean or one of "
                        + Arrays.toString(IvfAutoCalibrationProfile.values())
                        + " for field [field]",
                    e.getMessage()
                );

                e = expectThrows(
                    IllegalArgumentException.class,
                    versionMessage(version),
                    () -> DenseVectorAutoCalibrate.parse(node, version, FEATURE_OFF, FIELD)
                );
                assertEquals(versionMessage(version), booleanOnlyMessage(), e.getMessage());
            }
        }
    }

    public void testToXContent() {
        List<Tuple<Object, String>> cases = List.of(
            Tuple.tuple(null, "{}"),
            Tuple.tuple(true, "{\"auto_calibrate\":true}"),
            Tuple.tuple("true", "{\"auto_calibrate\":\"true\"}"),
            Tuple.tuple(false, "{\"auto_calibrate\":false}"),
            Tuple.tuple("iso_sizing", "{\"auto_calibrate\":\"iso_sizing\"}")
        );

        for (IndexVersion version : INDEX_VERSIONS) {
            for (Tuple<Object, String> testCase : cases) {
                DenseVectorAutoCalibrate autoCalibrate = DenseVectorAutoCalibrate.parse(testCase.v1(), version, FEATURE_ON, FIELD);
                String json = Strings.toString(autoCalibrate);
                assertEquals(versionMessage(version), testCase.v2(), json);

                Object reparsedNode = XContentHelper.convertToMap(JsonXContent.jsonXContent, json, false)
                    .get(DenseVectorAutoCalibrate.NAME);
                assertEquals(
                    versionMessage(version),
                    autoCalibrate,
                    DenseVectorAutoCalibrate.parse(reparsedNode, version, FEATURE_ON, FIELD)
                );
            }
        }
    }

    private static String versionMessage(IndexVersion version) {
        return "index version [" + version + "]";
    }

    private static String booleanOnlyMessage() {
        return "'auto_calibrate' must be a boolean for field [field]";
    }
}
