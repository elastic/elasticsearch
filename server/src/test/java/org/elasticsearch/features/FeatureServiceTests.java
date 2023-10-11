/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.features;

import org.elasticsearch.Version;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;
import org.junit.Before;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Map.entry;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;

public class FeatureServiceTests extends ESTestCase {

    private static List<Integer> MAJOR_VERSIONS = List.of(7, 8);

    private static int majorVersion() {
        return randomFrom(MAJOR_VERSIONS);
    }

    private static int majorVersion(int num) {
        return MAJOR_VERSIONS.get(num);
    }

    private static Version randomVersionFor(int majorVersion) {
        if (majorVersion >= 7) {
            return VersionUtils.randomVersionBetween(
                random(),
                Version.fromString(majorVersion + ".0.0"),
                VersionUtils.getPreviousVersion(
                    Version.min(Version.fromString((majorVersion + 1) + ".0.0"), FeatureService.MAX_HISTORICAL_VERSION_EXCLUSIVE)
                )
            );
        } else {
            // this could reference unreleased Versions, we need to be a bit sneaky here
            return Version.fromId(
                randomIntBetween(Version.fromString(majorVersion + ".0.0").id, Version.fromString((majorVersion + 1) + ".0.0").id - 100)
            );
        }
    }

    private static Version versionFor(int majorVersion, String minorString) {
        return Version.fromString(majorVersion + "." + minorString);
    }

    private static Map.Entry<NodeFeature, Version> generateHistoricalFeature(String featureId, int majorVersion) {
        return entry(new NodeFeature(featureId), randomVersionFor(majorVersion));
    }

    private static Map.Entry<NodeFeature, Version> generateHistoricalFeature(String featureId, int majorVersion, String minorString) {
        return entry(new NodeFeature(featureId), versionFor(majorVersion, minorString));
    }

    @Before
    public void loadTestSpecs() {
        FeatureService.resetSpecs();

        FeatureService.registerSpecificationsFrom(List.of(new FeatureSpecification() {
            @Override
            public Set<NodeFeature> getFeatures() {
                return Set.of(new NodeFeature("f1"), new NodeFeature("f2"));
            }
        }, new FeatureSpecification() {
            @Override
            public Set<NodeFeature> getFeatures() {
                return Set.of(new NodeFeature("f1_v7"));
            }
        }, new FeatureSpecification() {
            @Override
            public Map<NodeFeature, Version> getHistoricalFeatures() {
                return Map.ofEntries(
                    generateHistoricalFeature("hf1", majorVersion(1), "2.0"),
                    generateHistoricalFeature("hf2", majorVersion(1), "4.0"),
                    generateHistoricalFeature("hf3", majorVersion(1), "6.0")
                );
            }
        }));
    }

    public void testServiceRejectsDuplicates() {
        FeatureSpecification dupSpec = new FeatureSpecification() {
            @Override
            public Set<NodeFeature> getFeatures() {
                return Set.of(new NodeFeature("f1"));
            }
        };
        var iae = expectThrows(IllegalArgumentException.class, () -> FeatureService.registerSpecificationsFrom(List.of(dupSpec)));
        assertThat(iae.getMessage(), containsString("Duplicate feature"));

        FeatureSpecification dupHistoricalSpec = new FeatureSpecification() {
            @Override
            public Map<NodeFeature, Version> getHistoricalFeatures() {
                return Map.ofEntries(generateHistoricalFeature("f1", majorVersion()));
            }
        };
        iae = expectThrows(IllegalArgumentException.class, () -> FeatureService.registerSpecificationsFrom(List.of(dupHistoricalSpec)));
        assertThat(iae.getMessage(), containsString("Duplicate feature"));
    }

    public void testHistoricalFeaturesLoaded() {
        Version hf3Version = versionFor(majorVersion(1), "6.0");
        Version hf2Version = versionFor(majorVersion(1), "4.0");
        Version hf1Version = versionFor(majorVersion(1), "2.0");
        Version emptyVersion = versionFor(majorVersion(1), "0.0");
        assertThat(
            FeatureService.readHistoricalFeatures(VersionUtils.randomVersionBetween(random(), hf3Version, Version.CURRENT)),
            containsInAnyOrder("hf1", "hf2", "hf3")
        );
        assertThat(
            FeatureService.readHistoricalFeatures(
                VersionUtils.randomVersionBetween(random(), hf2Version, VersionUtils.getPreviousVersion(hf3Version))
            ),
            containsInAnyOrder("hf1", "hf2")
        );
        assertThat(
            FeatureService.readHistoricalFeatures(
                VersionUtils.randomVersionBetween(random(), hf1Version, VersionUtils.getPreviousVersion(hf2Version))
            ),
            containsInAnyOrder("hf1")
        );
        assertThat(
            FeatureService.readHistoricalFeatures(
                VersionUtils.randomVersionBetween(random(), emptyVersion, VersionUtils.getPreviousVersion(hf1Version))
            ),
            empty()
        );
    }

    public void testLoadingNewSpecsChangesOutputs() {
        // does not contain elided features
        assertThat(FeatureService.readFeatures(), containsInAnyOrder("f1", "f1_v7", "f2"));
        assertThat(FeatureService.readHistoricalFeatures(Version.CURRENT), containsInAnyOrder("hf1", "hf2", "hf3"));

        FeatureService.registerSpecificationsFrom(List.of(new FeatureSpecification() {
            @Override
            public Set<NodeFeature> getFeatures() {
                return Set.of(new NodeFeature("new_feature"));
            }

            @Override
            public Map<NodeFeature, Version> getHistoricalFeatures() {
                return Map.ofEntries(generateHistoricalFeature("new_hist_feature", 7));
            }
        }));

        assertThat(FeatureService.readFeatures(), containsInAnyOrder("f1", "f1_v7", "f2", "new_feature"));
        assertThat(FeatureService.readHistoricalFeatures(Version.CURRENT), containsInAnyOrder("hf1", "hf2", "hf3", "new_hist_feature"));
    }

    public void testSpecificFeaturesOverridesSpecs() {
        Set<String> features = Set.of("nf1", "nf2", "nf3");
        assertThat(new FeatureService(features).getPublishableFeatures(), containsInAnyOrder(features.toArray()));
    }
}
