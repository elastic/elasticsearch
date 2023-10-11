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
import org.junit.BeforeClass;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Map.entry;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

public class FeatureServiceTests extends ESTestCase {

    private static List<FeatureEra> ELIDED_ERAS;
    private static List<FeatureEra> PUBLISHABLE_ERAS;

    private static NodeFeature ELIDED_FEATURE;
    private static NodeFeature ELIDED_HISTORICAL_FEATURE;

    @BeforeClass
    public static void findEras() {
        ELIDED_ERAS = Arrays.stream(FeatureEra.values()).filter(e -> e.isPublishable() == false).toList();
        PUBLISHABLE_ERAS = Arrays.stream(FeatureEra.values()).filter(FeatureEra::isPublishable).toList();
        ELIDED_FEATURE = new NodeFeature("elided_v6", elidedEra());
        ELIDED_HISTORICAL_FEATURE = new NodeFeature("elided_hf_v6", elidedEra());
    }

    private static FeatureEra elidedEra() {
        return randomFrom(ELIDED_ERAS);
    }

    private static FeatureEra publishableEra() {
        return randomFrom(PUBLISHABLE_ERAS);
    }

    private static FeatureEra publishableEra(int num) {
        return PUBLISHABLE_ERAS.get(num);
    }

    private static Version randomVersionFor(FeatureEra era) {
        if (era.isPublishable()) {
            return VersionUtils.randomVersionBetween(
                random(),
                Version.fromString(era.era() + ".0.0"),
                VersionUtils.getPreviousVersion(
                    Version.min(Version.fromString((era.era() + 1) + ".0.0"), FeatureService.MAX_HISTORICAL_VERSION_EXCLUSIVE)
                )
            );
        } else {
            // this could reference unreleased Versions, we need to be a bit sneaky here
            return Version.fromId(
                randomIntBetween(Version.fromString(era.era() + ".0.0").id, Version.fromString((era.era() + 1) + ".0.0").id - 100)
            );
        }
    }

    private static Version versionFor(FeatureEra era, String minorString) {
        return Version.fromString(era.era() + "." + minorString);
    }

    private static Map.Entry<NodeFeature, Version> generateHistoricalFeature(String featureId, FeatureEra era) {
        return entry(new NodeFeature(featureId, era), randomVersionFor(era));
    }

    private static Map.Entry<NodeFeature, Version> generateHistoricalFeature(String featureId, FeatureEra era, String minorString) {
        return entry(new NodeFeature(featureId, era), versionFor(era, minorString));
    }

    @Before
    public void loadTestSpecs() {
        FeatureService.resetSpecs();

        FeatureService.registerSpecificationsFrom(List.of(new FeatureSpecification() {
            @Override
            public Set<NodeFeature> getFeatures() {
                return Set.of(new NodeFeature("f1", publishableEra()), new NodeFeature("f2", publishableEra()));
            }
        }, new FeatureSpecification() {
            @Override
            public Set<NodeFeature> getFeatures() {
                return Set.of(ELIDED_FEATURE, new NodeFeature("f1_v7", publishableEra()));
            }
        }, new FeatureSpecification() {
            @Override
            public Map<NodeFeature, Version> getHistoricalFeatures() {
                return Map.ofEntries(
                    generateHistoricalFeature("hf1", publishableEra(1), "2.0"),
                    generateHistoricalFeature("hf2", publishableEra(1), "4.0"),
                    generateHistoricalFeature("hf3", publishableEra(1), "6.0"),
                    entry(ELIDED_HISTORICAL_FEATURE, randomVersionFor(ELIDED_HISTORICAL_FEATURE.era()))
                );
            }
        }));
    }

    public void testServiceRejectsDuplicates() {
        FeatureSpecification dupSpec = new FeatureSpecification() {
            @Override
            public Set<NodeFeature> getFeatures() {
                return Set.of(new NodeFeature("f1", publishableEra()));
            }
        };
        var iae = expectThrows(IllegalArgumentException.class, () -> FeatureService.registerSpecificationsFrom(List.of(dupSpec)));
        assertThat(iae.getMessage(), containsString("Duplicate feature"));

        FeatureSpecification dupHistoricalSpec = new FeatureSpecification() {
            @Override
            public Map<NodeFeature, Version> getHistoricalFeatures() {
                return Map.ofEntries(generateHistoricalFeature("f1", publishableEra()));
            }
        };
        iae = expectThrows(IllegalArgumentException.class, () -> FeatureService.registerSpecificationsFrom(List.of(dupHistoricalSpec)));
        assertThat(iae.getMessage(), containsString("Duplicate feature"));
    }

    public void testServiceRejectsIncorrectHistoricalEra() {
        FeatureEra era = publishableEra(0);
        Version incorrectVersion = Version.fromString((era.era() + 1) + ".0.0");
        FeatureSpecification dupSpec = new FeatureSpecification() {
            @Override
            public Map<NodeFeature, Version> getHistoricalFeatures() {
                return Map.of(new NodeFeature("hf1_era", era), incorrectVersion);
            }
        };
        var iae = expectThrows(IllegalArgumentException.class, () -> FeatureService.registerSpecificationsFrom(List.of(dupSpec)));
        assertThat(iae.getMessage(), containsString("incorrect feature era"));
    }

    public void testHistoricalFeaturesLoaded() {
        Version hf3Version = versionFor(publishableEra(1), "6.0");
        Version hf2Version = versionFor(publishableEra(1), "4.0");
        Version hf1Version = versionFor(publishableEra(1), "2.0");
        Version emptyVersion = versionFor(publishableEra(1), "0.0");
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

    public void testElidedFeatureAccepted() {
        assertThat(new FeatureService().getPublishableFeatures(), not(hasItem("f1_v6")));
        for (int i = 0; i < PUBLISHABLE_ERAS.size(); i++) {
            Version ver = versionFor(publishableEra(i), "0.0");
            assertThat(FeatureService.nodeHasFeature(ver, Set.of(), ELIDED_FEATURE), is(true));
            assertThat(FeatureService.nodeHasFeature(ver, Set.of(), ELIDED_HISTORICAL_FEATURE), is(true));
        }
    }

    public void testLoadingNewSpecsChangesOutputs() {
        // does not contain elided features
        assertThat(FeatureService.readFeatures(), containsInAnyOrder("f1", "f1_v7", "f2"));
        assertThat(FeatureService.readHistoricalFeatures(Version.CURRENT), containsInAnyOrder("hf1", "hf2", "hf3"));

        FeatureService.registerSpecificationsFrom(List.of(new FeatureSpecification() {
            @Override
            public Set<NodeFeature> getFeatures() {
                return Set.of(new NodeFeature("new_feature", FeatureEra.V_8));
            }

            @Override
            public Map<NodeFeature, Version> getHistoricalFeatures() {
                return Map.ofEntries(generateHistoricalFeature("new_hist_feature", FeatureEra.V_7));
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
