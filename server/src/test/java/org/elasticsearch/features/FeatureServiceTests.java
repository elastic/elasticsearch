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
import org.junit.After;
import org.junit.Before;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

public class FeatureServiceTests extends ESTestCase {

    private static final NodeFeature ELIDED_FEATURE = new NodeFeature("elided_v6", FeatureEra.V_6);
    private static final NodeFeature ELIDED_HISTORICAL_FEATURE = new NodeFeature("elided_hf_v6", FeatureEra.V_6);

    @Before
    public void loadTestSpecs() {
        FeatureService.registerSpecificationsFrom(List.of(new FeatureSpecification() {
            @Override
            public Set<NodeFeature> getFeatures() {
                return Set.of(new NodeFeature("f1", FeatureEra.V_8), new NodeFeature("f2", FeatureEra.V_8));
            }
        }, new FeatureSpecification() {
            @Override
            public Set<NodeFeature> getFeatures() {
                return Set.of(ELIDED_FEATURE, new NodeFeature("f1_v7", FeatureEra.V_7));
            }
        }, new FeatureSpecification() {
            @Override
            public Map<NodeFeature, Version> getHistoricalFeatures() {
                return Map.of(
                    new NodeFeature("hf1", FeatureEra.V_8),
                    Version.V_8_2_0,
                    new NodeFeature("hf2", FeatureEra.V_8),
                    Version.V_8_7_0,
                    new NodeFeature("hf3", FeatureEra.V_8),
                    Version.V_8_8_0,
                    ELIDED_HISTORICAL_FEATURE,
                    Version.fromString("6.8.0")
                );
            }
        }));
    }

    public void testServiceRejectsDuplicates() {
        FeatureSpecification dupSpec = new FeatureSpecification() {
            @Override
            public Set<NodeFeature> getFeatures() {
                return Set.of(new NodeFeature("f1", FeatureEra.V_8));
            }
        };
        var iae = expectThrows(IllegalArgumentException.class, () -> FeatureService.registerSpecificationsFrom(List.of(dupSpec)));
        assertThat(iae.getMessage(), containsString("Duplicate feature"));

        FeatureSpecification dupHistoricalSpec = new FeatureSpecification() {
            @Override
            public Map<NodeFeature, Version> getHistoricalFeatures() {
                return Map.of(new NodeFeature("f1", FeatureEra.V_8), Version.V_8_1_0);
            }
        };
        iae = expectThrows(IllegalArgumentException.class, () -> FeatureService.registerSpecificationsFrom(List.of(dupHistoricalSpec)));
        assertThat(iae.getMessage(), containsString("Duplicate feature"));
    }

    public void testServiceRejectsIncorrectHistoricalEra() {
        FeatureSpecification dupSpec = new FeatureSpecification() {
            @Override
            public Map<NodeFeature, Version> getHistoricalFeatures() {
                return Map.of(new NodeFeature("hf1_era", FeatureEra.V_7), Version.V_8_0_0);
            }
        };
        var iae = expectThrows(IllegalArgumentException.class, () -> FeatureService.registerSpecificationsFrom(List.of(dupSpec)));
        assertThat(iae.getMessage(), containsString("incorrect feature era"));
    }

    public void testHistoricalFeaturesLoaded() {
        assertThat(
            FeatureService.readHistoricalFeatures(VersionUtils.randomVersionBetween(random(), Version.V_8_8_0, Version.CURRENT)),
            contains("hf1", "hf2", "hf3")
        );
        assertThat(
            FeatureService.readHistoricalFeatures(
                VersionUtils.randomVersionBetween(random(), Version.V_8_7_0, VersionUtils.getPreviousVersion(Version.V_8_8_0))
            ),
            contains("hf1", "hf2")
        );
        assertThat(
            FeatureService.readHistoricalFeatures(
                VersionUtils.randomVersionBetween(random(), Version.V_8_2_0, VersionUtils.getPreviousVersion(Version.V_8_7_0))
            ),
            contains("hf1")
        );
        assertThat(
            FeatureService.readHistoricalFeatures(
                VersionUtils.randomVersionBetween(random(), Version.V_8_0_0, VersionUtils.getPreviousVersion(Version.V_8_2_0))
            ),
            empty()
        );
    }

    public void testElidedFeatureAccepted() {
        assertThat(new FeatureService().readPublishableFeatures(), not(hasItem("f1_v6")));
        assertThat(FeatureService.nodeHasFeature(Version.V_8_0_0, Set.of(), ELIDED_FEATURE), is(true));
        assertThat(FeatureService.nodeHasFeature(Version.V_8_0_0, Set.of(), ELIDED_HISTORICAL_FEATURE), is(true));
        assertThat(FeatureService.nodeHasFeature(Version.V_7_0_0, Set.of(), ELIDED_FEATURE), is(true));
        assertThat(FeatureService.nodeHasFeature(Version.V_7_0_0, Set.of(), ELIDED_HISTORICAL_FEATURE), is(true));
    }

    public void testLoadingNewSpecsChangesOutputs() {
        assertThat(FeatureService.readFeatures(), contains("f1", "f1_v7", "f2"));
        assertThat(FeatureService.readHistoricalFeatures(Version.CURRENT), contains("hf1", "hf2", "hf3"));

        FeatureService.registerSpecificationsFrom(List.of(new FeatureSpecification() {
            @Override
            public Set<NodeFeature> getFeatures() {
                return Set.of(new NodeFeature("new_feature", FeatureEra.V_8));
            }

            @Override
            public Map<NodeFeature, Version> getHistoricalFeatures() {
                return Map.of(new NodeFeature("new_hist_feature", FeatureEra.V_7), Version.V_7_5_0);
            }
        }));

        assertThat(FeatureService.readFeatures(), contains("f1", "f1_v7", "f2", "new_feature"));
        assertThat(FeatureService.readHistoricalFeatures(Version.CURRENT), contains("hf1", "hf2", "hf3", "new_hist_feature"));
    }

    public void testSpecificFeaturesOverridesSpecs() {
        Set<String> features = Set.of("nf1", "nf2", "nf3");
        assertThat(new FeatureService(features).readPublishableFeatures(), contains(features.toArray()));
    }

    @After
    public void resetSpecs() {
        FeatureService.resetSpecs();
    }
}
