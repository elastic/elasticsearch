/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.features;

import org.elasticsearch.common.Strings;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Reads and consolidate features exposed by a list {@link FeatureSpecification},
 * grouping them together for the consumption of {@link FeatureService}
 */
public class FeatureData {

    private static final Logger Log = LogManager.getLogger(FeatureData.class);
    private static final boolean INCLUDE_TEST_FEATURES = System.getProperty("tests.testfeatures.enabled", "").equals("true");

    static {
        if (INCLUDE_TEST_FEATURES) {
            Log.warn("WARNING: Test features are enabled. This should ONLY be used in automated tests.");
        }
    }

    private final Map<String, NodeFeature> nodeFeatures;

    private FeatureData(Map<String, NodeFeature> nodeFeatures) {
        this.nodeFeatures = nodeFeatures;
    }

    public static FeatureData createFromSpecifications(List<? extends FeatureSpecification> specs) {
        Map<String, FeatureSpecification> allFeatures = new HashMap<>();
        Map<String, NodeFeature> nodeFeatures = new HashMap<>();
        for (FeatureSpecification spec : specs) {
            Set<NodeFeature> specFeatures = spec.getFeatures();
            if (INCLUDE_TEST_FEATURES) {
                specFeatures = new HashSet<>(specFeatures);
                specFeatures.addAll(spec.getTestFeatures());
            }

            for (NodeFeature f : specFeatures) {
                FeatureSpecification existing = allFeatures.putIfAbsent(f.id(), spec);
                if (existing != null && existing.getClass() != spec.getClass()) {
                    throw new IllegalArgumentException(
                        Strings.format("Duplicate feature - [%s] is declared by both [%s] and [%s]", f.id(), existing, spec)
                    );
                }

                nodeFeatures.put(f.id(), f);
            }
        }

        return new FeatureData(Map.copyOf(nodeFeatures));
    }

    public Map<String, NodeFeature> getNodeFeatures() {
        return nodeFeatures;
    }
}
