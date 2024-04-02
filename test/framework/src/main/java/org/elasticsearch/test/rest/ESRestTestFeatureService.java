/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.rest;

import org.elasticsearch.Version;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.features.FeatureData;
import org.elasticsearch.features.FeatureSpecification;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;

import static java.util.Collections.emptySet;

class ESRestTestFeatureService implements TestFeatureService {
    private final Set<String> allSupportedFeatures;
    private final Set<String> knownHistoricalFeatureNames;

    ESRestTestFeatureService(List<FeatureSpecification> featureSpecs, Collection<Version> nodeVersions, Set<String> clusterStateFeatures) {
        List<FeatureSpecification> specs = new ArrayList<>(featureSpecs);
        specs.add(new RestTestLegacyFeatures());
        if (MetadataHolder.HISTORICAL_FEATURES != null) {
            specs.add(MetadataHolder.HISTORICAL_FEATURES);
        }
        var historicalFeatures = FeatureData.createFromSpecifications(specs).getHistoricalFeatures();
        this.knownHistoricalFeatureNames = historicalFeatures.lastEntry().getValue();
        var minVersion = nodeVersions.stream().min(Comparator.naturalOrder());
        var supportedHistoricalFeatures = minVersion.map(v -> historicalFeatures.floorEntry(v).getValue())
            .orElse(knownHistoricalFeatureNames);
        this.allSupportedFeatures = Sets.union(clusterStateFeatures, supportedHistoricalFeatures);
    }

    public static boolean hasFeatureMetadata() {
        return MetadataHolder.HISTORICAL_FEATURES != null;
    }

    @Override
    public boolean clusterHasFeature(String featureId) {
        if (hasFeatureMetadata()
            && MetadataHolder.FEATURE_NAMES.contains(featureId) == false
            && knownHistoricalFeatureNames.contains(featureId) == false) {
            throw new IllegalArgumentException(
                Strings.format(
                    "Unknown feature %s: check the feature has been added to the correct FeatureSpecification in the relevant module or, "
                        + "if this is a legacy feature used only in tests, to a test-only FeatureSpecification such as %s.",
                    featureId,
                    RestTestLegacyFeatures.class.getCanonicalName()
                )
            );
        }
        return allSupportedFeatures.contains(featureId);
    }

    @Override
    public Set<String> getAllSupportedFeatures() {
        return allSupportedFeatures;
    }

    private static class MetadataHolder {
        private static final FeatureSpecification HISTORICAL_FEATURES;
        private static final Set<String> FEATURE_NAMES;

        static {
            String metadataPath = System.getProperty("tests.features.metadata.path");
            if (metadataPath == null) {
                FEATURE_NAMES = emptySet();
                HISTORICAL_FEATURES = null;
            } else {
                Set<String> featureNames = new HashSet<>();
                Map<NodeFeature, Version> historicalFeatures = new HashMap<>();
                loadFeatureMetadata(metadataPath, (key, value) -> {
                    if (key.equals("historical_features") && value instanceof Map<?, ?> map) {
                        for (var entry : map.entrySet()) {
                            historicalFeatures.put(new NodeFeature((String) entry.getKey()), Version.fromString((String) entry.getValue()));
                        }
                    }
                    if (key.equals("feature_names") && value instanceof Collection<?> collection) {
                        for (var entry : collection) {
                            featureNames.add((String) entry);
                        }
                    }
                });
                FEATURE_NAMES = Collections.unmodifiableSet(featureNames);
                Map<NodeFeature, Version> unmodifiableHistoricalFeatures = Collections.unmodifiableMap(historicalFeatures);
                HISTORICAL_FEATURES = new FeatureSpecification() {
                    @Override
                    public Map<NodeFeature, Version> getHistoricalFeatures() {
                        return unmodifiableHistoricalFeatures;
                    }
                };
            }
        }

        @SuppressForbidden(reason = "File#pathSeparator has not equivalent in java.nio.file")
        private static void loadFeatureMetadata(String metadataPath, BiConsumer<String, Object> consumer) {
            String[] metadataFiles = metadataPath.split(File.pathSeparator);
            for (String metadataFile : metadataFiles) {
                try (
                    InputStream in = Files.newInputStream(PathUtils.get(metadataFile));
                    XContentParser parser = JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, in)
                ) {
                    parser.map().forEach(consumer);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
        }
    }
}
