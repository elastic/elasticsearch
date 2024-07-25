/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.rest;

import org.elasticsearch.Version;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.Collections.emptySet;

class ESRestTestFeatureService implements TestFeatureService {

    /**
     * In order to migrate from version checks to cluster feature checks,
     * we provide synthetic features derived from versions, e.g. "gte_v8.0.0".
     */
    private static final Pattern VERSION_FEATURE_PATTERN = Pattern.compile("gte_v(\\d+\\.\\d+\\.\\d+)");

    private final Set<String> knownHistoricalFeatureNames;
    private final Collection<Version> nodeVersions;
    private final Collection<Set<String>> nodeFeatures;
    private final Collection<Set<String>> nodeHistoricalFeatures;

    ESRestTestFeatureService(List<FeatureSpecification> featureSpecs, Set<Version> nodeVersions, Collection<Set<String>> nodeFeatures) {
        List<FeatureSpecification> specs = new ArrayList<>(featureSpecs);
        specs.add(new RestTestLegacyFeatures());
        if (MetadataHolder.HISTORICAL_FEATURES != null) {
            specs.add(MetadataHolder.HISTORICAL_FEATURES);
        }
        FeatureData featureData = FeatureData.createFromSpecifications(specs);
        assert featureData.getNodeFeatures().isEmpty()
            : Strings.format(
                "Only historical features can be injected via ESRestTestCase#additionalTestOnlyHistoricalFeatures(), rejecting %s",
                featureData.getNodeFeatures().keySet()
            );
        this.knownHistoricalFeatureNames = featureData.getHistoricalFeatures().lastEntry().getValue();
        this.nodeVersions = nodeVersions;
        this.nodeFeatures = nodeFeatures;
        this.nodeHistoricalFeatures = nodeVersions.stream()
            .map(featureData.getHistoricalFeatures()::floorEntry)
            .map(Map.Entry::getValue)
            .toList();
    }

    public static boolean hasFeatureMetadata() {
        return MetadataHolder.HISTORICAL_FEATURES != null;
    }

    private static <T> boolean checkCollection(Collection<T> coll, Predicate<T> pred, boolean any) {
        return any ? coll.stream().anyMatch(pred) : coll.isEmpty() == false && coll.stream().allMatch(pred);
    }

    @Override
    public boolean clusterHasFeature(String featureId, boolean any) {
        if (checkCollection(nodeFeatures, s -> s.contains(featureId), any)
            || checkCollection(nodeHistoricalFeatures, s -> s.contains(featureId), any)) {
            return true;
        }
        if (MetadataHolder.FEATURE_NAMES.contains(featureId) || knownHistoricalFeatureNames.contains(featureId)) {
            return false; // feature known but not present
        }

        // check synthetic version features (used to migrate from version checks to feature checks)
        Matcher matcher = VERSION_FEATURE_PATTERN.matcher(featureId);
        if (matcher.matches()) {
            Version extractedVersion = Version.fromString(matcher.group(1));
            return checkCollection(nodeVersions, v -> v.onOrAfter(extractedVersion), any);
        }

        if (hasFeatureMetadata()) {
            throw new IllegalArgumentException(
                Strings.format(
                    "Unknown feature %s: check the respective FeatureSpecification is provided both in module-info.java "
                        + "as well as in META-INF/services and verify the module is loaded during tests.",
                    featureId
                )
            );
        }
        return false;
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
