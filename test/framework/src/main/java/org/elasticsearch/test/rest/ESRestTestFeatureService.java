/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.rest;

import org.elasticsearch.Version;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
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

    private final Collection<Version> nodeVersions;
    private final Collection<Set<String>> nodeFeatures;

    ESRestTestFeatureService(Set<Version> nodeVersions, Collection<Set<String>> nodeFeatures) {
        this.nodeVersions = nodeVersions;
        this.nodeFeatures = nodeFeatures;
    }

    private static <T> boolean checkCollection(Collection<T> coll, Predicate<T> pred, boolean any) {
        return any ? coll.stream().anyMatch(pred) : coll.isEmpty() == false && coll.stream().allMatch(pred);
    }

    @Override
    public boolean clusterHasFeature(String featureId, boolean any) {
        if (checkCollection(nodeFeatures, s -> s.contains(featureId), any)) {
            return true;
        }
        if (MetadataHolder.FEATURE_NAMES.contains(featureId)) {
            return false; // feature known but not present
        }

        // check synthetic version features (used to migrate from version checks to feature checks)
        Matcher matcher = VERSION_FEATURE_PATTERN.matcher(featureId);
        if (matcher.matches()) {
            Version extractedVersion = Version.fromString(matcher.group(1));
            if (extractedVersion.after(Version.CURRENT)) {
                throw new IllegalArgumentException(
                    Strings.format(
                        "Cannot use a synthetic feature [%s] for a version after the current version [%s]",
                        featureId,
                        Version.CURRENT
                    )
                );
            }

            if (extractedVersion.equals(Version.CURRENT)) {
                throw new IllegalArgumentException(
                    Strings.format(
                        "Cannot use a synthetic feature [%s] for the current version [%s]; "
                            + "please define a test cluster feature alongside the corresponding code change instead",
                        featureId,
                        Version.CURRENT
                    )
                );
            }

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

    public static boolean hasFeatureMetadata() {
        return MetadataHolder.FEATURE_NAMES.isEmpty() == false;
    }

    private static class MetadataHolder {
        private static final Set<String> FEATURE_NAMES;

        static {
            String metadataPath = System.getProperty("tests.features.metadata.path");
            if (metadataPath == null) {
                FEATURE_NAMES = emptySet();
            } else {
                Set<String> featureNames = new HashSet<>();
                loadFeatureMetadata(metadataPath, (key, value) -> {
                    if (key.equals("feature_names") && value instanceof Collection<?> collection) {
                        for (var entry : collection) {
                            featureNames.add((String) entry);
                        }
                    }
                });
                FEATURE_NAMES = Collections.unmodifiableSet(featureNames);
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
