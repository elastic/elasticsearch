/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.rest.yaml.section;

import org.elasticsearch.Version;
import org.elasticsearch.common.VersionId;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.yaml.ClientYamlTestExecutionContext;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;

public class Prerequisites {

    public static final Predicate<ClientYamlTestExecutionContext> TRUE = context -> true;
    public static final Predicate<ClientYamlTestExecutionContext> FALSE = context -> false;

    private Prerequisites() {}

    static Predicate<ClientYamlTestExecutionContext> skipOnVersionRange(String versionRange) {
        final List<VersionRange> versionRanges = VersionRange.parseVersionRanges(versionRange);
        assert versionRanges.isEmpty() == false;
        return context -> {
            // Try to extract the minimum node version. Assume CURRENT if nodes have non-semantic versions
            // TODO: push this logic down to VersionRange.
            // This way will have version parsing only when we actually have to skip on a version, we can remove the default and throw
            // IllegalArgumentException instead (attempting to skip on version where version is not semantic)
            var oldestNodeVersion = context.nodesVersions()
                .stream()
                .map(ESRestTestCase::parseLegacyVersion)
                .flatMap(Optional::stream)
                .min(VersionId::compareTo)
                .orElse(Version.CURRENT);

            return versionRanges.stream().anyMatch(range -> range.contains(oldestNodeVersion));
        };
    }

    static Predicate<ClientYamlTestExecutionContext> skipOnOsList(List<String> operatingSystems) {
        return context -> operatingSystems.stream().anyMatch(osName -> osName.equals(context.os()));
    }

    static Predicate<ClientYamlTestExecutionContext> hasXPack() {
        // TODO: change ESRestTestCase.hasXPack() to be context-specific
        return context -> ESRestTestCase.hasXPack();
    }

    static Predicate<ClientYamlTestExecutionContext> requireClusterFeatures(Set<String> clusterFeatures) {
        return context -> clusterFeatures.stream().allMatch(context::clusterHasFeature);
    }

    static Predicate<ClientYamlTestExecutionContext> skipOnClusterFeatures(Set<String> clusterFeatures) {
        return context -> clusterFeatures.stream().anyMatch(context::clusterHasFeature);
    }
}
