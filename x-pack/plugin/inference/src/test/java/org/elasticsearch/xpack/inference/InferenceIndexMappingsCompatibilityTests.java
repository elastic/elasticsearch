/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference;

import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.test.index.IndexVersionUtils;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;

/**
 * Enforces the mixed-version-cluster invariant of the {@code .inference} index mappings.
 *
 * <p>{@link InferenceIndexMappingManager} force-installs this node's <em>latest</em> mappings via an
 * origin-carrying put-mapping request, deliberately bypassing the compatibility downgrade the server
 * applies elsewhere (e.g. {@code TransportCreateIndexAction} selecting
 * {@code getDescriptorCompatibleWith(minVersion)}). In a mixed-version cluster the resulting mappings
 * are published in cluster state to older nodes, which must be able to parse them. That is only safe
 * while every {@code InferenceIndex.mappingsVN()} sticks to mapping constructs that all node versions
 * a rolling upgrade can pair us with already understand.
 *
 * <p>These tests approximate that constraint by parsing every mappings version with the oldest
 * index version the current code still supports for writes ({@link IndexVersions#MINIMUM_COMPATIBLE}),
 * which is older than any node that can share a cluster with this one. A field type or mapping
 * parameter that is gated on a newer index version fails to parse here and flags the mapping bump
 * for a proper compatibility strategy instead of the force-install.
 */
public class InferenceIndexMappingsCompatibilityTests extends MapperServiceTestCase {

    private static final Map<Integer, String> MAPPINGS_BY_VERSION = Map.of(
        1,
        InferenceIndex.mappingsV1(),
        2,
        InferenceIndex.mappingsV2(),
        3,
        InferenceIndex.mappingsV3(),
        4,
        InferenceIndex.mappingsV4()
    );

    /**
     * Fails when a mappings version reachable through the {@code .inference} system index descriptor
     * (the current descriptor or any of its prior descriptors) is not covered by these compatibility
     * tests. Whoever bumps the mappings version must add the new mappings to
     * {@link #MAPPINGS_BY_VERSION} so they are held to the same parseability invariant.
     */
    public void testEveryDescriptorMappingsVersionIsCovered() {
        SystemIndexDescriptor descriptor = InferencePlugin.createInferenceIndexDescriptor(InferenceIndex.settings());
        // SystemIndexDescriptor does not expose its prior descriptors directly; reconstruct the full
        // set of mappings versions by resolving the compatible descriptor at every version up to the
        // current one — each existing version resolves to itself.
        Set<Integer> descriptorVersions = new HashSet<>();
        for (int version = 1; version <= descriptor.getMappingsVersion().version(); version++) {
            SystemIndexDescriptor compatible = descriptor.getDescriptorCompatibleWith(
                new SystemIndexDescriptor.MappingsVersion(version, 0)
            );
            if (compatible != null) {
                descriptorVersions.add(compatible.getMappingsVersion().version());
            }
        }
        assertThat(
            "Every mappings version of the .inference descriptor must be covered by this test's MAPPINGS_BY_VERSION "
                + "(and no stale entries must remain), so that new mappings versions are held to the "
                + "mixed-version parseability invariant",
            MAPPINGS_BY_VERSION.keySet(),
            equalTo(descriptorVersions)
        );
    }

    public void testMappingsParseWithOldestSupportedIndexVersion() {
        assertMappingsParse(IndexVersions.MINIMUM_COMPATIBLE);
    }

    public void testMappingsParseWithRandomCompatibleIndexVersion() {
        assertMappingsParse(IndexVersionUtils.randomCompatibleWriteVersion());
    }

    private void assertMappingsParse(IndexVersion indexVersion) {
        for (var entry : MAPPINGS_BY_VERSION.entrySet()) {
            MapperService mapperService = createMapperService(indexVersion, getIndexSettings(), () -> true);
            try {
                merge(mapperService, entry.getValue());
            } catch (Exception e) {
                throw new AssertionError(
                    "Mappings [v"
                        + entry.getKey()
                        + "] failed to parse with index version ["
                        + indexVersion
                        + "]. The .inference mappings are force-installed on the master regardless of the oldest node "
                        + "in the cluster (see InferenceIndexMappingManager), so every mappings version must remain "
                        + "parseable by all node versions a rolling upgrade can pair this node with.",
                    e
                );
            }
        }
    }
}
