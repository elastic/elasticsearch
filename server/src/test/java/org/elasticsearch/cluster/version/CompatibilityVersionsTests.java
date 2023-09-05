/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.version;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;

import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class CompatibilityVersionsTests extends ESTestCase {

    public void testEmptyVersionsMap() {
        assertThat(
            CompatibilityVersions.minimumVersions(Map.of()),
            equalTo(new CompatibilityVersions(TransportVersions.MINIMUM_COMPATIBLE, Map.of()))
        );
    }

    public void testMinimumTransportVersions() {
        TransportVersion version1 = TransportVersionUtils.getNextVersion(TransportVersions.MINIMUM_COMPATIBLE, true);
        TransportVersion version2 = TransportVersionUtils.randomVersionBetween(
            random(),
            TransportVersionUtils.getNextVersion(version1, true),
            TransportVersion.current()
        );

        CompatibilityVersions compatibilityVersions1 = new CompatibilityVersions(version1, Map.of());
        CompatibilityVersions compatibilityVersions2 = new CompatibilityVersions(version2, Map.of());

        Map<String, CompatibilityVersions> versionsMap = Map.of("node1", compatibilityVersions1, "node2", compatibilityVersions2);

        assertThat(CompatibilityVersions.minimumVersions(versionsMap), equalTo(compatibilityVersions1));
    }

    public void testMinimumMappingsVersions() {
        SystemIndexDescriptor.MappingsVersion v1 = new SystemIndexDescriptor.MappingsVersion(1, 1);
        SystemIndexDescriptor.MappingsVersion v2 = new SystemIndexDescriptor.MappingsVersion(2, 2);
        SystemIndexDescriptor.MappingsVersion v3 = new SystemIndexDescriptor.MappingsVersion(3, 3);
        Map<String, SystemIndexDescriptor.MappingsVersion> mappings1 = Map.of(".system-index-1", v3, ".system-index-2", v1);
        Map<String, SystemIndexDescriptor.MappingsVersion> mappings2 = Map.of(".system-index-1", v2, ".system-index-2", v2);
        Map<String, SystemIndexDescriptor.MappingsVersion> mappings3 = Map.of(".system-index-3", v1);

        CompatibilityVersions compatibilityVersions1 = new CompatibilityVersions(TransportVersion.current(), mappings1);
        CompatibilityVersions compatibilityVersions2 = new CompatibilityVersions(TransportVersion.current(), mappings2);
        CompatibilityVersions compatibilityVersions3 = new CompatibilityVersions(TransportVersion.current(), mappings3);

        Map<String, CompatibilityVersions> versionsMap = Map.of(
            "node1",
            compatibilityVersions1,
            "node2",
            compatibilityVersions2,
            "node3",
            compatibilityVersions3
        );

        assertThat(
            CompatibilityVersions.minimumVersions(versionsMap),
            equalTo(
                new CompatibilityVersions(
                    TransportVersion.current(),
                    Map.of(".system-index-1", v2, ".system-index-2", v1, ".system-index-3", v1)
                )
            )
        );
    }

    /**
     * By design, all versions should increase monotonically through releases, so we shouldn't have a situation
     * where the minimum transport version is in one CompatibilityVersions object and a minimum system
     * index is in another. However, the minimumVersions method we're testing will handle that situation without
     * complaint.
     */
    public void testMinimumsAreMerged() {
        TransportVersion version1 = TransportVersionUtils.getNextVersion(TransportVersion.MINIMUM_COMPATIBLE, true);
        TransportVersion version2 = TransportVersionUtils.randomVersionBetween(
            random(),
            TransportVersionUtils.getNextVersion(version1, true),
            TransportVersion.current()
        );

        SystemIndexDescriptor.MappingsVersion v1 = new SystemIndexDescriptor.MappingsVersion(1, 1);
        SystemIndexDescriptor.MappingsVersion v2 = new SystemIndexDescriptor.MappingsVersion(2, 2);
        Map<String, SystemIndexDescriptor.MappingsVersion> mappings1 = Map.of(".system-index-1", v2);
        Map<String, SystemIndexDescriptor.MappingsVersion> mappings2 = Map.of(".system-index-1", v1);

        CompatibilityVersions compatibilityVersions1 = new CompatibilityVersions(version1, mappings1);
        CompatibilityVersions compatibilityVersions2 = new CompatibilityVersions(version2, mappings2);

        Map<String, CompatibilityVersions> versionsMap = Map.of("node1", compatibilityVersions1, "node2", compatibilityVersions2);

        assertThat(CompatibilityVersions.minimumVersions(versionsMap), equalTo(new CompatibilityVersions(version1, mappings2)));
    }
}
