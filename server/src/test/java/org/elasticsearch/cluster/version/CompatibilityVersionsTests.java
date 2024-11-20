/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.version;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class CompatibilityVersionsTests extends ESTestCase {

    public void testEmptyVersionsList() {
        assertThat(
            CompatibilityVersions.minimumVersions(List.of()),
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

        List<CompatibilityVersions> versions = List.of(compatibilityVersions1, compatibilityVersions2);

        assertThat(CompatibilityVersions.minimumVersions(versions), equalTo(compatibilityVersions1));
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

        List<CompatibilityVersions> versions = List.of(compatibilityVersions1, compatibilityVersions2, compatibilityVersions3);

        assertThat(
            CompatibilityVersions.minimumVersions(versions),
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
        TransportVersion version1 = TransportVersionUtils.getNextVersion(TransportVersions.MINIMUM_COMPATIBLE, true);
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

        List<CompatibilityVersions> versions = List.of(compatibilityVersions1, compatibilityVersions2);

        assertThat(CompatibilityVersions.minimumVersions(versions), equalTo(new CompatibilityVersions(version1, mappings2)));
    }

    public void testPreventJoinClusterWithUnsupportedTransportVersion() {
        List<TransportVersion> transportVersions = IntStream.range(0, randomIntBetween(2, 10))
            .mapToObj(i -> TransportVersionUtils.randomCompatibleVersion(random()))
            .toList();
        TransportVersion min = Collections.min(transportVersions);
        List<CompatibilityVersions> compatibilityVersions = transportVersions.stream()
            .map(transportVersion -> new CompatibilityVersions(transportVersion, Map.of()))
            .toList();

        // should not throw
        CompatibilityVersions.ensureVersionsCompatibility(
            new CompatibilityVersions(TransportVersionUtils.randomVersionBetween(random(), min, TransportVersion.current()), Map.of()),
            compatibilityVersions
        );

        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> CompatibilityVersions.ensureVersionsCompatibility(
                new CompatibilityVersions(
                    TransportVersionUtils.randomVersionBetween(
                        random(),
                        TransportVersionUtils.getFirstVersion(),
                        TransportVersionUtils.getPreviousVersion(min)
                    ),
                    Map.of()
                ),
                compatibilityVersions
            )
        );
        assertThat(e.getMessage(), containsString("may not join a cluster with minimum version"));
    }

    public void testPreventJoinClusterWithUnsupportedMappingsVersion() {
        List<CompatibilityVersions> compatibilityVersions = IntStream.range(0, randomIntBetween(2, 10))
            .mapToObj(
                i -> new CompatibilityVersions(
                    TransportVersion.current(),
                    Map.of(".system-index", new SystemIndexDescriptor.MappingsVersion(randomIntBetween(2, 10), -1))
                )
            )
            .toList();
        int min = compatibilityVersions.stream()
            .mapToInt(v -> v.systemIndexMappingsVersion().get(".system-index").version())
            .min()
            .orElse(2);

        // should not throw
        CompatibilityVersions.ensureVersionsCompatibility(
            new CompatibilityVersions(
                TransportVersion.current(),
                Map.of(".system-index", new SystemIndexDescriptor.MappingsVersion(min, -1))
            ),
            compatibilityVersions
        );

        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> CompatibilityVersions.ensureVersionsCompatibility(
                new CompatibilityVersions(
                    TransportVersion.current(),
                    Map.of(".system-index", new SystemIndexDescriptor.MappingsVersion(randomIntBetween(1, min - 1), -1))
                ),
                compatibilityVersions
            )
        );
        assertThat(e.getMessage(), containsString("may not join a cluster with minimum system index mappings versions"));
    }
}
