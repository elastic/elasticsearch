/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.version;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;

import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class CompatibilityVersionsTests extends ESTestCase {

    public void testMinimumVersions() {
        assertThat(
            CompatibilityVersions.minimumVersions(Map.of()),
            equalTo(new CompatibilityVersions(TransportVersion.MINIMUM_COMPATIBLE))
        );

        TransportVersion version1 = TransportVersionUtils.getNextVersion(TransportVersion.MINIMUM_COMPATIBLE, true);
        TransportVersion version2 = TransportVersionUtils.randomVersionBetween(
            random(),
            TransportVersionUtils.getNextVersion(version1, true),
            TransportVersion.current()
        );

        CompatibilityVersions compatibilityVersions1 = new CompatibilityVersions(version1);
        CompatibilityVersions compatibilityVersions2 = new CompatibilityVersions(version2);

        Map<String, CompatibilityVersions> versionsMap = Map.of("node1", compatibilityVersions1, "node2", compatibilityVersions2);

        assertThat(CompatibilityVersions.minimumVersions(versionsMap), equalTo(compatibilityVersions1));
    }
}
