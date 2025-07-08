/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.env;

import org.elasticsearch.Version;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;

public class BuildVersionTests extends ESTestCase {
    public void testBuildVersionCurrent() {
        assertThat(BuildVersion.current(), equalTo(BuildVersion.fromVersionId(Version.CURRENT.id())));
    }

    public void testBeforeMinimumCompatibleVersion() {
        BuildVersion beforeMinCompat = BuildVersion.fromVersionId(between(0, Version.CURRENT.minimumCompatibilityVersion().id() - 1));
        BuildVersion afterMinCompat = BuildVersion.fromVersionId(
            between(Version.CURRENT.minimumCompatibilityVersion().id(), Version.CURRENT.id())
        );
        BuildVersion futureVersion = BuildVersion.fromVersionId(between(Version.CURRENT.id() + 1, Version.CURRENT.id() + 1_000_000));

        assertFalse(beforeMinCompat.onOrAfterMinimumCompatible());
        assertTrue(afterMinCompat.onOrAfterMinimumCompatible());
        assertTrue(futureVersion.onOrAfterMinimumCompatible());
    }

    public void testIsFutureVersion() {
        BuildVersion beforeMinCompat = BuildVersion.fromVersionId(between(0, Version.CURRENT.minimumCompatibilityVersion().id() - 1));
        BuildVersion afterMinCompat = BuildVersion.fromVersionId(
            between(Version.CURRENT.minimumCompatibilityVersion().id(), Version.CURRENT.id())
        );
        BuildVersion futureVersion = BuildVersion.fromVersionId(between(Version.CURRENT.id() + 1, Version.CURRENT.id() + 1_000_000));

        assertFalse(beforeMinCompat.isFutureVersion());
        assertFalse(afterMinCompat.isFutureVersion());
        assertTrue(futureVersion.isFutureVersion());
    }

    public void testMinimumCompatibilityVersion() {
        BuildVersion minCompatible = BuildVersion.fromVersionId(Version.CURRENT.minimumCompatibilityVersion().id());
        assertThat(BuildVersion.current().minimumCompatibilityVersion(), equalTo(minCompatible));

        BuildVersion previousCompatible = BuildVersion.fromVersionId(
            Version.CURRENT.minimumCompatibilityVersion().minimumCompatibilityVersion().id()
        );
        assertThat(minCompatible.minimumCompatibilityVersion(), equalTo(previousCompatible));
    }

    public static BuildVersion increment(BuildVersion version) {
        return BuildVersion.fromVersionId(((DefaultBuildVersion) version).version.id() + 1);
    }
}
