/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class ReleaseVersionsTests extends ESTestCase {

    public void testReleaseVersions() {
        VersionLookup versions = ReleaseVersions.generateVersionsLookup(ReleaseVersionsTests.class);

        assertThat(versions.inferVersion(10), equalTo("8.0.0"));
        assertThat(versions.inferVersion(14), equalTo("8.1.0-8.1.1"));
        assertThat(versions.inferVersion(21), equalTo("8.2.0"));
        assertThat(versions.inferVersion(22), equalTo("8.2.1"));
    }

    public void testReturnsRange() {
        VersionLookup versions = ReleaseVersions.generateVersionsLookup(ReleaseVersionsTests.class);

        assertThat(versions.inferVersion(17), equalTo("8.1.2-8.2.0"));
        assertThat(versions.inferVersion(9), equalTo("0.0.0"));
        assertThat(versions.inferVersion(24), equalTo("8.2.2-snapshot[24]"));
    }

    public void testIdLookup() {
        VersionLookup versions = ReleaseVersions.generateVersionsLookup(ReleaseVersionsTests.class);

        assertThat(versions.findId(Version.fromString("8.0.0")).orElseThrow(AssertionError::new), equalTo(10));
        assertThat(versions.findId(Version.fromString("8.1.2")).isEmpty(), is(true));
    }
}
