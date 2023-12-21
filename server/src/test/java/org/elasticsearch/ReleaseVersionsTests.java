/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch;

import org.elasticsearch.test.ESTestCase;

import java.util.Map;
import java.util.NavigableMap;

import static org.hamcrest.Matchers.equalTo;

public class ReleaseVersionsTests extends ESTestCase {

    public void testReadVersions() {
        NavigableMap<Integer, String> versions = ReleaseVersions.readVersionsFile(ReleaseVersionsTests.class);

        assertThat(versions, equalTo(Map.of(10, "8.0.0", 14, "8.1.0", 21, "8.2.0", 22, "8.2.1")));
    }

    public void testFindsReleaseVersion() {
        assertThat(ReleaseVersions.findReleaseVersion(ReleaseVersionsTests.class, 14), equalTo("8.1.0"));
    }

    public void testReturnsRange() {
        assertThat(ReleaseVersions.findReleaseVersion(ReleaseVersionsTests.class, 17), equalTo("8.1.0-8.2.0"));
        assertThat(ReleaseVersions.findReleaseVersion(ReleaseVersionsTests.class, 9), equalTo("<9>-8.0.0"));
        assertThat(ReleaseVersions.findReleaseVersion(ReleaseVersionsTests.class, 24), equalTo("8.2.1-<24>"));
    }

    public void testVersionsFileNotFound() {
        assertThat(ReleaseVersions.findReleaseVersion(ReleaseVersions.class, 100), equalTo("<100>"));
    }
}
