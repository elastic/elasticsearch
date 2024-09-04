/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch;

import org.elasticsearch.test.ESTestCase;

import java.util.function.IntFunction;

import static org.hamcrest.Matchers.equalTo;

public class ReleaseVersionsTests extends ESTestCase {

    public void testReleaseVersions() {
        IntFunction<String> versions = ReleaseVersions.generateVersionsLookup(ReleaseVersionsTests.class, 23);

        assertThat(versions.apply(10), equalTo("8.0.0"));
        assertThat(versions.apply(14), equalTo("8.1.0-8.1.1"));
        assertThat(versions.apply(21), equalTo("8.2.0"));
        assertThat(versions.apply(22), equalTo("8.2.1"));
        assertThat(versions.apply(23), equalTo(Version.CURRENT.toString()));
    }

    public void testReturnsRange() {
        IntFunction<String> versions = ReleaseVersions.generateVersionsLookup(ReleaseVersionsTests.class, 23);

        assertThat(versions.apply(17), equalTo("8.1.2-8.2.0"));
        assertThat(versions.apply(9), equalTo("0.0.0"));
        assertThat(versions.apply(24), equalTo(new Version(Version.CURRENT.id + 100) + "-[24]"));
    }
}
