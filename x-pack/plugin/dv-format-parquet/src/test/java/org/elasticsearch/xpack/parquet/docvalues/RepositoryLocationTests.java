/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.docvalues;

import org.elasticsearch.test.ESTestCase;

public class RepositoryLocationTests extends ESTestCase {

    public void testRoundTrip() {
        RepositoryLocation location = new RepositoryLocation("lake", "events/2026/11/part-0001.parquet");
        assertEquals("repository://lake/events/2026/11/part-0001.parquet", location.toString());
        assertEquals(location, RepositoryLocation.parse(location.toString()));
        assertEquals(java.util.List.of("events", "2026", "11", "part-0001.parquet"), location.segments());
    }

    public void testThePathStaysBelowTheBasePath() {
        for (String path : new String[] {
            "../secret.parquet",
            "events/../../secret",
            "/absolute",
            "events//part",
            "./part",
            "events/",
            "" }) {
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new RepositoryLocation("lake", path));
            assertThat(e.getMessage(), org.hamcrest.Matchers.containsString("invalid path"));
        }
    }

    public void testRejectsAnInvalidRepositoryOrLocation() {
        expectThrows(IllegalArgumentException.class, () -> new RepositoryLocation("", "a"));
        expectThrows(IllegalArgumentException.class, () -> new RepositoryLocation("a/b", "a"));
        expectThrows(IllegalArgumentException.class, () -> RepositoryLocation.parse("file:///a/b"));
        expectThrows(IllegalArgumentException.class, () -> RepositoryLocation.parse("repository://lake"));
    }
}
