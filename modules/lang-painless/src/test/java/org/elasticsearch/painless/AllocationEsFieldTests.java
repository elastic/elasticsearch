/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.painless;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.painless.spi.WhitelistLoader;

/**
 * Tests for the Elasticsearch field-access allocation annotations in {@code org.elasticsearch.txt}: the constant
 * {@code GeoPoint} constructors (exercised end to end) and the estimators for {@code BytesRef.utf8ToString} and
 * {@code GeoPoints.getLats/getLons} (whose receivers come from doc values and so are exercised directly — the whitelist
 * loading in the end-to-end tests confirms their annotations resolve).
 */
public class AllocationEsFieldTests extends AllocationTestCase {

    public void testGeoPointConstructorCharged() {
        assertEquals(32L, allocatedBytes("new GeoPoint(1.0, 2.0); return \"x\";"));
    }

    public void testGeoPointConstructorTripsLimit() {
        assertTripsLimit("new GeoPoint(1.0, 2.0); return \"x\";");
    }

    public void testUtf8ToStringEstimator() {
        // A BytesRef of N UTF-8 bytes yields a String of at most N chars: overhead (32) + 2 bytes per char.
        assertEquals(32L + 2L * 5, AllocationEstimators.utf8ToStringBytes(new BytesRef("hello")));
        assertEquals(32L, AllocationEstimators.utf8ToStringBytes(new BytesRef("")));
        assertEquals(32L, AllocationEstimators.utf8ToStringBytes(null));
    }

    public void testContextWhitelistsWithConstantAnnotationsParse() {
        // StatsSummary (score) and sha1/256/512 (ingest/reindex/update/update_by_query) live in context whitelists that the
        // base test context does not load, so parse them directly to validate their new @allocates annotations.
        WhitelistLoader.loadFromResourceFiles(
            PainlessPlugin.class,
            "org.elasticsearch.script.score.txt",
            "org.elasticsearch.script.ingest.txt",
            "org.elasticsearch.script.reindex.txt",
            "org.elasticsearch.script.update.txt",
            "org.elasticsearch.script.update_by_query.txt"
        );
    }
}
