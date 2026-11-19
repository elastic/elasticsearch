/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.fixtures.oldelasticsearch;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Verifies the version boundary used to decide whether an old Elasticsearch fixture can run on
 * aarch64. Elasticsearch first published native {@code linux-aarch64} distributions in 7.8.0, so
 * everything below that must be skipped on aarch64 (see {@link OldElasticsearchContainer#start()}).
 */
public class OldElasticsearchContainerTests {

    @Test
    public void testPre78VersionsHaveNoNativeAarch64Distribution() {
        for (String version : new String[] { "0.90.13", "1.7.6", "2.4.5", "5.0.0", "5.6.16", "6.0.0", "6.8.20", "7.7.1" }) {
            assertFalse(version, OldElasticsearchContainer.hasNativeAarch64Distribution(version));
        }
    }

    @Test
    public void testVersionsFrom78OnwardsHaveNativeAarch64Distribution() {
        for (String version : new String[] { "7.8.0", "7.9.3", "7.10.0", "8.0.0", "9.2.0" }) {
            assertTrue(version, OldElasticsearchContainer.hasNativeAarch64Distribution(version));
        }
    }
}
