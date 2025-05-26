/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.s3;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.test.ESTestCase;

public class RegionFromEndpointGuesserTests extends ESTestCase {
    public void testRegionGuessing() {
        assertRegionGuess("s3.amazonaws.com", "us-east-1");
        assertRegionGuess("s3.eu-west-1.amazonaws.com", "eu-west-1");
        assertRegionGuess("s3.us-west-2.amazonaws.com", "us-west-2");
        assertRegionGuess("s3.ap-southeast-1.amazonaws.com", "ap-southeast-1");
        assertRegionGuess("s3-fips.us-gov-east-1.amazonaws.com", "us-gov-east-1");
        assertRegionGuess("10.0.0.4", null);
        assertRegionGuess("random.endpoint.internal.net", null);
    }

    private static void assertRegionGuess(String endpoint, @Nullable String expectedRegion) {
        assertEquals(endpoint, expectedRegion, RegionFromEndpointGuesser.guessRegion(endpoint));
    }
}
