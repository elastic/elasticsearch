/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.logstructurefinder;

public class AbstractLogStructureFinderTests extends LogStructureTestCase {

    public void testMoreLikelyGivenText() {
        assertTrue(AbstractLogStructureFinder.isMoreLikelyTextThanKeyword("the quick brown fox jumped over the lazy dog"));
        assertTrue(AbstractLogStructureFinder.isMoreLikelyTextThanKeyword(randomAlphaOfLengthBetween(257, 10000)));
    }

    public void testMoreLikelyGivenKeyword() {
        assertFalse(AbstractLogStructureFinder.isMoreLikelyTextThanKeyword("1"));
        assertFalse(AbstractLogStructureFinder.isMoreLikelyTextThanKeyword("DEBUG"));
        assertFalse(AbstractLogStructureFinder.isMoreLikelyTextThanKeyword(randomAlphaOfLengthBetween(1, 256)));
    }
}
