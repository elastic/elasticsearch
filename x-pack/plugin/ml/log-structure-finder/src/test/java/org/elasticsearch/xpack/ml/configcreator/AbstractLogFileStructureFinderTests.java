/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.configcreator;

public class AbstractLogFileStructureFinderTests extends LogFileStructureTestCase {

    public void testMoreLikelyGivenText() {
        assertTrue(AbstractLogFileStructureFinder.isMoreLikelyTextThanKeyword("the quick brown fox jumped over the lazy dog"));
        assertTrue(AbstractLogFileStructureFinder.isMoreLikelyTextThanKeyword(randomAlphaOfLengthBetween(257, 10000)));
    }

    public void testMoreLikelyGivenKeyword() {
        assertFalse(AbstractLogFileStructureFinder.isMoreLikelyTextThanKeyword("1"));
        assertFalse(AbstractLogFileStructureFinder.isMoreLikelyTextThanKeyword("DEBUG"));
        assertFalse(AbstractLogFileStructureFinder.isMoreLikelyTextThanKeyword(randomAlphaOfLengthBetween(1, 256)));
    }
}
