/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.configcreator;

public class AbstractLogFileStructureTests extends LogConfigCreatorTestCase {

    public void testBestLogstashQuoteFor() {
        assertEquals("\"", AbstractLogFileStructure.bestLogstashQuoteFor("normal"));
        assertEquals("\"", AbstractLogFileStructure.bestLogstashQuoteFor("1"));
        assertEquals("\"", AbstractLogFileStructure.bestLogstashQuoteFor("field with spaces"));
        assertEquals("\"", AbstractLogFileStructure.bestLogstashQuoteFor("field_with_'_in_it"));
        assertEquals("'", AbstractLogFileStructure.bestLogstashQuoteFor("field_with_\"_in_it"));
    }

    public void testMoreLikelyGivenText() {
        assertTrue(AbstractStructuredLogFileStructure.isMoreLikelyTextThanKeyword("the quick brown fox jumped over the lazy dog"));
        assertTrue(AbstractStructuredLogFileStructure.isMoreLikelyTextThanKeyword(randomAlphaOfLengthBetween(257, 10000)));
    }

    public void testMoreLikelyGivenKeyword() {
        assertFalse(AbstractStructuredLogFileStructure.isMoreLikelyTextThanKeyword("1"));
        assertFalse(AbstractStructuredLogFileStructure.isMoreLikelyTextThanKeyword("DEBUG"));
        assertFalse(AbstractStructuredLogFileStructure.isMoreLikelyTextThanKeyword(randomAlphaOfLengthBetween(1, 256)));
    }
}
