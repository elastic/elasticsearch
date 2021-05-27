/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.search;

import org.elasticsearch.test.ESTestCase;

public class SimpleQueryStringQueryParserTests extends ESTestCase {

    public void testEqualsSettings() {
        SimpleQueryStringQueryParser.Settings settings1 = new SimpleQueryStringQueryParser.Settings();
        SimpleQueryStringQueryParser.Settings settings2 = new SimpleQueryStringQueryParser.Settings();
        String s = "Some random other object";
        assertEquals(settings1, settings1);
        assertEquals(settings1, settings2);
        assertNotEquals(settings1, null);
        assertNotEquals(settings1, s);

        settings2.lenient(settings1.lenient() == false);
        assertNotEquals(settings1, settings2);

        settings2 = new SimpleQueryStringQueryParser.Settings();
        settings2.analyzeWildcard(settings1.analyzeWildcard() == false);
        assertNotEquals(settings1, settings2);

        settings2 = new SimpleQueryStringQueryParser.Settings();
        settings2.quoteFieldSuffix("a");
        assertNotEquals(settings1, settings2);

        settings2 = new SimpleQueryStringQueryParser.Settings();
        settings2.autoGenerateSynonymsPhraseQuery(settings1.autoGenerateSynonymsPhraseQuery() == false);
        assertNotEquals(settings1, settings2);

        settings2 = new SimpleQueryStringQueryParser.Settings();
        settings2.fuzzyPrefixLength(settings1.fuzzyPrefixLength() + 1);
        assertNotEquals(settings1, settings2);

        settings2 = new SimpleQueryStringQueryParser.Settings();
        settings2.fuzzyMaxExpansions(settings1.fuzzyMaxExpansions() + 1);
        assertNotEquals(settings1, settings2);

        settings2 = new SimpleQueryStringQueryParser.Settings();
        settings2.fuzzyTranspositions(settings1.fuzzyTranspositions() == false);
        assertNotEquals(settings1, settings2);
    }
}
