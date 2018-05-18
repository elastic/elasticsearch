/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.configcreator;

import org.elasticsearch.test.ESTestCase;

public class AbstractLogFileStructureTests extends ESTestCase {

    public void testBestLogstashQuoteFor() {
        assertEquals("\"", AbstractLogFileStructure.bestLogstashQuoteFor("normal"));
        assertEquals("\"", AbstractLogFileStructure.bestLogstashQuoteFor("1"));
        assertEquals("\"", AbstractLogFileStructure.bestLogstashQuoteFor("field with spaces"));
        assertEquals("\"", AbstractLogFileStructure.bestLogstashQuoteFor("field_with_'_in_it"));
        assertEquals("'", AbstractLogFileStructure.bestLogstashQuoteFor("field_with_\"_in_it"));
    }
}
