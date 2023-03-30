/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.gradle.internal.doc;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class SnippetsTaskTests {

    @Test
    public void testMatchSource() {
        SnippetsTask.Source source = SnippetsTask.matchSource("[source,console]");
        assertTrue(source.getMatches());
        assertEquals("console", source.getLanguage());
        assertNull(source.getName());

        source = SnippetsTask.matchSource("[source,console,id=snippet-name-1]");
        assertTrue(source.getMatches());
        assertEquals("console", source.getLanguage());
        assertEquals("snippet-name-1", source.getName());

        source = SnippetsTask.matchSource("[source, console, id=snippet-name-1]");
        assertTrue(source.getMatches());
        assertEquals("console", source.getLanguage());
        assertEquals("snippet-name-1", source.getName());

        source = SnippetsTask.matchSource("[source,console,attr=5,id=snippet-name-1,attr2=6]");
        assertTrue(source.getMatches());
        assertEquals("console", source.getLanguage());
        assertEquals("snippet-name-1", source.getName());

        source = SnippetsTask.matchSource("[source,console, attr=5, id=snippet-name-1, attr2=6]");
        assertTrue(source.getMatches());
        assertEquals("console", source.getLanguage());
        assertEquals("snippet-name-1", source.getName());

        source = SnippetsTask.matchSource("[\"source\",\"console\",id=\"snippet-name-1\"]");
        assertTrue(source.getMatches());
        assertEquals("console", source.getLanguage());
        assertEquals("snippet-name-1", source.getName());

        source = SnippetsTask.matchSource("[source,console,id=\"snippet-name-1\"]");
        assertTrue(source.getMatches());
        assertEquals("console", source.getLanguage());
        assertEquals("snippet-name-1", source.getName());
    }
}
