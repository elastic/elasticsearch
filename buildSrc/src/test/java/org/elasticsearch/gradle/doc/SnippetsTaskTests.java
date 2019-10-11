package org.elasticsearch.gradle.doc;

import org.elasticsearch.gradle.test.GradleUnitTestCase;

public class SnippetsTaskTests extends GradleUnitTestCase {

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
