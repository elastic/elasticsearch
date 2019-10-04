package org.elasticsearch.gradle.doc

import org.elasticsearch.gradle.test.GradleUnitTestCase

class SnippetsTaskTests extends GradleUnitTestCase {

    void testMatchSource() {
        def (boolean matches, String language, String name) = SnippetsTask.matchSource('[source,console]')
        assertTrue(matches)
        assertEquals('console', language)
        assertNull(name)

        (matches, language, name) = SnippetsTask.matchSource('[source,console,id=snippet-name-1]')
        assertTrue(matches)
        assertEquals('console', language)
        assertEquals('snippet-name-1', name)

        (matches, language, name) = SnippetsTask.matchSource('[source, console, id=snippet-name-1]')
        assertTrue(matches)
        assertEquals('console', language)
        assertEquals('snippet-name-1', name)

        (matches, language, name) = SnippetsTask.matchSource('[source,console,attr=5,id=snippet-name-1,attr2=6]')
        assertTrue(matches)
        assertEquals('console', language)
        assertEquals('snippet-name-1', name)

        (matches, language, name) = SnippetsTask.matchSource('[source,console, attr=5, id=snippet-name-1, attr2=6]')
        assertTrue(matches)
        assertEquals('console', language)
        assertEquals('snippet-name-1', name)

        (matches, language, name) = SnippetsTask.matchSource('["source","console",id="snippet-name-1"]')
        assertTrue(matches)
        assertEquals('console', language)
        assertEquals('snippet-name-1', name)

        (matches, language, name) = SnippetsTask.matchSource('[source,console,id="snippet-name-1"]')
        assertTrue(matches)
        assertEquals('console', language)
        assertEquals('snippet-name-1', name)
    }
}
