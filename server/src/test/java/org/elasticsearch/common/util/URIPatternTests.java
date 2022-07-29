/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.common.util;

import org.elasticsearch.test.ESTestCase;

import java.net.URI;

public class URIPatternTests extends ESTestCase {
    public void testURIPattern() throws Exception {
        assertTrue(new URIPattern("http://test.local/").match(new URI("http://test.local/")));
        assertFalse(new URIPattern("http://test.local/somepath").match(new URI("http://test.local/")));
        assertTrue(new URIPattern("http://test.local/somepath").match(new URI("http://test.local/somepath")));
        assertFalse(new URIPattern("http://test.local/somepath").match(new URI("http://test.local/somepath/more")));
        assertTrue(new URIPattern("http://test.local/somepath/*").match(new URI("http://test.local/somepath/more")));
        assertTrue(new URIPattern("http://test.local/somepath/*").match(new URI("http://test.local/somepath/more/andmore")));
        assertTrue(new URIPattern("http://test.local/somepath/*").match(new URI("http://test.local/somepath/more/andmore/../bitmore")));
        assertFalse(new URIPattern("http://test.local/somepath/*").match(new URI("http://test.local/somepath/../more")));
        assertFalse(new URIPattern("http://test.local/somepath/*").match(new URI("http://test.local/")));
        assertFalse(new URIPattern("http://test.local/somepath/*").match(new URI("https://test.local/somepath/more")));
        assertFalse(new URIPattern("http://test.local:1234/somepath/*").match(new URI("http://test.local/somepath/more")));
        assertFalse(new URIPattern("http://test.local:1234/somepath/*").match(new URI("http://test.local/somepath/more")));
        assertTrue(new URIPattern("http://test.local:1234/somepath/*").match(new URI("http://test.local:1234/somepath/more")));
        assertTrue(new URIPattern("http://*.local:1234/somepath/*").match(new URI("http://foobar.local:1234/somepath/more")));
        assertFalse(new URIPattern("http://*.local:1234/somepath/*").match(new URI("http://foobar.local:2345/somepath/more")));
        assertTrue(new URIPattern("http://*.local:*/somepath/*").match(new URI("http://foobar.local:2345/somepath/more")));
        assertFalse(new URIPattern("http://*.local:*/somepath/*").match(new URI("http://foobar.local:2345/somepath/more?par=val")));
        assertTrue(new URIPattern("http://*.local:*/somepath/*?*").match(new URI("http://foobar.local:2345/somepath/more?par=val")));
        assertFalse(new URIPattern("http://*.local:*/somepath/*?*").match(new URI("http://foobar.local:2345/somepath/more?par=val#frag")));
        assertTrue(new URIPattern("http://*.local:*/somepath/*?*#*").match(new URI("http://foobar.local:2345/somepath/more?par=val#frag")));
        assertTrue(new URIPattern("http://*.local/somepath/*?*#*").match(new URI("http://foobar.local/somepath/more")));
    }
}
