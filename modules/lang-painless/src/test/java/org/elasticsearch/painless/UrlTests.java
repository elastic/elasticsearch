/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless;

import java.net.MalformedURLException;

public class UrlTests extends ScriptTestCase {
    public void testInvalidURLs() {
        MalformedURLException e = expectScriptThrows(MalformedURLException.class, () -> exec("new URL('abc')"));
        assertEquals("no protocol: abc", e.getMessage());

        e = expectScriptThrows(MalformedURLException.class, () -> exec("new URL('')"));
        assertEquals("no protocol: ", e.getMessage());

        e = expectScriptThrows(MalformedURLException.class, () -> exec("new URL(null)"));
        assertEquals("Cannot invoke \"String.length()\" because \"spec\" is null", e.getMessage());

        e = expectScriptThrows(MalformedURLException.class, () -> exec("new URL('abc://www.test.com')"));
        assertEquals("unknown protocol: abc", e.getMessage());
    }

    public void testGetHost() {
        assertEquals("www.test.com", exec("URL url = new URL('https://www.test.com:8080/path?query#fragment'); url.getHost()"));

        assertEquals("www.test2.org", exec("URL url = new URL('https://www.test2.org'); url.getHost()"));
    }

    public void testGetPath() {
        assertEquals("/path", exec("URL url = new URL('https://www.test.com:8080/path?query#fragment'); url.getPath()"));

        assertEquals("/docs/example", exec("URL url = new URL('https://www.test.com/docs/example'); url.getPath()"));
    }

    public void testGetPort() {
        assertEquals(8080, exec("URL url = new URL('https://www.test.com:8080/path?query#fragment'); url.getPort()"));

        assertEquals(3000, exec("URL url = new URL('https://www.test.com:3000'); url.getPort()"));
    }

    public void testGetProtocol() {
        assertEquals("https", exec("URL url = new URL('https://www.test.com:8080/path?query#fragment'); url.getProtocol()"));

        assertEquals("ftp", exec("URL url = new URL('ftp://www.test.eu'); url.getProtocol()"));
    }

    public void testGetReference() {
        assertEquals("reference", exec("URL url = new URL('https://www.test.com:8080/path?query#reference'); url.getRef()"));

        assertEquals("section1", exec("URL url = new URL('https://www.test.com#section1'); url.getRef()"));
    }
}
