/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.web;

import org.elasticsearch.test.ESTestCase;

import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasEntry;

public class UriPartsTests extends ESTestCase {

    public void testUriParts() {

        // simple URI
        testUriParsing("http://www.google.com", Map.of("scheme", "http", "domain", "www.google.com", "path", ""));

        // custom port
        testUriParsing("http://www.google.com:88", Map.of("scheme", "http", "domain", "www.google.com", "path", "", "port", 88));

        // file
        testUriParsing(
            "http://www.google.com:88/google.png",
            Map.of("scheme", "http", "domain", "www.google.com", "extension", "png", "path", "/google.png", "port", 88)
        );

        // fragment
        testUriParsing(
            "https://www.google.com:88/foo#bar",
            Map.of("scheme", "https", "domain", "www.google.com", "fragment", "bar", "path", "/foo", "port", 88)
        );

        // path, extension
        testUriParsing(
            "https://www.google.com:88/foo.jpg",
            Map.of("scheme", "https", "domain", "www.google.com", "path", "/foo.jpg", "extension", "jpg", "port", 88)
        );

        // query
        testUriParsing(
            "https://www.google.com:88/foo?key=val",
            Map.of("scheme", "https", "domain", "www.google.com", "path", "/foo", "query", "key=val", "port", 88)
        );

        // user_info
        testUriParsing(
            "https://user:pw@www.google.com:88/foo",
            Map.of(
                "scheme",
                "https",
                "domain",
                "www.google.com",
                "path",
                "/foo",
                "port",
                88,
                "user_info",
                "user:pw",
                "username",
                "user",
                "password",
                "pw"
            )
        );

        // user_info without password
        testUriParsing(
            "https://user:@www.google.com:88/foo",
            Map.of(
                "scheme",
                "https",
                "domain",
                "www.google.com",
                "path",
                "/foo",
                "port",
                88,
                "user_info",
                "user:",
                "username",
                "user",
                "password",
                ""
            )
        );

        // everything!
        testUriParsing(
            "https://user:pw@testing.google.com:8080/foo/bar?foo1=bar1&foo2=bar2#anchorVal",
            Map.of(
                "scheme",
                "https",
                "domain",
                "testing.google.com",
                "fragment",
                "anchorVal",
                "path",
                "/foo/bar",
                "port",
                8080,
                "username",
                "user",
                "password",
                "pw",
                "user_info",
                "user:pw",
                "query",
                "foo1=bar1&foo2=bar2"
            )
        );

        // non-http schemes
        testUriParsing(
            "ftp://ftp.is.co.za/rfc/rfc1808.txt",
            Map.of("scheme", "ftp", "path", "/rfc/rfc1808.txt", "extension", "txt", "domain", "ftp.is.co.za")
        );

        testUriParsing("telnet://192.0.2.16:80/", Map.of("scheme", "telnet", "path", "/", "port", 80, "domain", "192.0.2.16"));

        testUriParsing(
            "ldap://[2001:db8::7]/c=GB?objectClass?one",
            Map.of("scheme", "ldap", "path", "/c=GB", "query", "objectClass?one", "domain", "[2001:db8::7]")
        );

        // keep original
        testUriParsing(
            "http://www.google.com:88/foo#bar",
            Map.of("scheme", "http", "domain", "www.google.com", "fragment", "bar", "path", "/foo", "port", 88)
        );

        // remove if successful
        testUriParsing(
            "http://www.google.com:88/foo#bar",
            Map.of("scheme", "http", "domain", "www.google.com", "fragment", "bar", "path", "/foo", "port", 88)
        );
    }

    public void testUrlWithCharactersNotToleratedByUri() throws Exception {
        testUriParsing(
            "http://www.google.com/path with spaces",
            Map.of("scheme", "http", "domain", "www.google.com", "path", "/path with spaces")
        );

        testUriParsing(
            "https://user:pw@testing.google.com:8080/foo with space/bar?foo1=bar1&foo2=bar2#anchorVal",
            Map.of(
                "scheme",
                "https",
                "domain",
                "testing.google.com",
                "fragment",
                "anchorVal",
                "path",
                "/foo with space/bar",
                "port",
                8080,
                "username",
                "user",
                "password",
                "pw",
                "user_info",
                "user:pw",
                "query",
                "foo1=bar1&foo2=bar2"
            )
        );
    }

    public void testDotPathWithoutExtension() throws Exception {
        testUriParsing(
            "https://www.google.com/path.withdot/filenamewithoutextension",
            Map.of("scheme", "https", "domain", "www.google.com", "path", "/path.withdot/filenamewithoutextension")
        );
    }

    public void testDotPathWithExtension() throws Exception {
        testUriParsing(
            "https://www.google.com/path.withdot/filenamewithextension.txt",
            Map.of("scheme", "https", "domain", "www.google.com", "path", "/path.withdot/filenamewithextension.txt", "extension", "txt")
        );
    }

    /**
     * This test verifies that we return an empty extension instead of <code>null</code> if the URI ends with a period. This is probably
     * not behaviour we necessarily want to keep forever, but this test ensures that we're conscious about changing that behaviour.
     */
    public void testEmptyExtension() throws Exception {
        testUriParsing(
            "https://www.google.com/foo/bar.",
            Map.of("scheme", "https", "domain", "www.google.com", "path", "/foo/bar.", "extension", "")
        );
    }

    public void testInvalidUri() {
        final String uri = "not:\\/_a_valid_uri";
        expectThrows(IllegalArgumentException.class, containsString("unable to parse URI [" + uri + "]"), () -> UriParts.parse(uri));
    }

    public void testNullValue() {
        expectThrows(NullPointerException.class, () -> UriParts.parse(null));
    }

    private void testUriParsing(String uri, Map<String, Object> expectedValues) {

        Map<String, Object> actualValues = UriParts.parse(uri);
        Map<String, Class<?>> expectedTypes = UriParts.getUriPartsTypes();

        for (Map.Entry<String, Object> entry : expectedValues.entrySet()) {
            String partName = entry.getKey();
            assertThat(actualValues, hasEntry(partName, entry.getValue()));

            Class<?> expectedType = expectedTypes.get(partName);
            assertNotNull("No type defined for key '" + partName + "'", expectedType);
            assertTrue("Type mismatch for key '" + partName + "' for URI: " + uri, expectedType.isInstance(actualValues.get(partName)));
        }

        // ensure that every key returned by parse() has a corresponding type definition in getUriPartsTypes()
        for (Map.Entry<String, Object> entry : actualValues.entrySet()) {
            assertTrue(
                "Key '" + entry.getKey() + "' from parsed URI '" + uri + "' is not defined in UriParts.getUriPartsTypes()",
                expectedTypes.containsKey(entry.getKey())
            );
        }
    }
}
