/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.common;

import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.TestIngestDocument;
import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasSize;

public class UriPartsProcessorTests extends ESTestCase {

    public void testUriParts() throws Exception {

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
            true,
            false,
            "http://www.google.com:88/foo#bar",
            Map.of("scheme", "http", "domain", "www.google.com", "fragment", "bar", "path", "/foo", "port", 88)
        );

        // remove if successful
        testUriParsing(
            false,
            true,
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

    public void testRemoveIfSuccessfulDoesNotRemoveTargetField() throws Exception {
        String field = "field";
        UriPartsProcessor processor = new UriPartsProcessor(null, null, field, field, true, false, false);

        Map<String, Object> source = new HashMap<>();
        source.put(field, "http://www.google.com");
        IngestDocument input = TestIngestDocument.withDefaultVersion(source);
        IngestDocument output = processor.execute(input);

        Map<String, Object> expectedSourceAndMetadata = new HashMap<>();
        expectedSourceAndMetadata.put(field, Map.of("scheme", "http", "domain", "www.google.com", "path", ""));
        for (Map.Entry<String, Object> entry : expectedSourceAndMetadata.entrySet()) {
            assertThat(output.getSourceAndMetadata(), hasEntry(entry.getKey(), entry.getValue()));
        }
    }

    public void testInvalidUri() {
        String uri = "not:\\/_a_valid_uri";
        UriPartsProcessor processor = new UriPartsProcessor(null, null, "field", "url", true, false, false);

        Map<String, Object> source = new HashMap<>();
        source.put("field", uri);
        IngestDocument input = TestIngestDocument.withDefaultVersion(source);

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> processor.execute(input));
        assertThat(e.getMessage(), containsString("unable to parse URI [" + uri + "]"));
    }

    public void testNullValue() {
        Map<String, Object> source = new HashMap<>();
        source.put("field", null);
        IngestDocument input = TestIngestDocument.withDefaultVersion(source);

        UriPartsProcessor processor = new UriPartsProcessor(null, null, "field", "url", true, false, false);

        expectThrows(NullPointerException.class, () -> processor.execute(input));
    }

    public void testMissingField() {
        Map<String, Object> source = new HashMap<>();
        IngestDocument input = TestIngestDocument.withDefaultVersion(source);

        UriPartsProcessor processor = new UriPartsProcessor(null, null, "field", "url", true, false, false);

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> processor.execute(input));
        assertThat(e.getMessage(), containsString("field [field] not present as part of path [field]"));
    }

    public void testIgnoreMissingField() throws Exception {
        Map<String, Object> source = new HashMap<>();
        // Adding a random field, so we can check the doc is leaved unchanged.
        source.put(randomIdentifier(), randomIdentifier());
        IngestDocument input = TestIngestDocument.withDefaultVersion(source);
        Map<String, Object> expectedSourceAndMetadata = Map.copyOf(input.getSourceAndMetadata());

        UriPartsProcessor processor = new UriPartsProcessor(null, null, "field", "url", true, false, true);
        IngestDocument output = processor.execute(input);

        assertThat(output.getSourceAndMetadata().entrySet(), hasSize(expectedSourceAndMetadata.size()));

        for (Map.Entry<String, Object> entry : expectedSourceAndMetadata.entrySet()) {
            assertThat(output.getSourceAndMetadata(), hasEntry(entry.getKey(), entry.getValue()));
        }
    }

    private void testUriParsing(String uri, Map<String, Object> expectedValues) throws Exception {
        testUriParsing(false, false, uri, expectedValues);
    }

    private void testUriParsing(boolean keepOriginal, boolean removeIfSuccessful, String uri, Map<String, Object> expectedValues)
        throws Exception {
        UriPartsProcessor processor = new UriPartsProcessor(null, null, "field", "url", removeIfSuccessful, keepOriginal, false);

        Map<String, Object> source = new HashMap<>();
        source.put("field", uri);
        IngestDocument input = TestIngestDocument.withDefaultVersion(source);
        IngestDocument output = processor.execute(input);

        Map<String, Object> expectedSourceAndMetadata = new HashMap<>();

        if (removeIfSuccessful == false) {
            expectedSourceAndMetadata.put("field", uri);
        }

        Map<String, Object> values;
        if (keepOriginal) {
            values = new HashMap<>(expectedValues);
            values.put("original", uri);
        } else {
            values = expectedValues;
        }
        expectedSourceAndMetadata.put("url", values);

        for (Map.Entry<String, Object> entry : expectedSourceAndMetadata.entrySet()) {
            assertThat(output.getSourceAndMetadata(), hasEntry(entry.getKey(), entry.getValue()));
        }
    }

}
