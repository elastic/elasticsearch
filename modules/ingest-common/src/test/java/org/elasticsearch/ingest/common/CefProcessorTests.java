/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.common;

import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.core.PathUtils;
import org.junit.runners.model.TestClass;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static java.util.Map.entry;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;

public class CefProcessorTests extends ESTestCase {
    private static String readCefMessageFile(String fileName) throws IOException, URISyntaxException {
        URL resource = TestClass.class.getResource("/" + fileName);
        return Files.readString(PathUtils.get((Objects.requireNonNull(resource).toURI())));
    }

    private IngestDocument document;

    public void testParse() throws IOException, URISyntaxException {
        String message;
        List<String> headers;
        Map<String, String> extensions;
        {
            message = readCefMessageFile("basic_message.cef.txt");
            headers = CefParser.parseHeaders(message);
            extensions = CefParser.parseExtensions(headers.removeLast());
            assertThat(headers, equalTo(List.of("CEF:0", "vendor", "product", "version", "class", "name", "severity")));
            assertThat(extensions, aMapWithSize(0));
        }
        {
            message = readCefMessageFile("message_with_extension.cef.txt");
            headers = CefParser.parseHeaders(message);
            extensions = CefParser.parseExtensions(headers.removeLast());
            assertThat(headers, equalTo(List.of("CEF:1", "vendor", "product", "version", "class", "name", "severity")));
            assertThat(extensions, equalTo(Map.of("someExtension", "someValue")));
        }
        {
            message = readCefMessageFile("message_with_escaped_pipe.cef.txt");
            headers = CefParser.parseHeaders(message);
            extensions = CefParser.parseExtensions(headers.removeLast());
            assertThat(headers, equalTo(List.of("CEF:1", "vendor", "product|pipe", "version space", "class\\slash", "name", "severity")));
            assertMapsEqual(extensions, Map.ofEntries(entry("ext1", "some value  "), entry("ext2", "pipe|value")));
        }
    }

    public void testExecute() throws IOException, URISyntaxException {
        Map<String, Object> source = new HashMap<>();
        String message = readCefMessageFile("message_execute.cef.txt");
        source.put("message", message);
        document = new IngestDocument("index", "id", 1L, null, null, source);
        CefProcessor processor = new CefProcessor("tag", "description", "message", "cef", false, true, null);
        processor.execute(document);
        assertMapsEqual(
            document.getSource(),
            Map.ofEntries(
                entry(
                    "cef",
                    Map.ofEntries(
                        entry("version", "0"),
                        entry(
                            "device",
                            Map.of("vendor", "Elastic", "product", "Vaporware", "version", "1.0.0-alpha", "event_class_id", "18")
                        ),
                        entry("name", "Web request"),
                        entry("severity", "low")
                    )
                ),
                entry("observer", Map.of("product", "Vaporware", "vendor", "Elastic", "version", "1.0.0-alpha")),
                entry("event", Map.of("id", "3457", "code", "18")),
                entry(
                    "source",
                    Map.ofEntries(
                        entry("ip", "89.160.20.156"),
                        entry("port", 33876),
                        entry("geo", Map.of("location", Map.of("lon", -77.511, "lat", 38.915))),
                        entry("service", Map.of("name", "httpd"))
                    )
                ),
                entry("destination", Map.of("ip", "192.168.10.1", "port", 443)),
                entry("http", Map.of("request", Map.of("method", "POST", "referrer", "https://www.google.com"))),
                entry("network", Map.of("transport", "TCP")),
                entry("url", Map.of("original", "https://www.example.com/cart")),
                entry("message", message)
            )
        );
    }

    public void testInvalidCefFormat() {
        Map<String, Object> invalidSource = new HashMap<>();
        invalidSource.put("message", "Invalid CEF message");
        IngestDocument invalidIngestDocument = new IngestDocument("index", "id", 1L, null, null, invalidSource);
        CefProcessor processor = new CefProcessor("tag", "description", "message", "cef", false, true, null);
        expectThrows(IllegalArgumentException.class, () -> processor.execute(invalidIngestDocument));
    }

    public void testStandardMessage() throws IOException, URISyntaxException {
        String message = readCefMessageFile("standard_message.cef.txt");
        Map<String, Object> source = new HashMap<>();
        source.put("message", message);
        document = new IngestDocument("index", "id", 1L, null, null, source);
        CefProcessor processor = new CefProcessor("tag", "description", "message", "cef", false, true, null);
        processor.execute(document);
        assertMapsEqual(
            document.getSource(),
            Map.ofEntries(
                entry(
                    "cef",
                    Map.ofEntries(
                        entry("version", "26"),
                        entry(
                            "device",
                            Map.of("vendor", "security", "product", "threatmanager", "version", "1.0", "event_class_id", "100")
                        ),
                        entry("name", "trojan successfully stopped"),
                        entry("severity", "10")
                    )
                ),
                entry("observer", Map.of("product", "threatmanager", "vendor", "security", "version", "1.0")),
                entry("source", Map.of("ip", "10.0.0.192", "port", 1232, "bytes", 4294L)),
                entry("destination", Map.of("ip", "12.121.122.82", "bytes", 4294L)),
                entry("event", Map.of("id", "1", "code", "100")),
                entry("message", message)
            )
        );
    }

    public void testHeaderOnly() throws IOException, URISyntaxException {
        String message = readCefMessageFile("header_only.cef.txt");
        Map<String, Object> source = new HashMap<>();
        source.put("message", message);
        document = new IngestDocument("index", "id", 1L, null, null, source);
        CefProcessor processor = new CefProcessor("tag", "description", "message", "cef", false, true, null);
        processor.execute(document);
        assertMapsEqual(
            document.getSource(),
            Map.ofEntries(
                entry(
                    "cef",
                    Map.ofEntries(
                        entry("version", "26"),
                        entry(
                            "device",
                            Map.of("vendor", "security", "product", "threatmanager", "version", "1.0", "event_class_id", "100")
                        ),
                        entry("name", "trojan successfully stopped"),
                        entry("severity", "10")
                    )
                ),
                entry("event", Map.of("code", "100")),
                entry("observer", Map.of("product", "threatmanager", "vendor", "security", "version", "1.0")),
                entry("message", message)
            )
        );
    }

    public void testEmptyDeviceFields() throws IOException, URISyntaxException {
        String message = readCefMessageFile("empty_device_fields.cef.txt");
        Map<String, Object> source = new HashMap<>();
        source.put("message", message);
        document = new IngestDocument("index", "id", 1L, null, null, source);
        CefProcessor processor = new CefProcessor("tag", "description", "message", "cef", false, true, null);
        processor.execute(document);
        assertMapsEqual(
            document.getSource(),
            Map.ofEntries(
                entry(
                    "cef",
                    Map.ofEntries(
                        entry("version", "0"),
                        entry("device", Map.of("vendor", "", "product", "", "version", "1.0", "event_class_id", "100")),
                        entry("name", "trojan successfully stopped"),
                        entry("severity", "10")
                    )
                ),
                entry("event", Map.of("code", "100")),
                entry("observer", Map.of("product", "", "vendor", "", "version", "1.0")),
                entry("source", Map.of("ip", "10.0.0.192", "port", 1232)),
                entry("destination", Map.of("ip", "12.121.122.82")),
                entry("message", message)
            )
        );
    }

    public void testEscapedPipeInHeader() throws IOException, URISyntaxException {
        String message = readCefMessageFile("escaped_pipe_in_header.cef.txt");
        Map<String, Object> source = new HashMap<>();
        source.put("message", message);
        document = new IngestDocument("index", "id", 1L, null, null, source);
        CefProcessor processor = new CefProcessor("tag", "description", "message", "cef", false, true, null);
        processor.execute(document);
        assertMapsEqual(
            document.getSource(),
            Map.ofEntries(
                entry(
                    "cef",
                    Map.ofEntries(
                        entry("version", "26"),
                        entry(
                            "device",
                            Map.of("vendor", "security", "product", "threat|->manager", "version", "1.0", "event_class_id", "100")
                        ),
                        entry("name", "trojan successfully stopped"),
                        entry("severity", "10")
                    )
                ),
                entry("event", Map.of("code", "100")),
                entry("observer", Map.of("product", "threat|->manager", "vendor", "security", "version", "1.0")),
                entry("source", Map.of("ip", "10.0.0.192", "port", 1232)),
                entry("destination", Map.of("ip", "12.121.122.82")),
                entry("message", message)
            )
        );
    }

    public void testEqualsSignInHeader() throws IOException, URISyntaxException {
        String message = readCefMessageFile("equals_in_header.cef.txt");
        Map<String, Object> source = new HashMap<>();
        source.put("message", message);
        document = new IngestDocument("index", "id", 1L, null, null, source);
        CefProcessor processor = new CefProcessor("tag", "description", "message", "cef", false, true, null);
        processor.execute(document);
        assertMapsEqual(
            document.getSource(),
            Map.ofEntries(
                entry(
                    "cef",
                    Map.ofEntries(
                        entry("version", "26"),
                        entry(
                            "device",
                            Map.of("vendor", "security", "product", "threat=manager", "version", "1.0", "event_class_id", "100")
                        ),
                        entry("name", "trojan successfully stopped"),
                        entry("severity", "10")
                    )
                ),
                entry("event", Map.of("code", "100")),
                entry("observer", Map.of("product", "threat=manager", "vendor", "security", "version", "1.0")),
                entry("source", Map.of("ip", "10.0.0.192", "port", 1232)),
                entry("destination", Map.of("ip", "12.121.122.82")),
                entry("message", message)
            )
        );
    }

    public void testEmptyExtensionValue() throws IOException, URISyntaxException {
        String message = readCefMessageFile("empty_extension.cef.txt");
        Map<String, Object> source = new HashMap<>();
        source.put("message", message);
        document = new IngestDocument("index", "id", 1L, null, null, source);
        CefProcessor processor = new CefProcessor("tag", "description", "message", "cef", false, true, null);
        processor.execute(document);
        assertMapsEqual(
            document.getSource(),
            Map.ofEntries(
                entry(
                    "cef",
                    Map.ofEntries(
                        entry("version", "26"),
                        entry(
                            "device",
                            Map.of("vendor", "security", "product", "threatmanager", "version", "1.0", "event_class_id", "100")
                        ),
                        entry("name", "trojan successfully stopped"),
                        entry("severity", "10")
                    )
                ),
                entry("event", Map.of("code", "100")),
                entry("observer", Map.of("product", "threatmanager", "vendor", "security", "version", "1.0")),
                entry("source", Map.of("ip", "10.0.0.192", "port", 1232)),
                entry("message", message)
            )
        );
    }

    public void testLeadingWhitespace() throws IOException, URISyntaxException {
        String message = readCefMessageFile("leading_whitespace.cef.txt");
        Map<String, Object> source = new HashMap<>();
        source.put("message", message);
        document = new IngestDocument("index", "id", 1L, null, null, source);
        CefProcessor processor = new CefProcessor("tag", "description", "message", "cef", false, true, null);
        processor.execute(document);
        assertMapsEqual(
            document.getSource(),
            Map.ofEntries(
                entry(
                    "cef",
                    Map.ofEntries(
                        entry("version", "0"),
                        entry(
                            "device",
                            Map.of("vendor", "security", "product", "threatmanager", "version", "1.0", "event_class_id", "100")
                        ),
                        entry("name", "trojan successfully stopped"),
                        entry("severity", "10")
                    )
                ),
                entry("event", Map.of("code", "100")),
                entry("observer", Map.of("product", "threatmanager", "vendor", "security", "version", "1.0")),
                entry("source", Map.of("ip", "10.0.0.192", "port", 1232)),
                entry("destination", Map.of("ip", "12.121.122.82")),
                entry("message", message)
            )
        );
    }

    public void testEscapedPipeInExtension() throws IOException, URISyntaxException {
        String message = readCefMessageFile("escaped_pipe_in_extension.cef.txt");
        Map<String, Object> source = new HashMap<>();
        source.put("message", message);
        document = new IngestDocument("index", "id", 1L, null, null, source);
        CefProcessor processor = new CefProcessor("tag", "description", "message", "cef", false, true, null);
        expectThrows(IllegalArgumentException.class, () -> processor.execute(document));
    }

    public void testPipeInMessage() throws IOException, URISyntaxException {
        String message = readCefMessageFile("pipe_in_message.cef.txt");
        Map<String, Object> source = new HashMap<>();
        source.put("message", message);
        document = new IngestDocument("index", "id", 1L, null, null, source);
        CefProcessor processor = new CefProcessor("tag", "description", "message", "cef", false, true, null);
        processor.execute(document);
        assertMapsEqual(
            document.getSource(),
            Map.ofEntries(
                entry(
                    "cef",
                    Map.ofEntries(
                        entry("version", "0"),
                        entry(
                            "device",
                            Map.of("vendor", "security", "product", "threatmanager", "version", "1.0", "event_class_id", "100")
                        ),
                        entry("name", "trojan successfully stopped"),
                        entry("severity", "10"),
                        entry("extensions", Map.of("moo", "this|has a pipe"))
                    )
                ),
                entry("event", Map.of("code", "100")),
                entry("observer", Map.of("product", "threatmanager", "vendor", "security", "version", "1.0")),
                entry("message", message)
            )
        );
    }

    public void testEqualsInMessage() throws IOException, URISyntaxException {
        String message = readCefMessageFile("equals_in_message.cef.txt");
        Map<String, Object> source = new HashMap<>();
        source.put("message", message);
        document = new IngestDocument("index", "id", 1L, null, null, source);
        CefProcessor processor = new CefProcessor("tag", "description", "message", "cef", false, true, null);
        Exception e = expectThrows(IllegalArgumentException.class, () -> processor.execute(document));
        assertThat(e.getMessage(), equalTo("CEF extensions contain unescaped equals sign"));
    }

    public void testEscapesInExtension() throws IOException, URISyntaxException {
        String message = readCefMessageFile("escapes_in_extension.cef.txt");
        Map<String, Object> source = new HashMap<>();
        source.put("message", message);
        document = new IngestDocument("index", "id", 1L, null, null, source);
        CefProcessor processor = new CefProcessor("tag", "description", "message", "cef", false, true, null);
        processor.execute(document);
        assertMapsEqual(
            document.getSource(),
            Map.ofEntries(
                entry(
                    "cef",
                    Map.ofEntries(
                        entry("version", "0"),
                        entry(
                            "device",
                            Map.of("vendor", "security", "product", "threatmanager", "version", "1.0", "event_class_id", "100")
                        ),
                        entry("name", "trojan successfully stopped"),
                        entry("severity", "10"),
                        entry("extensions", Map.of("x", "c\\d=z"))
                    )
                ),
                entry("event", Map.of("code", "100")),
                entry("observer", Map.of("product", "threatmanager", "vendor", "security", "version", "1.0")),
                entry("message", "a+b=c")
            )
        );
    }

    public void testMalformedExtensionEscape() throws IOException, URISyntaxException {
        String message = readCefMessageFile("malformed_extension_escape.cef.txt");
        Map<String, Object> source = new HashMap<>();
        source.put("message", message);
        document = new IngestDocument("index", "id", 1L, null, null, source);
        CefProcessor processor = new CefProcessor("tag", "description", "message", "cef", false, true, null);
        Exception e = expectThrows(IllegalArgumentException.class, () -> processor.execute(document));
        assertThat(e.getMessage(), equalTo("CEF extensions contain unescaped equals sign"));
    }

    public void testMultipleMalformedExtensionValues() throws IOException, URISyntaxException {
        String message = readCefMessageFile("multiple_malformed_extension_values.cef.txt");
        Map<String, Object> source = new HashMap<>();
        source.put("message", message);
        document = new IngestDocument("index", "id", 1L, null, null, source);
        CefProcessor processor = new CefProcessor("tag", "description", "message", "cef", false, true, null);
        Exception e = expectThrows(IllegalArgumentException.class, () -> processor.execute(document));
        assertThat(e.getMessage(), equalTo("CEF extensions contain unescaped equals sign"));
    }

    public void testPaddedMessage() throws IOException, URISyntaxException {
        String message = readCefMessageFile("padded_message.cef.txt");
        Map<String, Object> source = new HashMap<>();
        source.put("message", message);
        document = new IngestDocument("index", "id", 1L, null, null, source);
        CefProcessor processor = new CefProcessor("tag", "description", "message", "cef", false, true, null);
        processor.execute(document);
        assertMapsEqual(
            document.getSource(),
            Map.ofEntries(
                entry(
                    "cef",
                    Map.ofEntries(
                        entry("version", "0"),
                        entry(
                            "device",
                            Map.of("vendor", "security", "product", "threatmanager", "version", "1.0", "event_class_id", "100")
                        ),
                        entry("name", "message is padded"),
                        entry("severity", "10")
                    )
                ),
                entry("event", Map.of("code", "100")),
                entry("observer", Map.of("product", "threatmanager", "vendor", "security", "version", "1.0")),
                entry("source", Map.of("ip", "10.0.0.192", "port", 1232)),
                entry("message", "Trailing space in non-final extensions is  preserved   ")
            )
        );
    }

    public void testCrlfMessage() throws IOException, URISyntaxException {
        String message = readCefMessageFile("crlf_message.cef.txt");
        Map<String, Object> source = new HashMap<>();
        source.put("message", message);
        document = new IngestDocument("index", "id", 1L, null, null, source);
        CefProcessor processor = new CefProcessor("tag", "description", "message", "cef", false, true, null);
        processor.execute(document);
        assertMapsEqual(
            document.getSource(),
            Map.ofEntries(
                entry(
                    "cef",
                    Map.ofEntries(
                        entry("version", "0"),
                        entry(
                            "device",
                            Map.of("vendor", "security", "product", "threatmanager", "version", "1.0", "event_class_id", "100")
                        ),
                        entry("name", "message is padded"),
                        entry("severity", "10")
                    )
                ),
                entry("event", Map.of("code", "100")),
                entry("observer", Map.of("product", "threatmanager", "vendor", "security", "version", "1.0")),
                entry("source", Map.of("port", 1232)),
                entry("message", "Trailing space in final extensions is not preserved")
            )
        );
    }

    public void testTabMessage() throws IOException, URISyntaxException {
        String message = readCefMessageFile("tab_message.cef.txt");
        Map<String, Object> source = new HashMap<>();
        source.put("message", message);
        document = new IngestDocument("index", "id", 1L, null, null, source);
        CefProcessor processor = new CefProcessor("tag", "description", "message", "cef", false, true, null);
        processor.execute(document);
        assertMapsEqual(
            document.getSource(),
            Map.ofEntries(
                entry(
                    "cef",
                    Map.ofEntries(
                        entry("version", "0"),
                        entry(
                            "device",
                            Map.of("vendor", "security", "product", "threatmanager", "version", "1.0", "event_class_id", "100")
                        ),
                        entry("name", "message is padded"),
                        entry("severity", "10")
                    )
                ),
                entry("event", Map.of("code", "100")),
                entry("observer", Map.of("product", "threatmanager", "vendor", "security", "version", "1.0")),
                entry("source", Map.of("port", 1232, "ip", "127.0.0.1")),
                entry("message", "Tabs\tand\rcontrol\ncharacters are preserved\t")
            )
        );
    }

    public void testTabNoSepMessage() throws IOException, URISyntaxException {
        String message = readCefMessageFile("tab_no_sep_message.cef.txt");
        Map<String, Object> source = new HashMap<>();
        source.put("message", message);
        document = new IngestDocument("index", "id", 1L, null, null, source);
        CefProcessor processor = new CefProcessor("tag", "description", "message", "cef", false, true, null);
        Exception e = expectThrows(IllegalArgumentException.class, () -> processor.execute(document));
        assertThat(e.getMessage(), equalTo("CEF extensions contain unescaped equals sign"));
    }

    public void testEscapedMessage() throws IOException, URISyntaxException {
        String message = readCefMessageFile("escaped_message.cef.txt");
        Map<String, Object> source = new HashMap<>();
        source.put("message", message);
        document = new IngestDocument("index", "id", 1L, null, null, source);
        CefProcessor processor = new CefProcessor("tag", "description", "message", "cef", false, true, null);
        processor.execute(document);
        assertMapsEqual(
            document.getSource(),
            Map.ofEntries(
                entry(
                    "cef",
                    Map.ofEntries(
                        entry("version", "0"),
                        entry(
                            "device",
                            Map.ofEntries(
                                entry("vendor", "security\\compliance"),
                                entry("product", "threat|->manager"),
                                entry("version", "1.0"),
                                entry("event_class_id", "100")
                            )
                        ),
                        entry("name", "message contains escapes"),
                        entry("severity", "10")
                    )
                ),
                entry("event", Map.of("code", "100")),
                entry("observer", Map.of("product", "threat|->manager", "vendor", "security\\compliance", "version", "1.0")),
                entry("source", Map.of("port", 1232)),
                entry("message", "Newlines in messages\nare allowed.\r\nAnd so are carriage feeds\\newlines\\=."),
                entry("destination", Map.of("port", 4432))
            )
        );
    }

    public void testTruncatedHeader() throws IOException, URISyntaxException {
        String message = readCefMessageFile("truncated_header.cef.txt");
        Map<String, Object> source = new HashMap<>();
        source.put("message", message);
        document = new IngestDocument("index", "id", 1L, null, null, source);
        CefProcessor processor = new CefProcessor("tag", "description", "message", "cef", false, true, null);
        Exception e = expectThrows(IllegalArgumentException.class, () -> processor.execute(document));
        assertThat(e.getMessage(), equalTo("Incomplete CEF header"));
    }

    public void testIgnoreEmptyValuesInExtension() throws IOException, URISyntaxException {
        String message = readCefMessageFile("ignore_empty_values_in_extension.cef.txt");
        Map<String, Object> source = new HashMap<>();
        source.put("message", message);
        document = new IngestDocument("index", "id", 1L, null, null, source);
        CefProcessor processor = new CefProcessor("tag", "description", "message", "cef", false, true, null);
        processor.execute(document);
        assertMapsEqual(
            document.getSource(),
            Map.ofEntries(
                entry(
                    "cef",
                    Map.ofEntries(
                        entry("version", "26"),
                        entry(
                            "device",
                            Map.of("vendor", "security", "product", "threat=manager", "version", "1.0", "event_class_id", "100")
                        ),
                        entry("name", "trojan successfully stopped"),
                        entry("severity", "10")
                    )
                ),
                entry("event", Map.of("code", "100")),
                entry("observer", Map.of("product", "threat=manager", "vendor", "security", "version", "1.0")),
                entry("destination", Map.of("ip", "12.121.122.82")),
                entry("message", message)
            )
        );
    }

    public void testHyphenInExtensionKey() throws IOException, URISyntaxException {
        String message = readCefMessageFile("hyphen_in_extension_key.cef.txt");
        Map<String, Object> source = new HashMap<>();
        source.put("message", message);
        document = new IngestDocument("index", "id", 1L, null, null, source);
        CefProcessor processor = new CefProcessor("tag", "description", "message", "cef", false, true, null);
        processor.execute(document);
        assertMapsEqual(
            document.getSource(),
            Map.ofEntries(
                entry(
                    "cef",
                    Map.ofEntries(
                        entry("version", "26"),
                        entry(
                            "device",
                            Map.of("vendor", "security", "product", "threatmanager", "version", "1.0", "event_class_id", "100")
                        ),
                        entry("name", "trojan successfully stopped"),
                        entry("severity", "10"),
                        entry("extensions", Map.of("Some-Key", "123456"))
                    )
                ),
                entry("event", Map.of("code", "100")),
                entry("observer", Map.of("product", "threatmanager", "vendor", "security", "version", "1.0")),
                entry("message", message)
            )
        );
    }

    public void testAllFieldsInExtension() throws IOException, URISyntaxException {
        String message = readCefMessageFile("all_fields_in_extension.cef.txt");
        Map<String, Object> source = new HashMap<>();
        source.put("message", message);
        document = new IngestDocument("index", "id", 1L, null, null, source);
        CefProcessor processor = new CefProcessor("tag", "description", "message", "cef", false, true, null);
        processor.execute(document);
        assertMapsEqual(
            document.getSource(),
            Map.ofEntries(
                entry(
                    "cef",
                    Map.ofEntries(
                        entry("version", "0"),
                        entry(
                            "device",
                            Map.of("vendor", "security", "product", "threatmanager", "version", "1.0", "event_class_id", "100")
                        ),
                        entry("name", "trojan successfully stopped"),
                        entry("severity", "10"),
                        entry(
                            "extensions",
                            Map.ofEntries(
                                entry("agentTranslatedZoneKey", 54854L),
                                entry("agentZoneKey", 54855L),
                                entry("customerKey", 54866L),
                                entry("destinationTranslatedZoneKey", 54867L),
                                entry("destinationZoneKey", 54877L),
                                entry("deviceTranslatedZoneKey", 54898L),
                                entry("deviceZoneKey", 54899L),
                                entry("sourceTranslatedZoneKey", 54998L),
                                entry("sourceZoneKey", 546986L),
                                entry("parserVersion", "1.x.2"),
                                entry("parserIdentifier", "ABC123"),
                                entry("deviceNtDomain", "example.org"),
                                entry("agentZoneExternalID", "zoneExtId"),
                                entry("agentTimeZone", "UTC"),
                                entry("deviceCustomIPv6Address1Label", "c6a1Label"),
                                entry("deviceCustomString1", "customString1"),
                                entry("deviceCustomIPv6Address2Label", "c6a2Label"),
                                entry("deviceCustomNumber3", 345L),
                                entry("deviceCustomFloatingPoint1", 1.23),
                                entry("deviceCustomNumber2", 234L),
                                entry("deviceCustomFloatingPoint2", 2.34),
                                entry("deviceCustomFloatingPoint3", 3.45),
                                entry("deviceCustomFloatingPoint4", 4.56),
                                entry("flexDate1", ZonedDateTime.parse("2021-06-01T11:43:20Z")),
                                entry("destinationTranslatedZoneExternalID", "destExtId"),
                                entry("deviceCustomNumber1", 123L),
                                entry("deviceEventCategory", "category"),
                                entry("deviceCustomString6Label", "cs6Label"),
                                entry("deviceCustomNumber2Label", "cn2Label"),
                                entry("flexString1Label", "flexString1Label"),
                                entry("deviceCustomString5Label", "cs5Label"),
                                entry("agentZoneURI", "zoneUri"),
                                entry("deviceCustomString2Label", "cs2Label"),
                                entry("deviceCustomDate2Label", "customDate2Label"),
                                entry("deviceCustomNumber1Label", "cn1Label"),
                                entry("oldFileType", "oldType"),
                                entry("destinationZoneExternalID", "destZoneExtId"),
                                entry("categoryDeviceType", "catDeviceType"),
                                entry("deviceZoneURI", "zoneUri"),
                                entry("sourceTranslatedZoneExternalID", "sourceExtId"),
                                entry("agentTranslatedAddress", "10.0.0.1"),
                                entry("requestCookies", "cookies"),
                                entry("deviceCustomIPv6Address3", "2001:db8::3"),
                                entry("oldFilePath", "/old/path"),
                                entry("deviceCustomIPv6Address2", "2001:db8::2"),
                                entry("deviceCustomIPv6Address1", "2001:db8::1"),
                                entry("oldFileId", "oldId"),
                                entry("deviceTranslatedZoneExternalID", "transExtId"),
                                entry("deviceCustomFloatingPoint2Label", "cfp2Label"),
                                entry("deviceTranslatedZoneURI", "transUri"),
                                entry("deviceCustomIPv6Address4Label", "c6a4Label"),
                                entry("agentTranslatedZoneURI", "uri"),
                                entry("oldFilePermission", "rw-r--r--"),
                                entry("deviceCustomIPv6Address4", "2001:db8::4"),
                                entry("sourceZoneURI", "sourceZoneUri"),
                                entry("deviceCustomFloatingPoint3Label", "cfp3Label"),
                                entry("agentTranslatedZoneExternalID", "ext123"),
                                entry("destinationZoneURI", "destZoneUri"),
                                entry("flexDate1Label", "flexDate1Label"),
                                entry("agentNtDomain", "example.org"),
                                entry("deviceCustomDate2", ZonedDateTime.parse("2021-06-01T11:45Z")),
                                entry("deviceCustomDate1", ZonedDateTime.parse("2021-06-01T11:43:20Z")),
                                entry("deviceCustomString3Label", "cs3Label"),
                                entry("deviceCustomDate1Label", "customDate1Label"),
                                entry("destinationTranslatedZoneURI", "destUri"),
                                entry("oldFileModificationTime", ZonedDateTime.parse("2021-06-01T11:45Z")),
                                entry("deviceCustomFloatingPoint1Label", "cfp1Label"),
                                entry("deviceCustomIPv6Address3Label", "c6a3Label"),
                                entry("deviceCustomFloatingPoint4Label", "cfp4Label"),
                                entry("oldFileSize", 2048L),
                                entry("externalId", "extId"),
                                entry("baseEventCount", 1234),
                                entry("flexString2", "flexString2"),
                                entry("deviceCustomNumber3Label", "cn3Label"),
                                entry("flexString1", "flexString1"),
                                entry("deviceFacility", "16"),
                                entry("deviceCustomString4Label", "cs4Label"),
                                entry("flexString2Label", "flexString2Label"),
                                entry("deviceCustomString3", "customString3"),
                                entry("deviceCustomString2", "customString2"),
                                entry("deviceCustomString1Label", "cs1Label"),
                                entry("deviceCustomString5", "customString5"),
                                entry("deviceCustomString4", "customString4"),
                                entry("deviceZoneExternalID", "zoneExtId"),
                                entry("deviceCustomString6", "customString6"),
                                entry("oldFileName", "oldFile"),
                                entry("sourceZoneExternalID", "sourceZoneExtId"),
                                entry("oldFileHash", "oldHash"),
                                entry("sourceTranslatedZoneURI", "sourceUri"),
                                entry("oldFileCreateTime", ZonedDateTime.parse("2021-06-01T11:43:20Z"))
                            )
                        )
                    )
                ),
                entry("host", Map.of("nat", Map.of("ip", "10.0.0.3"))),
                entry(
                    "observer",
                    Map.ofEntries(
                        entry("ingress", Map.of("interface", Map.of("name", "eth0"))),
                        entry("registered_domain", "example.com"),
                        entry("product", "threatmanager"),
                        entry("hostname", "host1"),
                        entry("vendor", "security"),
                        entry("ip", "192.168.0.3"),
                        entry("name", "extId"),
                        entry("version", "1.0"),
                        entry("mac", "00:0a:95:9d:68:16"),
                        entry("egress", Map.of("interface", Map.of("name", "eth1")))
                    )
                ),
                entry(
                    "agent",
                    Map.ofEntries(
                        entry("ip", "192.168.0.1"),
                        entry("name", "example.com"),
                        entry("id", "agentId"),
                        entry("type", "agentType"),
                        entry("version", "1.0"),
                        entry("mac", "00:0a:95:9d:68:16")
                    )
                ),
                entry("process", Map.of("name", "procName", "pid", 5678)),
                entry(
                    "destination",
                    Map.ofEntries(
                        entry("nat", Map.of("port", 8080, "ip", "10.0.0.2")),
                        entry("geo", Map.of("location", Map.of("lon", -122.4194, "lat", 37.7749))),
                        entry("registered_domain", "destNtDomain"),
                        entry("process", Map.of("name", "destProc", "pid", 1234)),
                        entry("port", 80),
                        entry("bytes", 91011L),
                        entry("service", Map.of("name", "destService")),
                        entry("domain", "destHost"),
                        entry("ip", "192.168.0.2"),
                        entry("user", Map.of("name", "destUser", "id", "destUserId", "group", Map.of("name", "admin"))),
                        entry("mac", "00:0a:95:9d:68:16")
                    )
                ),
                entry(
                    "source",
                    Map.ofEntries(
                        entry("geo", Map.of("location", Map.of("lon", -122.4194, "lat", 37.7749))),
                        entry("nat", Map.of("port", 8081, "ip", "10.0.0.4")),
                        entry("registered_domain", "sourceNtDomain"),
                        entry("process", Map.of("name", "sourceProc", "pid", 1234)),
                        entry("port", 443),
                        entry("service", Map.of("name", "sourceService")),
                        entry("bytes", 5678L),
                        entry("ip", "192.168.0.4"),
                        entry("domain", "sourceDomain"),
                        entry("user", Map.of("name", "sourceUser", "id", "sourceUserId", "group", Map.of("name", "sourcePriv"))),
                        entry("mac", "00:0a:95:9d:68:16")
                    )
                ),
                entry("message", "message"),
                entry("url", Map.of("original", "url")),
                entry("network", Map.of("protocol", "HTTP", "transport", "TCP", "direction", "inbound")),
                entry(
                    "file",
                    Map.ofEntries(
                        entry("inode", "5678"),
                        entry("path", "/path/to/file"),
                        entry("size", 1024L),
                        entry("created", ZonedDateTime.parse("2021-06-01T11:43:20Z")),
                        entry("name", "file.txt"),
                        entry("mtime", ZonedDateTime.parse("2021-06-01T11:45Z")),
                        entry("type", "txt"),
                        entry("hash", "abcd1234"),
                        entry("group", "rw-r--r--")
                    )
                ),
                entry("@timestamp", ZonedDateTime.parse("2021-06-01T11:43:20Z")),
                entry("organization", Map.of("name", "custUri", "id", "custExtId")),
                entry(
                    "event",
                    Map.ofEntries(
                        entry("action", "blocked"),
                        entry("timezone", "UTC"),
                        entry("end", ZonedDateTime.parse("2021-06-01T11:45Z")),
                        entry("id", "evt123"),
                        entry("outcome", "success"),
                        entry("start", ZonedDateTime.parse("2021-06-01T11:43:20Z")),
                        entry("reason", "reason"),
                        entry("ingested", ZonedDateTime.parse("2021-06-01T11:43:20Z")),
                        entry("kind", "1"),
                        entry("original", "rawEvent"),
                        entry("created", ZonedDateTime.parse("2021-06-01T11:43:20Z")),
                        entry("code", "100")
                    )
                ),
                entry("user_agent", Map.of("original", "Mozilla")),
                entry("http", Map.of("request", Map.of("referrer", "referrer", "method", "GET")))
            )
        );
    }

    // Date parsing tests
    public void testToTimestampWithUnixTimestamp() {
        CefParser parser = new CefParser(ZoneId.of("UTC"), false);
        String unixTimestamp = "1633072800000"; // Example Unix timestamp in milliseconds
        ZonedDateTime expected = ZonedDateTime.ofInstant(Instant.ofEpochMilli(Long.parseLong(unixTimestamp)), ZoneId.of("UTC"));
        ZonedDateTime result = parser.toTimestamp(unixTimestamp);
        assertEquals(expected, result);
    }

    public void testToTimestampWithFormattedDate() {
        CefParser parser = new CefParser(ZoneId.of("Europe/Stockholm"), false);
        String formattedDate = "Oct 01 2021 12:00:00 UTC"; // Example formatted date
        ZonedDateTime expected = ZonedDateTime.parse("2021-10-01T14:00+02:00[Europe/Stockholm]");
        ZonedDateTime result = parser.toTimestamp(formattedDate);
        assertEquals(expected, result);
    }

    public void testToTimestampWithFormattedDateWithoutYear() {
        CefParser parser = new CefParser(ZoneId.of("UTC"), false);
        String formattedDate = "Oct 01 12:00:00 UTC"; // Example formatted date without year
        int currentYear = ZonedDateTime.now(ZoneId.of("UTC")).getYear();
        ZonedDateTime expected = ZonedDateTime.parse(currentYear + "-10-01T12:00:00Z[UTC]");
        ZonedDateTime result = parser.toTimestamp(formattedDate);
        assertEquals(expected, result);
    }

    public void testToTimestampWithFormattedDateWithoutTimezone() {
        CefParser parser = new CefParser(ZoneId.of("UTC"), false);
        String formattedDate = "Sep 07 2018 14:50:39"; // Example formatted date without year
        ZonedDateTime expected = ZonedDateTime.parse("2018-09-07T14:50:39Z[UTC]");
        ZonedDateTime result = parser.toTimestamp(formattedDate);
        assertEquals(expected, result);
    }

    public void testToTimestampWithInvalidDate() {
        CefParser parser = new CefParser(ZoneId.of("UTC"), false);
        String invalidDate = "invalid date";
        expectThrows(IllegalArgumentException.class, () -> parser.toTimestamp(invalidDate));
    }

    public void testToMacAddressWithSeparators() {
        CefParser parser = new CefParser(ZoneId.of("UTC"), false);
        List<String> macAddresses = List.of(
            // EUI-48 (with separators).
            "00:0D:60:AF:1B:61",
            "00-0D-60-AF-1B-61",
            "000D.60AF.1B61",

            // EUI-64 (with separators).
            "00:0D:60:FF:FE:AF:1B:61",
            "00-0D-60-FF-FE-AF-1B-61",
            "000D.60FF.FEAF.1B61"
        );
        macAddresses.forEach(macAddress -> {
            String result = parser.toMACAddress(macAddress);
            assertEquals(macAddress, result);
        });
    }

    public void testInvalidMacAddresses() {
        CefParser parser = new CefParser(ZoneId.of("UTC"), false);
        for (String invalid : List.of("00|0D|60|AF|1B|61", "00:0D:60:AF:1B:61 foo", "0000:0D:60:AF:1B:61")) {
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> parser.toMACAddress(invalid));
            assertThat(e.getMessage(), equalTo("Invalid MAC address format"));
        }
    }

    public void testEUI48ToMacAddressWithOutSeparators() {
        CefParser parser = new CefParser(ZoneId.of("UTC"), false);
        String macAddress = "000D60AF1B61";
        String result = parser.toMACAddress(macAddress);
        assertEquals("00:0D:60:AF:1B:61", result);
    }

    public void testEUI64ToMacAddressWithOutSeparators() {
        CefParser parser = new CefParser(ZoneId.of("UTC"), false);
        String macAddress = "000D60FFFEAF1B61";
        String result = parser.toMACAddress(macAddress);
        assertEquals("00:0D:60:FF:FE:AF:1B:61", result);
    }

    public void testtoIPValidIPv4Address() {
        CefParser parser = new CefParser(ZoneId.of("UTC"), true);
        String result = parser.toIP("192.168.1.1");
        assertEquals("192.168.1.1", result);
    }

    public void testToIPValidIPv6Address() {
        CefParser parser = new CefParser(ZoneId.of("UTC"), true);
        String result = parser.toIP("2001:0db8:85a3:0000:0000:8a2e:0370:7334");
        assertEquals("2001:db8:85a3::8a2e:370:7334", result);
    }

    public void testToIPInvalidIPAddress() {
        CefParser parser = new CefParser(ZoneId.of("UTC"), true);
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> parser.toIP("invalid_ip"));
        assertEquals("Invalid IP address format", exception.getMessage());
    }

    private static void assertMapsEqual(Map<?, ?> actual, Map<?, ?> expected) {
        innerAssertMapsEqual(actual, expected, "");
    }

    private static void innerAssertMapsEqual(final Map<?, ?> actual, final Map<?, ?> expected, final String path) {
        // as a trivial check, make sure the key sets match
        assertThat(
            "The set of keys in the result are not the same as the set of expected keys",
            actual.keySet(),
            containsInAnyOrder(expected.keySet().toArray(new Object[0]))
        );
        // then for each expected key, compare values
        for (Map.Entry<?, ?> entry : expected.entrySet()) {
            Object key = entry.getKey();
            String newPath = path.isEmpty() ? String.valueOf(key) : path + "." + key;
            Object expectedValue = entry.getValue();
            Object actualValue = actual.get(key);
            if (expectedValue instanceof Map<?, ?> expectedMap && actualValue instanceof Map<?, ?> actualMap) {
                innerAssertMapsEqual(expectedMap, actualMap, newPath);
            } else {
                assertThat("Unexpected value for path [" + newPath + "]", actualValue, equalTo(expectedValue));
            }
        }
        // as a last check, make sure they're actually equal -- the above checks are intended to be friendly (and accurate), but this
        // last check makes sure nothing ever sneaks through
        assertThat(actual, equalTo(expected));
    }
}
