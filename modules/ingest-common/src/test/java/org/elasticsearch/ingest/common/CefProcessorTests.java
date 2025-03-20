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
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class CefProcessorTests extends ESTestCase {

    private IngestDocument ingestDocument;

    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testExecute() {
        Map<String, Object> source = new HashMap<>();
        source.put(
            "message",
            "CEF:0|Elastic|Vaporware|1.0.0-alpha|18|Web request|low|eventId=3457 requestMethod=POST "
                + "slat=38.915 slong=-77.511 proto=TCP sourceServiceName=httpd requestContext=https://www.google.com "
                + "src=89.160.20.156 spt=33876 dst=192.168.10.1 dpt=443 request=https://www.example.com/cart"
        );
        ingestDocument = new IngestDocument("index", "id", 1L, null, null, source);
        CefProcessor processor = new CefProcessor("tag", "description", "message", "cef", false, true);
        processor.execute(ingestDocument);

        Map<String, Object> cef = ingestDocument.getFieldValue("cef", Map.class);
        assertThat(cef.get("version"), equalTo("0"));
        assertThat(cef.get("device.vendor"), equalTo("Elastic"));
        assertThat(cef.get("device.product"), equalTo("Vaporware"));
        assertThat(cef.get("device.version"), equalTo("1.0.0-alpha"));
        assertThat(cef.get("device.event_class_id"), equalTo("18"));
        assertThat(cef.get("name"), equalTo("Web request"));
        assertThat(cef.get("severity"), equalTo("low"));
        // ECS fields
        assertThat(cef.get("event.id"), equalTo("3457"));
        assertThat(cef.get("source.ip"), equalTo("89.160.20.156"));
        assertThat(cef.get("source.port"), equalTo("33876"));
        assertThat(cef.get("destination.ip"), equalTo("192.168.10.1"));
        assertThat(cef.get("destination.port"), equalTo("443"));
        assertThat(cef.get("event.id"), equalTo("3457"));
        assertThat(cef.get("http.request.method"), equalTo("POST"));
        assertThat(cef.get("source.geo.location.lat"), equalTo("38.915"));
        assertThat(cef.get("source.geo.location.lon"), equalTo("-77.511"));
        assertThat(cef.get("network.transport"), equalTo("TCP"));
        assertThat(cef.get("source.service.name"), equalTo("httpd"));
        assertThat(cef.get("url.original"), equalTo("https://www.example.com/cart"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidCefFormat() {
        Map<String, Object> invalidSource = new HashMap<>();
        invalidSource.put("message", "Invalid CEF message");
        IngestDocument invalidIngestDocument = new IngestDocument("index", "id", 1L, null, null, invalidSource);

        CefProcessor processor = new CefProcessor("tag", "description", "message", "cef", false, true);
        processor.execute(invalidIngestDocument);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testStandardMessage() {
        String message = "CEF:26|security|threatmanager|1.0|100|trojan successfully stopped|10|"
            + "src=10.0.0.192 dst=12.121.122.82 spt=1232 eventId=1 in=4294967296 out=4294967296";
        Map<String, Object> source = new HashMap<>();
        source.put("message", message);
        ingestDocument = new IngestDocument("index", "id", 1L, null, null, source);
        CefProcessor processor = new CefProcessor("tag", "description", "message", "cef", false, true);
        processor.execute(ingestDocument);

        Map<String, Object> cef = ingestDocument.getFieldValue("cef", Map.class);
        assertThat(cef.get("version"), equalTo("26"));
        assertThat(cef.get("device.vendor"), equalTo("security"));
        assertThat(cef.get("device.product"), equalTo("threatmanager"));
        assertThat(cef.get("device.version"), equalTo("1.0"));
        assertThat(cef.get("device.event_class_id"), equalTo("100"));
        assertThat(cef.get("name"), equalTo("trojan successfully stopped"));
        assertThat(cef.get("severity"), equalTo("10"));
        // ECS fields
        assertThat(cef.get("source.ip"), equalTo("10.0.0.192"));
        assertThat(cef.get("destination.ip"), equalTo("12.121.122.82"));
        assertThat(cef.get("source.port"), equalTo("1232"));
        assertThat(cef.get("event.id"), equalTo("1"));
        assertThat(cef.get("source.bytes"), equalTo("4294967296"));
        assertThat(cef.get("destination.bytes"), equalTo("4294967296"));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testHeaderOnly() {
        String message = "CEF:26|security|threatmanager|1.0|100|trojan successfully stopped|10|";
        Map<String, Object> source = new HashMap<>();
        source.put("message", message);
        ingestDocument = new IngestDocument("index", "id", 1L, null, null, source);
        CefProcessor processor = new CefProcessor("tag", "description", "message", "cef", false, true);
        processor.execute(ingestDocument);

        Map<String, Object> cef = ingestDocument.getFieldValue("cef", Map.class);
        assertThat(cef.get("version"), equalTo("26"));
        assertThat(cef.get("device.vendor"), equalTo("security"));
        assertThat(cef.get("device.product"), equalTo("threatmanager"));
        assertThat(cef.get("device.version"), equalTo("1.0"));
        assertThat(cef.get("device.event_class_id"), equalTo("100"));
        assertThat(cef.get("name"), equalTo("trojan successfully stopped"));
        assertThat(cef.get("severity"), equalTo("10"));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testEmptyDeviceFields() {
        String message = "CEF:0|||1.0|100|trojan successfully stopped|10|src=10.0.0.192 dst=12.121.122.82 spt=1232";
        Map<String, Object> source = new HashMap<>();
        source.put("message", message);
        ingestDocument = new IngestDocument("index", "id", 1L, null, null, source);
        CefProcessor processor = new CefProcessor("tag", "description", "message", "cef", false, true);
        processor.execute(ingestDocument);

        Map<String, Object> cef = ingestDocument.getFieldValue("cef", Map.class);

        assertThat(cef.get("source.ip"), equalTo("10.0.0.192"));
        assertThat(cef.get("destination.ip"), equalTo("12.121.122.82"));
        assertThat(cef.get("source.port"), equalTo("1232"));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testEscapedPipeInHeader() {
        String message = "CEF:26|security|threat\\|->manager|1.0|100|"
            + "trojan successfully stopped|10|src=10.0.0.192 dst=12.121.122.82 spt=1232";
        Map<String, Object> source = new HashMap<>();
        source.put("message", message);
        ingestDocument = new IngestDocument("index", "id", 1L, null, null, source);
        CefProcessor processor = new CefProcessor("tag", "description", "message", "cef", false, true);
        processor.execute(ingestDocument);

        Map<String, Object> cef = ingestDocument.getFieldValue("cef", Map.class);
        assertThat(cef.get("device.product"), equalTo("threat|->manager"));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testEqualsSignInHeader() {
        String message = "CEF:26|security|threat=manager|1.0|100|trojan successfully stopped|10|src=10.0.0.192 dst=12.121.122.82 spt=1232";
        Map<String, Object> source = new HashMap<>();
        source.put("message", message);
        ingestDocument = new IngestDocument("index", "id", 1L, null, null, source);
        CefProcessor processor = new CefProcessor("tag", "description", "message", "cef", false, true);
        processor.execute(ingestDocument);

        Map<String, Object> cef = ingestDocument.getFieldValue("cef", Map.class);
        assertThat(cef.get("device.product"), equalTo("threat=manager"));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testEmptyExtensionValue() {
        String message = "CEF:26|security|threatmanager|1.0|100|trojan successfully stopped|10|src=10.0.0.192 dst= spt=1232";
        Map<String, Object> source = new HashMap<>();
        source.put("message", message);
        ingestDocument = new IngestDocument("index", "id", 1L, null, null, source);
        CefProcessor processor = new CefProcessor("tag", "description", "message", "cef", false, true);
        processor.execute(ingestDocument);

        Map<String, Object> cef = ingestDocument.getFieldValue("cef", Map.class);
        assertThat(cef.get("version"), equalTo("26"));
        assertThat(cef.get("device.vendor"), equalTo("security"));
        assertThat(cef.get("device.product"), equalTo("threatmanager"));
        assertThat(cef.get("device.version"), equalTo("1.0"));
        assertThat(cef.get("device.event_class_id"), equalTo("100"));
        assertThat(cef.get("name"), equalTo("trojan successfully stopped"));
        assertThat(cef.get("severity"), equalTo("10"));

        assertThat(cef.get("source.ip"), equalTo("10.0.0.192"));
        assertThat(cef.get("source.port"), equalTo("1232"));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testLeadingWhitespace() {
        String message = "CEF:0|security|threatmanager|1.0|100|trojan successfully stopped|10| src=10.0.0.192 dst=12.121.122.82 spt=1232";
        Map<String, Object> source = new HashMap<>();
        source.put("message", message);
        ingestDocument = new IngestDocument("index", "id", 1L, null, null, source);
        CefProcessor processor = new CefProcessor("tag", "description", "message", "cef", false, true);
        processor.execute(ingestDocument);

        Map<String, Object> cef = ingestDocument.getFieldValue("cef", Map.class);
        assertThat(cef.get("version"), equalTo("0"));
        assertThat(cef.get("device.vendor"), equalTo("security"));
        assertThat(cef.get("device.product"), equalTo("threatmanager"));
        assertThat(cef.get("device.version"), equalTo("1.0"));
        assertThat(cef.get("device.event_class_id"), equalTo("100"));
        assertThat(cef.get("name"), equalTo("trojan successfully stopped"));
        assertThat(cef.get("severity"), equalTo("10"));

        assertThat(cef.get("source.ip"), equalTo("10.0.0.192"));
        assertThat(cef.get("destination.ip"), equalTo("12.121.122.82"));
        assertThat(cef.get("source.port"), equalTo("1232"));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testEscapedPipeInExtension() {
        String message = "CEF:0|security|threatmanager|1.0|100|trojan successfully stopped|10|moo=this\\|has an escaped pipe";
        Map<String, Object> source = new HashMap<>();
        source.put("message", message);
        ingestDocument = new IngestDocument("index", "id", 1L, null, null, source);
        CefProcessor processor = new CefProcessor("tag", "description", "message", "cef", false, true);
        processor.execute(ingestDocument);

        Map<String, Object> cef = ingestDocument.getFieldValue("cef", Map.class);
        assertThat(cef.get("version"), equalTo("0"));
        assertThat(cef.get("device.vendor"), equalTo("security"));
        assertThat(cef.get("device.product"), equalTo("threatmanager"));
        assertThat(cef.get("device.version"), equalTo("1.0"));
        assertThat(cef.get("device.event_class_id"), equalTo("100"));
        assertThat(cef.get("name"), equalTo("trojan successfully stopped"));
        assertThat(cef.get("severity"), equalTo("10"));
        Map<String, String> extensions = (Map<String, String>) cef.get("extensions");
        assertThat(cef.get("moo"), equalTo("this\\|has an escaped pipe"));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testPipeInMessage() {
        String message = "CEF:0|security|threatmanager|1.0|100|trojan successfully stopped|10|moo=this|has an pipe";
        Map<String, Object> source = new HashMap<>();
        source.put("message", message);
        ingestDocument = new IngestDocument("index", "id", 1L, null, null, source);
        CefProcessor processor = new CefProcessor("tag", "description", "message", "cef", false, true);
        processor.execute(ingestDocument);

        Map<String, Object> cef = ingestDocument.getFieldValue("cef", Map.class);
        assertThat(cef.get("version"), equalTo("0"));
        assertThat(cef.get("device.vendor"), equalTo("security"));
        assertThat(cef.get("device.product"), equalTo("threatmanager"));
        assertThat(cef.get("device.version"), equalTo("1.0"));
        assertThat(cef.get("device.event_class_id"), equalTo("100"));
        assertThat(cef.get("name"), equalTo("trojan successfully stopped"));
        assertThat(cef.get("severity"), equalTo("10"));
        Map<String, String> extensions = (Map<String, String>) cef.get("extensions");
        assertThat(cef.get("moo"), equalTo("this|has an pipe"));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testEqualsInMessage() {
        String message =
            "CEF:0|security|threatmanager|1.0|100|trojan successfully stopped|10|moo=this =has = equals\\= dst=12.121.122.82 spt=1232";
        Map<String, Object> source = new HashMap<>();
        source.put("message", message);
        ingestDocument = new IngestDocument("index", "id", 1L, null, null, source);
        CefProcessor processor = new CefProcessor("tag", "description", "message", "cef", false, true);
        processor.execute(ingestDocument);

        Map<String, Object> cef = ingestDocument.getFieldValue("cef", Map.class);
        assertThat(cef.get("version"), equalTo("0"));
        assertThat(cef.get("device.vendor"), equalTo("security"));
        assertThat(cef.get("device.product"), equalTo("threatmanager"));
        assertThat(cef.get("device.version"), equalTo("1.0"));
        assertThat(cef.get("device.event_class_id"), equalTo("100"));
        assertThat(cef.get("name"), equalTo("trojan successfully stopped"));
        assertThat(cef.get("severity"), equalTo("10"));

        assertThat(cef.get("moo"), equalTo("this =has = equals="));
        assertThat(cef.get("destination.ip"), equalTo("12.121.122.82"));
        assertThat(cef.get("source.port"), equalTo("1232"));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testEscapesInExtension() {
        String message = "CEF:0|security|threatmanager|1.0|100|trojan successfully stopped|10|msg=a+b\\=c x=c\\\\d\\=z";
        Map<String, Object> source = new HashMap<>();
        source.put("message", message);
        ingestDocument = new IngestDocument("index", "id", 1L, null, null, source);
        CefProcessor processor = new CefProcessor("tag", "description", "message", "cef", false, true);
        processor.execute(ingestDocument);

        Map<String, Object> cef = ingestDocument.getFieldValue("cef", Map.class);
        assertThat(cef.get("version"), equalTo("0"));
        assertThat(cef.get("device.vendor"), equalTo("security"));
        assertThat(cef.get("device.product"), equalTo("threatmanager"));
        assertThat(cef.get("device.version"), equalTo("1.0"));
        assertThat(cef.get("device.event_class_id"), equalTo("100"));
        assertThat(cef.get("name"), equalTo("trojan successfully stopped"));
        assertThat(cef.get("severity"), equalTo("10"));

        assertThat(cef.get("message"), equalTo("a+b=c"));
        assertThat(cef.get("x"), equalTo("c\\d=z"));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testMalformedExtensionEscape() {
        String message = "CEF:0|FooBar|Web Gateway|1.2.3.45.67|200|Success|2|rt=Sep 07 2018 14:50:39 cat=Access Log dst=1.1.1.1 "
            + "dhost=foo.example.com suser=redacted src=2.2.2.2 requestMethod=POST request='https://foo.example.com/bar/bingo/1' "
            + "requestClientApplication='Foo-Bar/2018.1.7; =Email:user@example.com; Guid:test=' cs1= cs1Label=Foo Bar";
        Map<String, Object> source = new HashMap<>();
        source.put("message", message);
        ingestDocument = new IngestDocument("index", "id", 1L, null, null, source);
        CefProcessor processor = new CefProcessor("tag", "description", "message", "cef", false, true);
        processor.execute(ingestDocument);

        Map<String, Object> cef = ingestDocument.getFieldValue("cef", Map.class);
        assertThat(cef.get("version"), equalTo("0"));
        assertThat(cef.get("device.vendor"), equalTo("FooBar"));
        assertThat(cef.get("device.product"), equalTo("Web Gateway"));
        assertThat(cef.get("device.version"), equalTo("1.2.3.45.67"));
        assertThat(cef.get("device.event_class_id"), equalTo("200"));
        assertThat(cef.get("name"), equalTo("Success"));
        assertThat(cef.get("severity"), equalTo("2"));

        assertThat(cef.get("@timestamp"), equalTo("Sep 07 2018 14:50:39"));
        assertThat(cef.get("cat"), equalTo("Access Log"));
        assertThat(cef.get("destination.ip"), equalTo("1.1.1.1"));
        assertThat(cef.get("destination.domain"), equalTo("foo.example.com"));
        assertThat(cef.get("suser"), equalTo("redacted"));
        assertThat(cef.get("source.ip"), equalTo("2.2.2.2"));
        assertThat(cef.get("http.request.method"), equalTo("POST"));
        assertThat(cef.get("url.original"), equalTo("'https://foo.example.com/bar/bingo/1'"));
        assertThat(cef.get("user_agent.original"), equalTo("'Foo-Bar/2018.1.7; =Email:user@example.com; Guid:test='"));
        assertThat(cef.get("cs1Label"), equalTo("Foo Bar"));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testMultipleMalformedExtensionValues() {
        String message = "CEF:0|vendor|product|version|event_id|name|Very-High| "
            + "msg=Hello World error=Failed because id==old_id user=root angle=106.7<=180";
        Map<String, Object> source = new HashMap<>();
        source.put("message", message);
        ingestDocument = new IngestDocument("index", "id", 1L, null, null, source);
        CefProcessor processor = new CefProcessor("tag", "description", "message", "cef", false, true);
        processor.execute(ingestDocument);

        Map<String, Object> cef = ingestDocument.getFieldValue("cef", Map.class);
        assertThat(cef.get("version"), equalTo("0"));
        assertThat(cef.get("device.vendor"), equalTo("vendor"));
        assertThat(cef.get("device.product"), equalTo("product"));
        assertThat(cef.get("device.version"), equalTo("version"));
        assertThat(cef.get("device.event_class_id"), equalTo("event_id"));
        assertThat(cef.get("name"), equalTo("name"));
        assertThat(cef.get("severity"), equalTo("Very-High"));

        assertThat(cef.get("message"), equalTo("Hello World"));
        assertThat(cef.get("error"), equalTo("Failed because"));
        assertThat(cef.get("id"), equalTo("=old_id"));
        assertThat(cef.get("user"), equalTo("root"));
        assertThat(cef.get("angle"), equalTo("106.7<=180"));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testPaddedMessage() {
        String message = "CEF:0|security|threatmanager|1.0|100|message is padded|10|spt=1232 "
            + "msg=Trailing space in non-final extensions is  preserved    src=10.0.0.192 ";
        Map<String, Object> source = new HashMap<>();
        source.put("message", message);
        ingestDocument = new IngestDocument("index", "id", 1L, null, null, source);
        CefProcessor processor = new CefProcessor("tag", "description", "message", "cef", false, true);
        processor.execute(ingestDocument);

        Map<String, Object> cef = ingestDocument.getFieldValue("cef", Map.class);
        assertThat(cef.get("version"), equalTo("0"));
        assertThat(cef.get("device.vendor"), equalTo("security"));
        assertThat(cef.get("device.product"), equalTo("threatmanager"));
        assertThat(cef.get("device.version"), equalTo("1.0"));
        assertThat(cef.get("device.event_class_id"), equalTo("100"));
        assertThat(cef.get("name"), equalTo("message is padded"));
        assertThat(cef.get("severity"), equalTo("10"));

        assertThat(cef.get("source.port"), equalTo("1232"));
        assertThat(cef.get("message"), equalTo("Trailing space in non-final extensions is  preserved"));
        assertThat(cef.get("source.ip"), equalTo("10.0.0.192"));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testCrlfMessage() {
        String message = "CEF:0|security|threatmanager|1.0|100|message is padded|10|"
            + "spt=1232 msg=Trailing space in final extensions is not preserved\t \r\ndpt=1234";
        Map<String, Object> source = new HashMap<>();
        source.put("message", message);
        ingestDocument = new IngestDocument("index", "id", 1L, null, null, source);
        CefProcessor processor = new CefProcessor("tag", "description", "message", "cef", false, true);
        processor.execute(ingestDocument);

        Map<String, Object> cef = ingestDocument.getFieldValue("cef", Map.class);
        assertThat(cef.get("version"), equalTo("0"));
        assertThat(cef.get("device.vendor"), equalTo("security"));
        assertThat(cef.get("device.product"), equalTo("threatmanager"));
        assertThat(cef.get("device.version"), equalTo("1.0"));
        assertThat(cef.get("device.event_class_id"), equalTo("100"));
        assertThat(cef.get("name"), equalTo("message is padded"));
        assertThat(cef.get("severity"), equalTo("10"));
        assertThat(cef.get("source.port"), equalTo("1232"));
        assertThat(cef.get("message"), equalTo("Trailing space in final extensions is not preserved"));
        assertThat(cef.get("destination.port"), equalTo("1234"));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testTabMessage() {
        String message = "CEF:0|security|threatmanager|1.0|100|message is padded|10|"
            + "spt=1232 msg=Tabs\tand\rcontrol\ncharacters are preserved\t src=127.0.0.1";
        Map<String, Object> source = new HashMap<>();
        source.put("message", message);
        ingestDocument = new IngestDocument("index", "id", 1L, null, null, source);
        CefProcessor processor = new CefProcessor("tag", "description", "message", "cef", false, true);
        processor.execute(ingestDocument);

        Map<String, Object> cef = ingestDocument.getFieldValue("cef", Map.class);
        assertThat(cef.get("version"), equalTo("0"));
        assertThat(cef.get("device.vendor"), equalTo("security"));
        assertThat(cef.get("device.product"), equalTo("threatmanager"));
        assertThat(cef.get("device.version"), equalTo("1.0"));
        assertThat(cef.get("device.event_class_id"), equalTo("100"));
        assertThat(cef.get("name"), equalTo("message is padded"));
        assertThat(cef.get("severity"), equalTo("10"));
        assertThat(cef.get("source.port"), equalTo("1232"));
        assertThat(cef.get("message"), equalTo("Tabs\tand\rcontrol\ncharacters are preserved"));
        assertThat(cef.get("source.ip"), equalTo("127.0.0.1"));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testTabNoSepMessage() {
        String message = "CEF:0|security|threatmanager|1.0|100|message has tabs|10|spt=1232 msg=Tab is not a separator\tsrc=127.0.0.1";
        Map<String, Object> source = new HashMap<>();
        source.put("message", message);
        ingestDocument = new IngestDocument("index", "id", 1L, null, null, source);
        CefProcessor processor = new CefProcessor("tag", "description", "message", "cef", false, true);
        processor.execute(ingestDocument);

        Map<String, Object> cef = ingestDocument.getFieldValue("cef", Map.class);
        assertThat(cef.get("version"), equalTo("0"));
        assertThat(cef.get("device.vendor"), equalTo("security"));
        assertThat(cef.get("device.product"), equalTo("threatmanager"));
        assertThat(cef.get("device.version"), equalTo("1.0"));
        assertThat(cef.get("device.event_class_id"), equalTo("100"));
        assertThat(cef.get("name"), equalTo("message has tabs"));
        assertThat(cef.get("severity"), equalTo("10"));
        assertThat(cef.get("source.port"), equalTo("1232"));
        assertThat(cef.get("message"), equalTo("Tab is not a separator"));
        assertThat(cef.get("source.ip"), equalTo("127.0.0.1"));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testEscapedMessage() {
        String message = "CEF:0|security\\compliance|threat\\|->manager|1.0|100|message contains escapes|10|"
            + "spt=1232 msg=Newlines in messages\\\nare allowed.\\\r\\\nAnd so are carriage feeds\\\\newlines\\\\\\=. dpt=4432";
        Map<String, Object> source = new HashMap<>();
        source.put("message", message);
        ingestDocument = new IngestDocument("index", "id", 1L, null, null, source);
        CefProcessor processor = new CefProcessor("tag", "description", "message", "cef", false, true);
        processor.execute(ingestDocument);

        Map<String, Object> cef = ingestDocument.getFieldValue("cef", Map.class);
        assertThat(cef.get("version"), equalTo("0"));
        assertThat(cef.get("device.vendor"), equalTo("security\\compliance"));
        assertThat(cef.get("device.product"), equalTo("threat|->manager"));
        assertThat(cef.get("device.version"), equalTo("1.0"));
        assertThat(cef.get("device.event_class_id"), equalTo("100"));
        assertThat(cef.get("name"), equalTo("message contains escapes"));
        assertThat(cef.get("severity"), equalTo("10"));
        assertThat(cef.get("source.port"), equalTo("1232"));
        assertThat(cef.get("message"), equalTo("Newlines in messages\nare allowed.\r\nAnd so are carriage feeds\\newlines\\=."));
        assertThat(cef.get("destination.port"), equalTo("4432"));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testTruncatedHeader() {
        String message = "CEF:0|SentinelOne|Mgmt|activityID=1111111111111111111 activityType=3505 "
            + "siteId=None siteName=None accountId=1222222222222222222 accountName=foo-bar mdr notificationScope=ACCOUNT";
        Map<String, Object> source = new HashMap<>();
        source.put("message", message);
        ingestDocument = new IngestDocument("index", "id", 1L, null, null, source);
        CefProcessor processor = new CefProcessor("tag", "description", "message", "cef", false, true);
        processor.execute(ingestDocument);

        Map<String, Object> cef = ingestDocument.getFieldValue("cef", Map.class);
        assertThat(cef.get("version"), equalTo("0"));
        assertThat(cef.get("device.vendor"), equalTo("SentinelOne"));
        assertThat(cef.get("device.product"), equalTo("Mgmt"));
        assertThat(cef.get("activityID"), equalTo("1111111111111111111"));
        assertThat(cef.get("activityType"), equalTo("3505"));
        assertThat(cef.get("siteId"), equalTo("None"));
        assertThat(cef.get("siteName"), equalTo("None"));
        assertThat(cef.get("accountId"), equalTo("1222222222222222222"));
        assertThat(cef.get("accountName"), equalTo("foo-bar mdr"));
        assertThat(cef.get("notificationScope"), equalTo("ACCOUNT"));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testRemoveEmptyValueInExtension() {
        String message = "CEF:26|security|threat=manager|1.0|100|trojan successfully stopped|10|src= dst=12.121.122.82 spt=";
        Map<String, Object> source = new HashMap<>();
        source.put("message", message);
        ingestDocument = new IngestDocument("index", "id", 1L, null, null, source);
        CefProcessor processor = new CefProcessor("tag", "description", "message", "cef", false, true);
        processor.execute(ingestDocument);

        Map<String, Object> cef = ingestDocument.getFieldValue("cef", Map.class);
        assertThat(cef.get("version"), equalTo("26"));
        assertThat(cef.get("device.vendor"), equalTo("security"));
        assertThat(cef.get("device.product"), equalTo("threat=manager"));
        assertThat(cef.get("device.version"), equalTo("1.0"));
        assertThat(cef.get("device.event_class_id"), equalTo("100"));
        assertThat(cef.get("name"), equalTo("trojan successfully stopped"));
        assertThat(cef.get("severity"), equalTo("10"));
        assertThat(cef.get("destination.ip"), equalTo("12.121.122.82"));
        assertThat(cef.get("source.port"), equalTo(null));
        assertThat(cef.get("source.ip"), equalTo(null));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testHyphenInExtensionKey() {
        String message = "CEF:26|security|threatmanager|1.0|100|trojan successfully stopped|10|Some-Key=123456";
        Map<String, Object> source = new HashMap<>();
        source.put("message", message);
        ingestDocument = new IngestDocument("index", "id", 1L, null, null, source);
        CefProcessor processor = new CefProcessor("tag", "description", "message", "cef", false, true);
        processor.execute(ingestDocument);

        Map<String, Object> cef = ingestDocument.getFieldValue("cef", Map.class);
        assertThat(cef.get("version"), equalTo("26"));
        assertThat(cef.get("device.vendor"), equalTo("security"));
        assertThat(cef.get("device.product"), equalTo("threatmanager"));
        assertThat(cef.get("device.version"), equalTo("1.0"));
        assertThat(cef.get("device.event_class_id"), equalTo("100"));
        assertThat(cef.get("name"), equalTo("trojan successfully stopped"));
        assertThat(cef.get("severity"), equalTo("10"));
        assertThat(cef.get("Some-Key"), equalTo("123456"));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testAllFieldsInExtension() {
        String message =
            "CEF:0|security|threatmanager|1.0|100|trojan successfully stopped|10|app=HTTP in=1234 out=5678 dst=192.168.0.1 " +
                "dlat=37.7749 dlong=-122.4194 dhost=example.com dmac=00:0a:95:9d:68:16 dntdom=example.org dpt=80 dpid=1234 " +
                "dproc=procname duid=1001 duser=username dpriv=admin act=blocked dvc=192.168.0.2 deviceDirection=inbound " +
                "deviceDnsDomain=example.com deviceExternalId=ext123 deviceFacility=16 dvchost=host1 deviceInboundInterface=eth0 " +
                "dvcmac=00:0a:95:9d:68:16 deviceOutboundInterface=eth1 dvcpid=5678 deviceProcessName=procname rt=1622547800000 " +
                "dtz=UTC deviceTranslatedAddress=10.0.0.1 device.version=1.0 end=1622547900000 eventId=evt123 outcome=success " +
                "fileCreateTime=1622547800000 fileHash=abcd1234 fileId=5678 fileModificationTime=1622547900000 fname=file.txt " +
                "filePath=/path/to/file filePermission=rw-r--r-- fsize=1024 fileType=txt mrt=1622547800000 msg=message " +
                "reason=reason requestClientApplication=Mozilla requestContext=referrer requestMethod=GET request=url " +
                "src=192.168.0.3 sourceDnsDomain=example.net slat=37.7749 slong=-122.4194 shost=source.com " +
                "smac=00:0a:95:9d:68:16 sntdom=example.net spt=443 spid=1234 sproc=procname sourceServiceName=service " +
                "start=1622547800000 proto=TCP";
        Map<String, Object> source = new HashMap<>();
        source.put("message", message);
        ingestDocument = new IngestDocument("index", "id", 1L, null, null, source);
        CefProcessor processor = new CefProcessor("tag", "description", "message", "cef", false, true);
        processor.execute(ingestDocument);

        Map<String, Object> cef = ingestDocument.getFieldValue("cef", Map.class);
        assertThat(cef.get("version"), equalTo("0"));
        assertThat(cef.get("device.vendor"), equalTo("security"));
        assertThat(cef.get("device.product"), equalTo("threatmanager"));
        assertThat(cef.get("device.version"), equalTo("1.0"));
        assertThat(cef.get("device.event_class_id"), equalTo("100"));
        assertThat(cef.get("name"), equalTo("trojan successfully stopped"));
        assertThat(cef.get("severity"), equalTo("10"));
        assertThat(cef.get("network.protocol"), equalTo("HTTP"));
        assertThat(cef.get("source.bytes"), equalTo("1234"));
        assertThat(cef.get("destination.bytes"), equalTo("5678"));
        assertThat(cef.get("destination.ip"), equalTo("192.168.0.1"));
        assertThat(cef.get("destination.geo.location.lat"), equalTo("37.7749"));
        assertThat(cef.get("destination.geo.location.lon"), equalTo("-122.4194"));
        assertThat(cef.get("destination.domain"), equalTo("example.com"));
        assertThat(cef.get("destination.mac"), equalTo("00:0a:95:9d:68:16"));
        assertThat(cef.get("destination.registered_domain"), equalTo("example.org"));
        assertThat(cef.get("destination.port"), equalTo("80"));
        assertThat(cef.get("destination.process.pid"), equalTo("1234"));
        assertThat(cef.get("destination.process.name"), equalTo("procname"));
        assertThat(cef.get("destination.user.id"), equalTo("1001"));
        assertThat(cef.get("destination.user.name"), equalTo("username"));
        assertThat(cef.get("destination.user.group.name"), equalTo("admin"));
        assertThat(cef.get("event.action"), equalTo("blocked"));
        assertThat(cef.get("observer.ip"), equalTo("192.168.0.2"));
        assertThat(cef.get("network.direction"), equalTo("inbound"));
        assertThat(cef.get("observer.registered_domain"), equalTo("example.com"));
        assertThat(cef.get("observer.name"), equalTo("ext123"));
        assertThat(cef.get("log.syslog.facility.code"), equalTo("16"));
        assertThat(cef.get("observer.hostname"), equalTo("host1"));
        assertThat(cef.get("observer.ingress.interface.name"), equalTo("eth0"));
        assertThat(cef.get("observer.mac"), equalTo("00:0a:95:9d:68:16"));
        assertThat(cef.get("observer.egress.interface.name"), equalTo("eth1"));
        assertThat(cef.get("process.pid"), equalTo("5678"));
        assertThat(cef.get("process.name"), equalTo("procname"));
        assertThat(cef.get("@timestamp"), equalTo("1622547800000"));
        assertThat(cef.get("event.timezone"), equalTo("UTC"));
        assertThat(cef.get("host.nat.ip"), equalTo("10.0.0.1"));
        assertThat(cef.get("observer.version"), equalTo("1.0"));
        assertThat(cef.get("event.end"), equalTo("1622547900000"));
        assertThat(cef.get("event.id"), equalTo("evt123"));
        assertThat(cef.get("event.outcome"), equalTo("success"));
        assertThat(cef.get("file.created"), equalTo("1622547800000"));
        assertThat(cef.get("file.hash"), equalTo("abcd1234"));
        assertThat(cef.get("file.inode"), equalTo("5678"));
        assertThat(cef.get("file.mtime"), equalTo("1622547900000"));
        assertThat(cef.get("file.name"), equalTo("file.txt"));
        assertThat(cef.get("file.path"), equalTo("/path/to/file"));
        assertThat(cef.get("file.group"), equalTo("rw-r--r--"));
        assertThat(cef.get("file.size"), equalTo("1024"));
        assertThat(cef.get("file.extension"), equalTo("txt"));
        assertThat(cef.get("event.ingested"), equalTo("1622547800000"));
        assertThat(cef.get("message"), equalTo("message"));
        assertThat(cef.get("event.reason"), equalTo("reason"));
        assertThat(cef.get("user_agent.original"), equalTo("Mozilla"));
        assertThat(cef.get("http.request.referrer"), equalTo("referrer"));
        assertThat(cef.get("http.request.method"), equalTo("GET"));
        assertThat(cef.get("url.original"), equalTo("url"));
        assertThat(cef.get("source.ip"), equalTo("192.168.0.3"));
        assertThat(cef.get("source.registered_domain"), equalTo("example.net"));
        assertThat(cef.get("source.geo.location.lat"), equalTo("37.7749"));
        assertThat(cef.get("source.geo.location.lon"), equalTo("-122.4194"));
        assertThat(cef.get("source.domain"), equalTo("source.com"));
        assertThat(cef.get("source.mac"), equalTo("00:0a:95:9d:68:16"));
        assertThat(cef.get("source.registered_domain"), equalTo("example.net"));
        assertThat(cef.get("source.port"), equalTo("443"));
        assertThat(cef.get("source.process.pid"), equalTo("1234"));
        assertThat(cef.get("source.process.name"), equalTo("procname"));
        assertThat(cef.get("source.service.name"), equalTo("service"));
        assertThat(cef.get("event.start"), equalTo("1622547800000"));
        assertThat(cef.get("network.transport"), equalTo("TCP"));
    }
}
