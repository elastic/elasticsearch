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
        assertThat(cef.get("deviceVendor"), equalTo("Elastic"));
        assertThat(cef.get("deviceProduct"), equalTo("Vaporware"));
        assertThat(cef.get("deviceVersion"), equalTo("1.0.0-alpha"));
        assertThat(cef.get("deviceEventClassId"), equalTo("18"));
        assertThat(cef.get("name"), equalTo("Web request"));
        assertThat(cef.get("severity"), equalTo("low"));

        Map<String, String> extensions = (Map<String, String>) cef.get("translatedFields");
        assertThat(extensions.get("event.id"), equalTo("3457"));
        assertThat(extensions.get("source.ip"), equalTo("89.160.20.156"));
        assertThat(extensions.get("source.port"), equalTo("33876"));
        assertThat(extensions.get("destination.ip"), equalTo("192.168.10.1"));
        assertThat(extensions.get("destination.port"), equalTo("443"));
        assertThat(extensions.get("event.id"), equalTo("3457"));
        assertThat(extensions.get("http.request.method"), equalTo("POST"));
        assertThat(extensions.get("source.geo.location.lat"), equalTo("38.915"));
        assertThat(extensions.get("source.geo.location.lon"), equalTo("-77.511"));
        assertThat(extensions.get("network.transport"), equalTo("TCP"));
        assertThat(extensions.get("source.service.name"), equalTo("httpd"));
        assertThat(extensions.get("url.original"), equalTo("https://www.example.com/cart"));
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
        assertThat(cef.get("deviceVendor"), equalTo("security"));
        assertThat(cef.get("deviceProduct"), equalTo("threatmanager"));
        assertThat(cef.get("deviceVersion"), equalTo("1.0"));
        assertThat(cef.get("deviceEventClassId"), equalTo("100"));
        assertThat(cef.get("name"), equalTo("trojan successfully stopped"));
        assertThat(cef.get("severity"), equalTo("10"));

        Map<String, String> extensions = (Map<String, String>) cef.get("translatedFields");
        assertThat(extensions.get("source.ip"), equalTo("10.0.0.192"));
        assertThat(extensions.get("destination.ip"), equalTo("12.121.122.82"));
        assertThat(extensions.get("source.port"), equalTo("1232"));
        assertThat(extensions.get("event.id"), equalTo("1"));
        assertThat(extensions.get("source.bytes"), equalTo("4294967296"));
        assertThat(extensions.get("destination.bytes"), equalTo("4294967296"));
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
        assertThat(cef.get("deviceVendor"), equalTo("security"));
        assertThat(cef.get("deviceProduct"), equalTo("threatmanager"));
        assertThat(cef.get("deviceVersion"), equalTo("1.0"));
        assertThat(cef.get("deviceEventClassId"), equalTo("100"));
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
        Map<String, String> extensions = (Map<String, String>) cef.get("translatedFields");
        assertThat(extensions.get("source.ip"), equalTo("10.0.0.192"));
        assertThat(extensions.get("destination.ip"), equalTo("12.121.122.82"));
        assertThat(extensions.get("source.port"), equalTo("1232"));
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
        assertThat(cef.get("deviceProduct"), equalTo("threat|->manager"));
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
        assertThat(cef.get("deviceProduct"), equalTo("threat=manager"));
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
        assertThat(cef.get("deviceVendor"), equalTo("security"));
        assertThat(cef.get("deviceProduct"), equalTo("threatmanager"));
        assertThat(cef.get("deviceVersion"), equalTo("1.0"));
        assertThat(cef.get("deviceEventClassId"), equalTo("100"));
        assertThat(cef.get("name"), equalTo("trojan successfully stopped"));
        assertThat(cef.get("severity"), equalTo("10"));
        Map<String, String> extensions = (Map<String, String>) cef.get("translatedFields");
        assertThat(extensions.get("source.ip"), equalTo("10.0.0.192"));
        assertThat(extensions.get("source.port"), equalTo("1232"));
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
        assertThat(cef.get("deviceVendor"), equalTo("security"));
        assertThat(cef.get("deviceProduct"), equalTo("threatmanager"));
        assertThat(cef.get("deviceVersion"), equalTo("1.0"));
        assertThat(cef.get("deviceEventClassId"), equalTo("100"));
        assertThat(cef.get("name"), equalTo("trojan successfully stopped"));
        assertThat(cef.get("severity"), equalTo("10"));
        Map<String, String> extensions = (Map<String, String>) cef.get("translatedFields");
        assertThat(extensions.get("source.ip"), equalTo("10.0.0.192"));
        assertThat(extensions.get("destination.ip"), equalTo("12.121.122.82"));
        assertThat(extensions.get("source.port"), equalTo("1232"));
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
        assertThat(cef.get("deviceVendor"), equalTo("security"));
        assertThat(cef.get("deviceProduct"), equalTo("threatmanager"));
        assertThat(cef.get("deviceVersion"), equalTo("1.0"));
        assertThat(cef.get("deviceEventClassId"), equalTo("100"));
        assertThat(cef.get("name"), equalTo("trojan successfully stopped"));
        assertThat(cef.get("severity"), equalTo("10"));
        Map<String, String> extensions = (Map<String, String>) cef.get("extensions");
        assertThat(extensions.get("moo"), equalTo("this\\|has an escaped pipe"));
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
        assertThat(cef.get("deviceVendor"), equalTo("security"));
        assertThat(cef.get("deviceProduct"), equalTo("threatmanager"));
        assertThat(cef.get("deviceVersion"), equalTo("1.0"));
        assertThat(cef.get("deviceEventClassId"), equalTo("100"));
        assertThat(cef.get("name"), equalTo("trojan successfully stopped"));
        assertThat(cef.get("severity"), equalTo("10"));
        Map<String, String> extensions = (Map<String, String>) cef.get("extensions");
        assertThat(extensions.get("moo"), equalTo("this|has an pipe"));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testEqualsInMessage() {
        String message = "CEF:0|security|threatmanager|1.0|100|trojan successfully stopped|10|moo=this =has = equals\\= dst=12.121.122.82 spt=1232";
        Map<String, Object> source = new HashMap<>();
        source.put("message", message);
        ingestDocument = new IngestDocument("index", "id", 1L, null, null, source);
        CefProcessor processor = new CefProcessor("tag", "description", "message", "cef", false, true);
        processor.execute(ingestDocument);

        Map<String, Object> cef = ingestDocument.getFieldValue("cef", Map.class);
        assertThat(cef.get("version"), equalTo("0"));
        assertThat(cef.get("deviceVendor"), equalTo("security"));
        assertThat(cef.get("deviceProduct"), equalTo("threatmanager"));
        assertThat(cef.get("deviceVersion"), equalTo("1.0"));
        assertThat(cef.get("deviceEventClassId"), equalTo("100"));
        assertThat(cef.get("name"), equalTo("trojan successfully stopped"));
        assertThat(cef.get("severity"), equalTo("10"));
        Map<String, String> extensions = (Map<String, String>) cef.get("translatedFields");
        assertThat(extensions.get("moo"), equalTo("this =has = equals="));
        assertThat(extensions.get("destination.ip"), equalTo("12.121.122.82"));
        assertThat(extensions.get("source.port"), equalTo("1232"));
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
        assertThat(cef.get("deviceVendor"), equalTo("security"));
        assertThat(cef.get("deviceProduct"), equalTo("threatmanager"));
        assertThat(cef.get("deviceVersion"), equalTo("1.0"));
        assertThat(cef.get("deviceEventClassId"), equalTo("100"));
        assertThat(cef.get("name"), equalTo("trojan successfully stopped"));
        assertThat(cef.get("severity"), equalTo("10"));

        Map<String, String> extensions = (Map<String, String>) cef.get("translatedFields");
        assertThat(extensions.get("message"), equalTo("a+b=c"));
        assertThat(extensions.get("x"), equalTo("c\\d=z"));
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
        assertThat(cef.get("deviceVendor"), equalTo("FooBar"));
        assertThat(cef.get("deviceProduct"), equalTo("Web Gateway"));
        assertThat(cef.get("deviceVersion"), equalTo("1.2.3.45.67"));
        assertThat(cef.get("deviceEventClassId"), equalTo("200"));
        assertThat(cef.get("name"), equalTo("Success"));
        assertThat(cef.get("severity"), equalTo("2"));

        Map<String, String> extensions = (Map<String, String>) cef.get("translatedFields");
        assertThat(extensions.get("@timestamp"), equalTo("Sep 07 2018 14:50:39"));
        assertThat(extensions.get("cat"), equalTo("Access Log"));
        assertThat(extensions.get("destination.ip"), equalTo("1.1.1.1"));
        assertThat(extensions.get("destination.domain"), equalTo("foo.example.com"));
        assertThat(extensions.get("suser"), equalTo("redacted"));
        assertThat(extensions.get("source.ip"), equalTo("2.2.2.2"));
        assertThat(extensions.get("http.request.method"), equalTo("POST"));
        assertThat(extensions.get("url.original"), equalTo("'https://foo.example.com/bar/bingo/1'"));
        assertThat(extensions.get("user_agent.original"), equalTo("'Foo-Bar/2018.1.7; =Email:user@example.com; Guid:test='"));
        assertThat(extensions.get("cs1Label"), equalTo("Foo Bar"));
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
        assertThat(cef.get("deviceVendor"), equalTo("vendor"));
        assertThat(cef.get("deviceProduct"), equalTo("product"));
        assertThat(cef.get("deviceVersion"), equalTo("version"));
        assertThat(cef.get("deviceEventClassId"), equalTo("event_id"));
        assertThat(cef.get("name"), equalTo("name"));
        assertThat(cef.get("severity"), equalTo("Very-High"));

        Map<String, String> extensions = (Map<String, String>) cef.get("translatedFields");
        assertThat(extensions.get("message"), equalTo("Hello World"));
        assertThat(extensions.get("error"), equalTo("Failed because"));
        assertThat(extensions.get("id"), equalTo("=old_id"));
        assertThat(extensions.get("user"), equalTo("root"));
        assertThat(extensions.get("angle"), equalTo("106.7<=180"));
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
        assertThat(cef.get("deviceVendor"), equalTo("security"));
        assertThat(cef.get("deviceProduct"), equalTo("threatmanager"));
        assertThat(cef.get("deviceVersion"), equalTo("1.0"));
        assertThat(cef.get("deviceEventClassId"), equalTo("100"));
        assertThat(cef.get("name"), equalTo("message is padded"));
        assertThat(cef.get("severity"), equalTo("10"));

        Map<String, String> extensions = (Map<String, String>) cef.get("translatedFields");
        assertThat(extensions.get("source.port"), equalTo("1232"));
        assertThat(extensions.get("message"), equalTo("Trailing space in non-final extensions is  preserved"));
        assertThat(extensions.get("source.ip"), equalTo("10.0.0.192"));
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
        assertThat(cef.get("deviceVendor"), equalTo("security"));
        assertThat(cef.get("deviceProduct"), equalTo("threatmanager"));
        assertThat(cef.get("deviceVersion"), equalTo("1.0"));
        assertThat(cef.get("deviceEventClassId"), equalTo("100"));
        assertThat(cef.get("name"), equalTo("message is padded"));
        assertThat(cef.get("severity"), equalTo("10"));

        Map<String, String> extensions = (Map<String, String>) cef.get("translatedFields");
        assertThat(extensions.get("source.port"), equalTo("1232"));
        assertThat(extensions.get("message"), equalTo("Trailing space in final extensions is not preserved"));
        assertThat(extensions.get("destination.port"), equalTo("1234"));
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
        assertThat(cef.get("deviceVendor"), equalTo("security"));
        assertThat(cef.get("deviceProduct"), equalTo("threatmanager"));
        assertThat(cef.get("deviceVersion"), equalTo("1.0"));
        assertThat(cef.get("deviceEventClassId"), equalTo("100"));
        assertThat(cef.get("name"), equalTo("message is padded"));
        assertThat(cef.get("severity"), equalTo("10"));

        Map<String, String> extensions = (Map<String, String>) cef.get("translatedFields");
        assertThat(extensions.get("source.port"), equalTo("1232"));
        assertThat(extensions.get("message"), equalTo("Tabs\tand\rcontrol\ncharacters are preserved"));
        assertThat(extensions.get("source.ip"), equalTo("127.0.0.1"));
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
        assertThat(cef.get("deviceVendor"), equalTo("security"));
        assertThat(cef.get("deviceProduct"), equalTo("threatmanager"));
        assertThat(cef.get("deviceVersion"), equalTo("1.0"));
        assertThat(cef.get("deviceEventClassId"), equalTo("100"));
        assertThat(cef.get("name"), equalTo("message has tabs"));
        assertThat(cef.get("severity"), equalTo("10"));

        Map<String, String> extensions = (Map<String, String>) cef.get("translatedFields");
        assertThat(extensions.get("source.port"), equalTo("1232"));
        assertThat(extensions.get("message"), equalTo("Tab is not a separator"));
        assertThat(extensions.get("source.ip"), equalTo("127.0.0.1"));
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
        assertThat(cef.get("deviceVendor"), equalTo("security\\compliance"));
        assertThat(cef.get("deviceProduct"), equalTo("threat|->manager"));
        assertThat(cef.get("deviceVersion"), equalTo("1.0"));
        assertThat(cef.get("deviceEventClassId"), equalTo("100"));
        assertThat(cef.get("name"), equalTo("message contains escapes"));
        assertThat(cef.get("severity"), equalTo("10"));

        Map<String, String> extensions = (Map<String, String>) cef.get("translatedFields");
        assertThat(extensions.get("source.port"), equalTo("1232"));
        assertThat(extensions.get("message"), equalTo("Newlines in messages\nare allowed.\r\nAnd so are carriage feeds\\newlines\\=."));
        assertThat(extensions.get("destination.port"), equalTo("4432"));
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
        assertThat(cef.get("deviceVendor"), equalTo("SentinelOne"));
        assertThat(cef.get("deviceProduct"), equalTo("Mgmt"));

        Map<String, String> extensions = (Map<String, String>) cef.get("translatedFields");
        assertThat(extensions.get("activityID"), equalTo("1111111111111111111"));
        assertThat(extensions.get("activityType"), equalTo("3505"));
        assertThat(extensions.get("siteId"), equalTo("None"));
        assertThat(extensions.get("siteName"), equalTo("None"));
        assertThat(extensions.get("accountId"), equalTo("1222222222222222222"));
        assertThat(extensions.get("accountName"), equalTo("foo-bar mdr"));
        assertThat(extensions.get("notificationScope"), equalTo("ACCOUNT"));
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
        assertThat(cef.get("deviceVendor"), equalTo("security"));
        assertThat(cef.get("deviceProduct"), equalTo("threat=manager"));
        assertThat(cef.get("deviceVersion"), equalTo("1.0"));
        assertThat(cef.get("deviceEventClassId"), equalTo("100"));
        assertThat(cef.get("name"), equalTo("trojan successfully stopped"));
        assertThat(cef.get("severity"), equalTo("10"));

        Map<String, String> extensions = (Map<String, String>) cef.get("translatedFields");
        assertThat(extensions.get("destination.ip"), equalTo("12.121.122.82"));
        assertThat(extensions.get("source.port"), equalTo(null));
        assertThat(extensions.get("source.ip"), equalTo(null));
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
        assertThat(cef.get("deviceVendor"), equalTo("security"));
        assertThat(cef.get("deviceProduct"), equalTo("threatmanager"));
        assertThat(cef.get("deviceVersion"), equalTo("1.0"));
        assertThat(cef.get("deviceEventClassId"), equalTo("100"));
        assertThat(cef.get("name"), equalTo("trojan successfully stopped"));
        assertThat(cef.get("severity"), equalTo("10"));
        Map<String, String> extensions = (Map<String, String>) cef.get("translatedFields");
        assertThat(extensions.get("Some-Key"), equalTo("123456"));
    }
}
