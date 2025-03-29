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

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;

public class CefProcessorTests extends ESTestCase {

    private IngestDocument ingestDocument;

    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

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
        CefProcessor processor = new CefProcessor("tag", "description", "message", "cef", false, true, null);
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
        assertThat(ingestDocument.getFieldValue("event.id", String.class), equalTo("3457"));
        assertThat(ingestDocument.getFieldValue("source.ip", String.class), equalTo("89.160.20.156"));
        assertThat(ingestDocument.getFieldValue("source.port", Integer.class), equalTo(33876));
        assertThat(ingestDocument.getFieldValue("destination.ip", String.class), equalTo("192.168.10.1"));
        assertThat(ingestDocument.getFieldValue("destination.port", Integer.class), equalTo(443));
        assertThat(ingestDocument.getFieldValue("http.request.method", String.class), equalTo("POST"));
        assertThat(ingestDocument.getFieldValue("source.geo.location.lat", Double.class), equalTo(38.915));
        assertThat(ingestDocument.getFieldValue("source.geo.location.lon", Double.class), equalTo(-77.511));
        assertThat(ingestDocument.getFieldValue("network.transport", String.class), equalTo("TCP"));
        assertThat(ingestDocument.getFieldValue("source.service.name", String.class), equalTo("httpd"));
        assertThat(ingestDocument.getFieldValue("url.original", String.class), equalTo("https://www.example.com/cart"));
    }

    public void testInvalidCefFormat() {
        Map<String, Object> invalidSource = new HashMap<>();
        invalidSource.put("message", "Invalid CEF message");
        IngestDocument invalidIngestDocument = new IngestDocument("index", "id", 1L, null, null, invalidSource);

        CefProcessor processor = new CefProcessor("tag", "description", "message", "cef", false, true, null);
        expectThrows(IllegalArgumentException.class, () -> processor.execute(invalidIngestDocument));
    }

    @SuppressWarnings("unchecked")
    public void testStandardMessage() {
        String message = "CEF:26|security|threatmanager|1.0|100|trojan successfully stopped|10|"
            + "src=10.0.0.192 dst=12.121.122.82 spt=1232 eventId=1 in=4294967296 out=4294967296";
        Map<String, Object> source = new HashMap<>();
        source.put("message", message);
        ingestDocument = new IngestDocument("index", "id", 1L, null, null, source);
        CefProcessor processor = new CefProcessor("tag", "description", "message", "cef", false, true, null);
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
        assertThat(ingestDocument.getFieldValue("source.ip", String.class), equalTo("10.0.0.192"));
        assertThat(ingestDocument.getFieldValue("destination.ip", String.class), equalTo("12.121.122.82"));
        assertThat(ingestDocument.getFieldValue("source.port", Integer.class), equalTo(1232));
        assertThat(ingestDocument.getFieldValue("event.id", String.class), equalTo("1"));
        assertThat(ingestDocument.getFieldValue("source.bytes", Long.class), equalTo(4294967296L));
        assertThat(ingestDocument.getFieldValue("destination.bytes", Long.class), equalTo(4294967296L));
    }

    @SuppressWarnings("unchecked")
    public void testHeaderOnly() {
        String message = "CEF:26|security|threatmanager|1.0|100|trojan successfully stopped|10|";
        Map<String, Object> source = new HashMap<>();
        source.put("message", message);
        ingestDocument = new IngestDocument("index", "id", 1L, null, null, source);
        CefProcessor processor = new CefProcessor("tag", "description", "message", "cef", false, true, null);
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

    @SuppressWarnings("unchecked")
    public void testEmptyDeviceFields() {
        String message = "CEF:0|||1.0|100|trojan successfully stopped|10|src=10.0.0.192 dst=12.121.122.82 spt=1232";
        Map<String, Object> source = new HashMap<>();
        source.put("message", message);
        ingestDocument = new IngestDocument("index", "id", 1L, null, null, source);
        CefProcessor processor = new CefProcessor("tag", "description", "message", "cef", false, true, null);
        processor.execute(ingestDocument);

        Map<String, Object> cef = ingestDocument.getFieldValue("cef", Map.class);
        assertThat(cef.get("version"), equalTo("0"));
        assertThat(cef.get("device.vendor"), equalTo(""));
        assertThat(cef.get("device.product"), equalTo(""));
        assertThat(cef.get("device.version"), equalTo("1.0"));
        assertThat(cef.get("device.event_class_id"), equalTo("100"));
        assertThat(cef.get("name"), equalTo("trojan successfully stopped"));
        assertThat(cef.get("severity"), equalTo("10"));
        // ECS fields
        assertThat(ingestDocument.getFieldValue("source.ip", String.class), equalTo("10.0.0.192"));
        assertThat(ingestDocument.getFieldValue("destination.ip", String.class), equalTo("12.121.122.82"));
        assertThat(ingestDocument.getFieldValue("source.port", Integer.class), equalTo(1232));
    }

    @SuppressWarnings("unchecked")
    public void testEscapedPipeInHeader() {
        String message = "CEF:26|security|threat\\|->manager|1.0|100|"
            + "trojan successfully stopped|10|src=10.0.0.192 dst=12.121.122.82 spt=1232";
        Map<String, Object> source = new HashMap<>();
        source.put("message", message);
        ingestDocument = new IngestDocument("index", "id", 1L, null, null, source);
        CefProcessor processor = new CefProcessor("tag", "description", "message", "cef", false, true, null);
        processor.execute(ingestDocument);

        Map<String, Object> cef = ingestDocument.getFieldValue("cef", Map.class);
        assertThat(cef.get("device.product"), equalTo("threat|->manager"));
        assertThat(cef.get("version"), equalTo("26"));
        assertThat(cef.get("device.vendor"), equalTo("security"));
        assertThat(cef.get("device.version"), equalTo("1.0"));
        assertThat(cef.get("device.event_class_id"), equalTo("100"));
        assertThat(cef.get("name"), equalTo("trojan successfully stopped"));
        assertThat(cef.get("severity"), equalTo("10"));

        assertThat(ingestDocument.getFieldValue("source.ip", String.class), equalTo("10.0.0.192"));
        assertThat(ingestDocument.getFieldValue("destination.ip", String.class), equalTo("12.121.122.82"));
        assertThat(ingestDocument.getFieldValue("source.port", Integer.class), equalTo(1232));
    }

    @SuppressWarnings("unchecked")
    public void testEqualsSignInHeader() {
        String message = "CEF:26|security|threat=manager|1.0|100|trojan successfully stopped|10|src=10.0.0.192 dst=12.121.122.82 spt=1232";
        Map<String, Object> source = new HashMap<>();
        source.put("message", message);
        ingestDocument = new IngestDocument("index", "id", 1L, null, null, source);
        CefProcessor processor = new CefProcessor("tag", "description", "message", "cef", false, true, null);
        processor.execute(ingestDocument);

        Map<String, Object> cef = ingestDocument.getFieldValue("cef", Map.class);
        assertThat(cef.get("version"), equalTo("26"));
        assertThat(cef.get("device.vendor"), equalTo("security"));
        assertThat(cef.get("device.product"), equalTo("threat=manager"));
        assertThat(cef.get("device.version"), equalTo("1.0"));
        assertThat(cef.get("device.event_class_id"), equalTo("100"));
        assertThat(cef.get("name"), equalTo("trojan successfully stopped"));
        assertThat(cef.get("severity"), equalTo("10"));

        assertThat(ingestDocument.getFieldValue("source.ip", String.class), equalTo("10.0.0.192"));
        assertThat(ingestDocument.getFieldValue("destination.ip", String.class), equalTo("12.121.122.82"));
        assertThat(ingestDocument.getFieldValue("source.port", Integer.class), equalTo(1232));
    }

    @SuppressWarnings("unchecked")
    public void testEmptyExtensionValue() {
        String message = "CEF:26|security|threatmanager|1.0|100|trojan successfully stopped|10|src=10.0.0.192 dst= spt=1232";
        Map<String, Object> source = new HashMap<>();
        source.put("message", message);
        ingestDocument = new IngestDocument("index", "id", 1L, null, null, source);
        CefProcessor processor = new CefProcessor("tag", "description", "message", "cef", false, true, null);
        processor.execute(ingestDocument);

        Map<String, Object> cef = ingestDocument.getFieldValue("cef", Map.class);
        assertThat(cef.get("version"), equalTo("26"));
        assertThat(cef.get("device.vendor"), equalTo("security"));
        assertThat(cef.get("device.product"), equalTo("threatmanager"));
        assertThat(cef.get("device.version"), equalTo("1.0"));
        assertThat(cef.get("device.event_class_id"), equalTo("100"));
        assertThat(cef.get("name"), equalTo("trojan successfully stopped"));
        assertThat(cef.get("severity"), equalTo("10"));

        assertThat(ingestDocument.getFieldValue("source.ip", String.class), equalTo("10.0.0.192"));
        assertThat(ingestDocument.getFieldValue("source.port", Integer.class), equalTo(1232));
    }

    @SuppressWarnings("unchecked")
    public void testLeadingWhitespace() {
        String message = "CEF:0|security|threatmanager|1.0|100|trojan successfully stopped|10| src=10.0.0.192 dst=12.121.122.82 spt=1232";
        Map<String, Object> source = new HashMap<>();
        source.put("message", message);
        ingestDocument = new IngestDocument("index", "id", 1L, null, null, source);
        CefProcessor processor = new CefProcessor("tag", "description", "message", "cef", false, true, null);
        processor.execute(ingestDocument);

        Map<String, Object> cef = ingestDocument.getFieldValue("cef", Map.class);
        assertThat(cef.get("version"), equalTo("0"));
        assertThat(cef.get("device.vendor"), equalTo("security"));
        assertThat(cef.get("device.product"), equalTo("threatmanager"));
        assertThat(cef.get("device.version"), equalTo("1.0"));
        assertThat(cef.get("device.event_class_id"), equalTo("100"));
        assertThat(cef.get("name"), equalTo("trojan successfully stopped"));
        assertThat(cef.get("severity"), equalTo("10"));

        assertThat(ingestDocument.getFieldValue("source.ip", String.class), equalTo("10.0.0.192"));
        assertThat(ingestDocument.getFieldValue("destination.ip", String.class), equalTo("12.121.122.82"));
        assertThat(ingestDocument.getFieldValue("source.port", Integer.class), equalTo(1232));
    }

    @SuppressWarnings("unchecked")
    public void testEscapedPipeInExtension() {
        String message = "CEF:0|security|threatmanager|1.0|100|trojan successfully stopped|10|moo=this\\|has an escaped pipe";
        Map<String, Object> source = new HashMap<>();
        source.put("message", message);
        ingestDocument = new IngestDocument("index", "id", 1L, null, null, source);
        CefProcessor processor = new CefProcessor("tag", "description", "message", "cef", false, true, null);
        processor.execute(ingestDocument);

        Map<String, Object> cef = ingestDocument.getFieldValue("cef", Map.class);
        assertThat(cef.get("version"), equalTo("0"));
        assertThat(cef.get("device.vendor"), equalTo("security"));
        assertThat(cef.get("device.product"), equalTo("threatmanager"));
        assertThat(cef.get("device.version"), equalTo("1.0"));
        assertThat(cef.get("device.event_class_id"), equalTo("100"));
        assertThat(cef.get("name"), equalTo("trojan successfully stopped"));
        assertThat(cef.get("severity"), equalTo("10"));

        assertThat(ingestDocument.getFieldValue("cef.extensions.moo", String.class), equalTo("this\\|has an escaped pipe"));
    }

    @SuppressWarnings("unchecked")
    public void testPipeInMessage() {
        String message = "CEF:0|security|threatmanager|1.0|100|trojan successfully stopped|10|moo=this|has an pipe";
        Map<String, Object> source = new HashMap<>();
        source.put("message", message);
        ingestDocument = new IngestDocument("index", "id", 1L, null, null, source);
        CefProcessor processor = new CefProcessor("tag", "description", "message", "cef", false, true, null);
        processor.execute(ingestDocument);

        Map<String, Object> cef = ingestDocument.getFieldValue("cef", Map.class);
        assertThat(cef.get("version"), equalTo("0"));
        assertThat(cef.get("device.vendor"), equalTo("security"));
        assertThat(cef.get("device.product"), equalTo("threatmanager"));
        assertThat(cef.get("device.version"), equalTo("1.0"));
        assertThat(cef.get("device.event_class_id"), equalTo("100"));
        assertThat(cef.get("name"), equalTo("trojan successfully stopped"));
        assertThat(cef.get("severity"), equalTo("10"));

        assertThat(ingestDocument.getFieldValue("cef.extensions.moo", String.class), equalTo("this|has an pipe"));
    }

    @SuppressWarnings("unchecked")
    public void testEqualsInMessage() {
        String message =
            "CEF:0|security|threatmanager|1.0|100|trojan successfully stopped|10|moo=this =has = equals\\= dst=12.121.122.82 spt=1232";
        Map<String, Object> source = new HashMap<>();
        source.put("message", message);
        ingestDocument = new IngestDocument("index", "id", 1L, null, null, source);
        CefProcessor processor = new CefProcessor("tag", "description", "message", "cef", false, true, null);
        processor.execute(ingestDocument);

        Map<String, Object> cef = ingestDocument.getFieldValue("cef", Map.class);
        assertThat(cef.get("version"), equalTo("0"));
        assertThat(cef.get("device.vendor"), equalTo("security"));
        assertThat(cef.get("device.product"), equalTo("threatmanager"));
        assertThat(cef.get("device.version"), equalTo("1.0"));
        assertThat(cef.get("device.event_class_id"), equalTo("100"));
        assertThat(cef.get("name"), equalTo("trojan successfully stopped"));
        assertThat(cef.get("severity"), equalTo("10"));

        assertThat(ingestDocument.getFieldValue("destination.ip", String.class), equalTo("12.121.122.82"));
        assertThat(ingestDocument.getFieldValue("source.port", Integer.class), equalTo(1232));
        assertThat(ingestDocument.getFieldValue("cef.extensions.moo", String.class), equalTo("this =has = equals="));
    }

    @SuppressWarnings("unchecked")
    public void testEscapesInExtension() {
        String message = "CEF:0|security|threatmanager|1.0|100|trojan successfully stopped|10|msg=a+b\\=c x=c\\\\d\\=z";
        Map<String, Object> source = new HashMap<>();
        source.put("message", message);
        ingestDocument = new IngestDocument("index", "id", 1L, null, null, source);
        CefProcessor processor = new CefProcessor("tag", "description", "message", "cef", false, true, null);
        processor.execute(ingestDocument);

        Map<String, Object> cef = ingestDocument.getFieldValue("cef", Map.class);
        assertThat(cef.get("version"), equalTo("0"));
        assertThat(cef.get("device.vendor"), equalTo("security"));
        assertThat(cef.get("device.product"), equalTo("threatmanager"));
        assertThat(cef.get("device.version"), equalTo("1.0"));
        assertThat(cef.get("device.event_class_id"), equalTo("100"));
        assertThat(cef.get("name"), equalTo("trojan successfully stopped"));
        assertThat(cef.get("severity"), equalTo("10"));

        assertThat(ingestDocument.getFieldValue("message", String.class), equalTo("a+b=c"));
        assertThat(ingestDocument.getFieldValue("cef.extensions.x", String.class), equalTo("c\\d=z"));
    }

    @SuppressWarnings("unchecked")
    public void testMalformedExtensionEscape() {
        String message = "CEF:0|FooBar|Web Gateway|1.2.3.45.67|200|Success|2|rt=Sep 07 2018 14:50:39 cat=Access Log dst=1.1.1.1 "
            + "dhost=foo.example.com suser=redacted src=2.2.2.2 requestMethod=POST request='https://foo.example.com/bar/bingo/1' "
            + "requestClientApplication='Foo-Bar/2018.1.7; =Email:user@example.com; Guid:test=' cs1= cs1Label=Foo Bar";
        Map<String, Object> source = new HashMap<>();
        source.put("message", message);
        ingestDocument = new IngestDocument("index", "id", 1L, null, null, source);
        CefProcessor processor = new CefProcessor("tag", "description", "message", "cef", false, true, null);
        processor.execute(ingestDocument);

        Map<String, Object> cef = ingestDocument.getFieldValue("cef", Map.class);
        assertThat(cef.get("version"), equalTo("0"));
        assertThat(cef.get("device.vendor"), equalTo("FooBar"));
        assertThat(cef.get("device.product"), equalTo("Web Gateway"));
        assertThat(cef.get("device.version"), equalTo("1.2.3.45.67"));
        assertThat(cef.get("device.event_class_id"), equalTo("200"));
        assertThat(cef.get("name"), equalTo("Success"));
        assertThat(cef.get("severity"), equalTo("2"));

        assertThat(ingestDocument.getFieldValue("@timestamp", ZonedDateTime.class), equalTo(ZonedDateTime.parse("2018-09-07T14:50:39Z")));
        assertThat(ingestDocument.getFieldValue("destination.ip", String.class), equalTo("1.1.1.1"));
        assertThat(ingestDocument.getFieldValue("destination.domain", String.class), equalTo("foo.example.com"));
        assertThat(ingestDocument.getFieldValue("source.user.name", String.class), equalTo("redacted"));
        assertThat(ingestDocument.getFieldValue("source.ip", String.class), equalTo("2.2.2.2"));
        assertThat(ingestDocument.getFieldValue("http.request.method", String.class), equalTo("POST"));
        assertThat(ingestDocument.getFieldValue("url.original", String.class), equalTo("'https://foo.example.com/bar/bingo/1'"));
        assertThat(
            ingestDocument.getFieldValue("user_agent.original", String.class),
            equalTo("'Foo-Bar/2018.1.7; =Email:user@example.com; Guid:test='")
        );

        assertThat(ingestDocument.getFieldValue("cef.extensions.deviceCustomString1Label", String.class), equalTo("Foo Bar"));
        assertThat(ingestDocument.getFieldValue("cef.extensions.deviceEventCategory", String.class), equalTo("Access Log"));
    }

    @SuppressWarnings("unchecked")
    public void testMultipleMalformedExtensionValues() {
        String message = "CEF:0|vendor|product|version|event_id|name|Very-High| "
            + "msg=Hello World error=Failed because id==old_id user=root angle=106.7<=180";
        Map<String, Object> source = new HashMap<>();
        source.put("message", message);
        ingestDocument = new IngestDocument("index", "id", 1L, null, null, source);
        CefProcessor processor = new CefProcessor("tag", "description", "message", "cef", false, true, null);
        processor.execute(ingestDocument);

        Map<String, Object> cef = ingestDocument.getFieldValue("cef", Map.class);
        assertThat(cef.get("version"), equalTo("0"));
        assertThat(cef.get("device.vendor"), equalTo("vendor"));
        assertThat(cef.get("device.product"), equalTo("product"));
        assertThat(cef.get("device.version"), equalTo("version"));
        assertThat(cef.get("device.event_class_id"), equalTo("event_id"));
        assertThat(cef.get("name"), equalTo("name"));
        assertThat(cef.get("severity"), equalTo("Very-High"));

        assertThat(ingestDocument.getFieldValue("message", String.class), equalTo("Hello World"));
        assertThat(ingestDocument.getFieldValue("cef.extensions.error", String.class), equalTo("Failed because"));
        assertThat(ingestDocument.getFieldValue("cef.extensions.id", String.class), equalTo("=old_id"));
        assertThat(ingestDocument.getFieldValue("cef.extensions.user", String.class), equalTo("root"));
        assertThat(ingestDocument.getFieldValue("cef.extensions.angle", String.class), equalTo("106.7<=180"));
    }

    @SuppressWarnings("unchecked")
    public void testPaddedMessage() {
        String message = "CEF:0|security|threatmanager|1.0|100|message is padded|10|spt=1232 "
            + "msg=Trailing space in non-final extensions is  preserved    src=10.0.0.192 ";
        Map<String, Object> source = new HashMap<>();
        source.put("message", message);
        ingestDocument = new IngestDocument("index", "id", 1L, null, null, source);
        CefProcessor processor = new CefProcessor("tag", "description", "message", "cef", false, true, null);
        processor.execute(ingestDocument);

        Map<String, Object> cef = ingestDocument.getFieldValue("cef", Map.class);
        assertThat(cef.get("version"), equalTo("0"));
        assertThat(cef.get("device.vendor"), equalTo("security"));
        assertThat(cef.get("device.product"), equalTo("threatmanager"));
        assertThat(cef.get("device.version"), equalTo("1.0"));
        assertThat(cef.get("device.event_class_id"), equalTo("100"));
        assertThat(cef.get("name"), equalTo("message is padded"));
        assertThat(cef.get("severity"), equalTo("10"));

        assertThat(ingestDocument.getFieldValue("source.port", Integer.class), equalTo(1232));
        assertThat(ingestDocument.getFieldValue("message", String.class), equalTo("Trailing space in non-final extensions is  preserved"));
        assertThat(ingestDocument.getFieldValue("source.ip", String.class), equalTo("10.0.0.192"));
    }

    @SuppressWarnings("unchecked")
    public void testCrlfMessage() {
        String message = "CEF:0|security|threatmanager|1.0|100|message is padded|10|"
            + "spt=1232 msg=Trailing space in final extensions is not preserved\t \r\ndpt=1234";
        Map<String, Object> source = new HashMap<>();
        source.put("message", message);
        ingestDocument = new IngestDocument("index", "id", 1L, null, null, source);
        CefProcessor processor = new CefProcessor("tag", "description", "message", "cef", false, true, null);
        processor.execute(ingestDocument);

        Map<String, Object> cef = ingestDocument.getFieldValue("cef", Map.class);
        assertThat(cef.get("version"), equalTo("0"));
        assertThat(cef.get("device.vendor"), equalTo("security"));
        assertThat(cef.get("device.product"), equalTo("threatmanager"));
        assertThat(cef.get("device.version"), equalTo("1.0"));
        assertThat(cef.get("device.event_class_id"), equalTo("100"));
        assertThat(cef.get("name"), equalTo("message is padded"));
        assertThat(cef.get("severity"), equalTo("10"));

        assertThat(ingestDocument.getFieldValue("source.port", Integer.class), equalTo(1232));
        assertThat(ingestDocument.getFieldValue("message", String.class), equalTo("Trailing space in final extensions is not preserved"));
        assertThat(ingestDocument.getFieldValue("destination.port", Integer.class), equalTo(1234));
    }

    @SuppressWarnings("unchecked")
    public void testTabMessage() {
        String message = "CEF:0|security|threatmanager|1.0|100|message is padded|10|"
            + "spt=1232 msg=Tabs\tand\rcontrol\ncharacters are preserved\t src=127.0.0.1";
        Map<String, Object> source = new HashMap<>();
        source.put("message", message);
        ingestDocument = new IngestDocument("index", "id", 1L, null, null, source);
        CefProcessor processor = new CefProcessor("tag", "description", "message", "cef", false, true, null);
        processor.execute(ingestDocument);

        Map<String, Object> cef = ingestDocument.getFieldValue("cef", Map.class);
        assertThat(cef.get("version"), equalTo("0"));
        assertThat(cef.get("device.vendor"), equalTo("security"));
        assertThat(cef.get("device.product"), equalTo("threatmanager"));
        assertThat(cef.get("device.version"), equalTo("1.0"));
        assertThat(cef.get("device.event_class_id"), equalTo("100"));
        assertThat(cef.get("name"), equalTo("message is padded"));
        assertThat(cef.get("severity"), equalTo("10"));

        assertThat(ingestDocument.getFieldValue("source.port", Integer.class), equalTo(1232));
        assertThat(ingestDocument.getFieldValue("message", String.class), equalTo("Tabs\tand\rcontrol\ncharacters are preserved"));
        assertThat(ingestDocument.getFieldValue("source.ip", String.class), equalTo("127.0.0.1"));
    }

    @SuppressWarnings("unchecked")
    public void testTabNoSepMessage() {
        String message = "CEF:0|security|threatmanager|1.0|100|message has tabs|10|spt=1232 msg=Tab is not a separator\tsrc=127.0.0.1";
        Map<String, Object> source = new HashMap<>();
        source.put("message", message);
        ingestDocument = new IngestDocument("index", "id", 1L, null, null, source);
        CefProcessor processor = new CefProcessor("tag", "description", "message", "cef", false, true, null);
        processor.execute(ingestDocument);

        Map<String, Object> cef = ingestDocument.getFieldValue("cef", Map.class);
        assertThat(cef.get("version"), equalTo("0"));
        assertThat(cef.get("device.vendor"), equalTo("security"));
        assertThat(cef.get("device.product"), equalTo("threatmanager"));
        assertThat(cef.get("device.version"), equalTo("1.0"));
        assertThat(cef.get("device.event_class_id"), equalTo("100"));
        assertThat(cef.get("name"), equalTo("message has tabs"));
        assertThat(cef.get("severity"), equalTo("10"));

        assertThat(ingestDocument.getFieldValue("source.port", Integer.class), equalTo(1232));
        assertThat(ingestDocument.getFieldValue("message", String.class), equalTo("Tab is not a separator"));
        assertThat(ingestDocument.getFieldValue("source.ip", String.class), equalTo("127.0.0.1"));
    }

    @SuppressWarnings("unchecked")
    public void testEscapedMessage() {
        String message = "CEF:0|security\\compliance|threat\\|->manager|1.0|100|message contains escapes|10|"
            + "spt=1232 msg=Newlines in messages\\\nare allowed.\\\r\\\nAnd so are carriage feeds\\\\newlines\\\\\\=. dpt=4432";
        Map<String, Object> source = new HashMap<>();
        source.put("message", message);
        ingestDocument = new IngestDocument("index", "id", 1L, null, null, source);
        CefProcessor processor = new CefProcessor("tag", "description", "message", "cef", false, true, null);
        processor.execute(ingestDocument);

        Map<String, Object> cef = ingestDocument.getFieldValue("cef", Map.class);
        assertThat(cef.get("version"), equalTo("0"));
        assertThat(cef.get("device.vendor"), equalTo("security\\compliance"));
        assertThat(cef.get("device.product"), equalTo("threat|->manager"));
        assertThat(cef.get("device.version"), equalTo("1.0"));
        assertThat(cef.get("device.event_class_id"), equalTo("100"));
        assertThat(cef.get("name"), equalTo("message contains escapes"));
        assertThat(cef.get("severity"), equalTo("10"));

        assertThat(ingestDocument.getFieldValue("source.port", Integer.class), equalTo(1232));
        assertThat(
            ingestDocument.getFieldValue("message", String.class),
            equalTo("Newlines in messages\nare allowed.\r\nAnd so are carriage feeds\\newlines\\=.")
        );
        assertThat(ingestDocument.getFieldValue("destination.port", Integer.class), equalTo(4432));
    }

    @SuppressWarnings("unchecked")
    public void testTruncatedHeader() {
        String message = "CEF:0|SentinelOne|Mgmt|activityID=1111111111111111111 activityType=3505 "
            + "siteId=None siteName=None accountId=1222222222222222222 accountName=foo-bar mdr notificationScope=ACCOUNT";
        Map<String, Object> source = new HashMap<>();
        source.put("message", message);
        ingestDocument = new IngestDocument("index", "id", 1L, null, null, source);
        CefProcessor processor = new CefProcessor("tag", "description", "message", "cef", false, true, null);
        processor.execute(ingestDocument);

        Map<String, Object> cef = ingestDocument.getFieldValue("cef", Map.class);
        assertThat(cef.get("version"), equalTo("0"));
        assertThat(cef.get("device.vendor"), equalTo("SentinelOne"));
        assertThat(cef.get("device.product"), equalTo("Mgmt"));

        assertThat(ingestDocument.getFieldValue("cef.extensions.activityID", String.class), equalTo("1111111111111111111"));
        assertThat(ingestDocument.getFieldValue("cef.extensions.activityType", String.class), equalTo("3505"));
        assertThat(ingestDocument.getFieldValue("cef.extensions.siteId", String.class), equalTo("None"));
        assertThat(ingestDocument.getFieldValue("cef.extensions.siteName", String.class), equalTo("None"));
        assertThat(ingestDocument.getFieldValue("cef.extensions.accountId", String.class), equalTo("1222222222222222222"));
        assertThat(ingestDocument.getFieldValue("cef.extensions.accountName", String.class), equalTo("foo-bar mdr"));
        assertThat(ingestDocument.getFieldValue("cef.extensions.notificationScope", String.class), equalTo("ACCOUNT"));

        // Incomplete Header yields an error message too
        assertThat(ingestDocument.getFieldValue("error.message", HashSet.class), equalTo(new HashSet<>(Set.of("incomplete CEF header"))));
    }

    @SuppressWarnings("unchecked")
    public void testRemoveEmptyValueInExtension() {
        String message = "CEF:26|security|threat=manager|1.0|100|trojan successfully stopped|10|src= dst=12.121.122.82 spt=";
        Map<String, Object> source = new HashMap<>();
        source.put("message", message);
        ingestDocument = new IngestDocument("index", "id", 1L, null, null, source);
        CefProcessor processor = new CefProcessor("tag", "description", "message", "cef", false, true, null);
        processor.execute(ingestDocument);

        Map<String, Object> cef = ingestDocument.getFieldValue("cef", Map.class);
        assertThat(cef.get("version"), equalTo("26"));
        assertThat(cef.get("device.vendor"), equalTo("security"));
        assertThat(cef.get("device.product"), equalTo("threat=manager"));
        assertThat(cef.get("device.version"), equalTo("1.0"));
        assertThat(cef.get("device.event_class_id"), equalTo("100"));
        assertThat(cef.get("name"), equalTo("trojan successfully stopped"));
        assertThat(cef.get("severity"), equalTo("10"));

        assertThat(ingestDocument.getFieldValue("destination.ip", String.class), equalTo("12.121.122.82"));

        // Empty src fields are not mapped into the ingestDocument
        assertThat(ingestDocument.hasField("source.port"), equalTo(false));
        assertThat(ingestDocument.hasField("source.ip"), equalTo(false));
    }

    @SuppressWarnings("unchecked")
    public void testHyphenInExtensionKey() {
        String message = "CEF:26|security|threatmanager|1.0|100|trojan successfully stopped|10|Some-Key=123456";
        Map<String, Object> source = new HashMap<>();
        source.put("message", message);
        ingestDocument = new IngestDocument("index", "id", 1L, null, null, source);
        CefProcessor processor = new CefProcessor("tag", "description", "message", "cef", false, true, null);
        processor.execute(ingestDocument);

        Map<String, Object> cef = ingestDocument.getFieldValue("cef", Map.class);
        assertThat(cef.get("version"), equalTo("26"));
        assertThat(cef.get("device.vendor"), equalTo("security"));
        assertThat(cef.get("device.product"), equalTo("threatmanager"));
        assertThat(cef.get("device.version"), equalTo("1.0"));
        assertThat(cef.get("device.event_class_id"), equalTo("100"));
        assertThat(cef.get("name"), equalTo("trojan successfully stopped"));
        assertThat(cef.get("severity"), equalTo("10"));
        assertThat(ingestDocument.getFieldValue("cef.extensions.Some-Key", String.class), equalTo("123456"));
    }

    @SuppressWarnings("unchecked")
    public void testAllFieldsInExtension() {
        String message = "CEF:0|security|threatmanager|1.0|100|trojan successfully stopped|10|"
            + "agt=192.168.0.1 agentDnsDomain=example.com ahost=agentHost aid=agentId amac=00:0a:95:9d:68:16 agentNtDomain=example.org "
            + "art=1622547800000 atz=UTC agentTranslatedAddress=10.0.0.1 agentTranslatedZoneExternalID=ext123 agentTranslatedZoneURI=uri "
            + "at=agentType av=1.0 agentZoneExternalID=zoneExtId agentZoneURI=zoneUri app=HTTP cnt=1234 in=5678 out=91011 "
            + "customerExternalID=custExtId customerURI=custUri dst=192.168.0.2 dlat=37.7749 dlong=-122.4194 "
            + "dhost=destHost dmac=00:0a:95:9d:68:16 dntdom=destNtDomain dpt=80 dpid=1234 "
            + "dproc=destProc destinationServiceName=destService "
            + "destinationTranslatedAddress=10.0.0.2 destinationTranslatedPort=8080 destinationTranslatedZoneExternalID=destExtId "
            + "destinationTranslatedZoneURI=destUri duid=destUserId duser=destUser dpriv=admin destinationZoneExternalID=destZoneExtId "
            + "destinationZoneURI=destZoneUri act=blocked dvc=192.168.0.3 cfp1Label=cfp1Label cfp3Label=cfp3Label cfp4Label=cfp4Label "
            + "deviceCustomDate1=1622547800000 deviceCustomDate1Label=customDate1Label deviceCustomDate2=1622547900000 "
            + "deviceCustomDate2Label=customDate2Label cfp1=1.23 cfp2=2.34 cfp2Label=cfp2Label cfp3=3.45 cfp4=4.56 c6a1=2001:db8::1 "
            + "c6a1Label=c6a1Label c6a2=2001:db8::2 c6a2Label=c6a2Label c6a3=2001:db8::3 c6a3Label=c6a3Label c6a4=2001:db8::4 "
            + "C6a4Label=c6a4Label cn1=123 cn1Label=cn1Label cn2=234 cn2Label=cn2Label cn3=345 cn3Label=cn3Label cs1=customString1 "
            + "cs1Label=cs1Label cs2=customString2 cs2Label=cs2Label cs3=customString3 cs3Label=cs3Label "
            + "cs4=customString4 cs4Label=cs4Label "
            + "cs5=customString5 cs5Label=cs5Label cs6=customString6 cs6Label=cs6Label deviceDirection=inbound deviceDnsDomain=example.com "
            + "cat=category deviceExternalId=extId deviceFacility=16 dvchost=host1 deviceInboundInterface=eth0 dvcmac=00:0a:95:9d:68:16 "
            + "deviceNtDomain=example.org deviceOutboundInterface=eth1 devicePayloadId=payloadId dvcpid=5678 deviceProcessName=procName "
            + "rt=1622547800000 dtz=UTC deviceTranslatedAddress=10.0.0.3 deviceTranslatedZoneExternalID=transExtId "
            + "deviceTranslatedZoneURI=transUri deviceZoneExternalID=zoneExtId deviceZoneURI=zoneUri end=1622547900000 eventId=evt123 "
            + "outcome=success externalId=extId fileCreateTime=1622547800000 fileHash=abcd1234 fileId=5678 "
            + "fileModificationTime=1622547900000 "
            + "fname=file.txt filePath=/path/to/file filePermission=rw-r--r-- fsize=1024 fileType=txt flexDate1=1622547800000 "
            + "flexDate1Label=flexDate1Label flexString1=flexString1 flexString2=flexString2 flexString1Label=flexString1Label "
            + "flexString2Label=flexString2Label msg=message oldFileCreateTime=1622547800000 oldFileHash=oldHash oldFileId=oldId "
            + "oldFileModificationTime=1622547900000 oldFileName=oldFile oldFilePath=/old/path "
            + "oldFilePermission=rw-r--r-- oldFileSize=2048 "
            + "oldFileType=oldType rawEvent=rawEvent reason=reason requestClientApplication=Mozilla requestContext=referrer "
            + "requestCookies=cookies requestMethod=GET request=url src=192.168.0.4 sourceDnsDomain=sourceDomain "
            + "slat=37.7749 slong=-122.4194 "
            + "shost=sourceHost smac=00:0a:95:9d:68:16 sntdom=sourceNtDomain spt=443 spid=1234 "
            + "sproc=sourceProc sourceServiceName=sourceService "
            + "sourceTranslatedAddress=10.0.0.4 sourceTranslatedPort=8081 sourceTranslatedZoneExternalID=sourceExtId "
            + "sourceTranslatedZoneURI=sourceUri suid=sourceUserId suser=sourceUser spriv=sourcePriv sourceZoneExternalID=sourceZoneExtId "
            + "sourceZoneURI=sourceZoneUri start=1622547800000 proto=TCP type=1 catdt=catDeviceType mrt=1622547800000";
        Map<String, Object> source = new HashMap<>();
        source.put("message", message);
        ingestDocument = new IngestDocument("index", "id", 1L, null, null, source);
        CefProcessor processor = new CefProcessor("tag", "description", "message", "cef", false, true, null);
        processor.execute(ingestDocument);

        Map<String, Object> cef = ingestDocument.getFieldValue("cef", Map.class);
        assertThat(cef.get("version"), equalTo("0"));
        assertThat(cef.get("device.vendor"), equalTo("security"));
        assertThat(cef.get("device.product"), equalTo("threatmanager"));
        assertThat(cef.get("device.version"), equalTo("1.0"));
        assertThat(cef.get("device.event_class_id"), equalTo("100"));
        assertThat(cef.get("name"), equalTo("trojan successfully stopped"));
        assertThat(cef.get("severity"), equalTo("10"));

        assertThat(ingestDocument.getFieldValue("agent.ip", String.class), equalTo("192.168.0.1"));
        assertThat(ingestDocument.getFieldValue("agent.name", String.class), equalTo("example.com"));
        assertThat(ingestDocument.getFieldValue("agent.id", String.class), equalTo("agentId"));
        assertThat(ingestDocument.getFieldValue("agent.type", String.class), equalTo("agentType"));
        assertThat(ingestDocument.getFieldValue("agent.version", String.class), equalTo("1.0"));
        assertThat(ingestDocument.getFieldValue("agent.mac", String.class), equalTo("00:0a:95:9d:68:16"));
        assertThat(ingestDocument.getFieldValue("cef.extensions.deviceNtDomain", String.class), equalTo("example.org"));
        assertThat(ingestDocument.getFieldValue("cef.extensions.agentZoneExternalID", String.class), equalTo("zoneExtId"));
        assertThat(ingestDocument.getFieldValue("cef.extensions.agentTimeZone", String.class), equalTo("UTC"));
        assertThat(ingestDocument.getFieldValue("cef.extensions.deviceCustomIPv6Address1Label", String.class), equalTo("c6a1Label"));
        assertThat(ingestDocument.getFieldValue("cef.extensions.deviceCustomString1", String.class), equalTo("customString1"));
        assertThat(ingestDocument.getFieldValue("cef.extensions.deviceCustomIPv6Address2Label", String.class), equalTo("c6a2Label"));
        assertThat(ingestDocument.getFieldValue("cef.extensions.deviceCustomNumber3", Long.class), equalTo(345L));
        assertThat(ingestDocument.getFieldValue("cef.extensions.deviceCustomFloatingPoint1", Float.class), equalTo(1.23f));
        assertThat(ingestDocument.getFieldValue("cef.extensions.deviceCustomNumber2", Long.class), equalTo(234L));
        assertThat(ingestDocument.getFieldValue("cef.extensions.deviceCustomFloatingPoint2", Float.class), equalTo(2.34f));
        assertThat(ingestDocument.getFieldValue("cef.extensions.deviceCustomFloatingPoint3", Float.class), equalTo(3.45f));
        assertThat(ingestDocument.getFieldValue("cef.extensions.deviceCustomFloatingPoint4", Float.class), equalTo(4.56f));
        assertThat(
            ingestDocument.getFieldValue("cef.extensions.flexDate1", ZonedDateTime.class),
            equalTo(ZonedDateTime.parse("2021-06-01T11:43:20Z"))
        );
        assertThat(ingestDocument.getFieldValue("cef.extensions.destinationTranslatedZoneExternalID", String.class), equalTo("destExtId"));
        assertThat(ingestDocument.getFieldValue("cef.extensions.deviceCustomNumber1", Long.class), equalTo(123L));
        assertThat(ingestDocument.getFieldValue("cef.extensions.deviceEventCategory", String.class), equalTo("category"));
        assertThat(ingestDocument.getFieldValue("cef.extensions.deviceCustomString6Label", String.class), equalTo("cs6Label"));
        assertThat(ingestDocument.getFieldValue("cef.extensions.deviceCustomNumber2Label", String.class), equalTo("cn2Label"));
        assertThat(ingestDocument.getFieldValue("cef.extensions.flexString1Label", String.class), equalTo("flexString1Label"));
        assertThat(ingestDocument.getFieldValue("cef.extensions.deviceCustomString5Label", String.class), equalTo("cs5Label"));
        assertThat(ingestDocument.getFieldValue("cef.extensions.agentZoneURI", String.class), equalTo("zoneUri"));
        assertThat(ingestDocument.getFieldValue("cef.extensions.deviceCustomString2Label", String.class), equalTo("cs2Label"));
        assertThat(ingestDocument.getFieldValue("cef.extensions.deviceCustomDate2Label", String.class), equalTo("customDate2Label"));
        assertThat(ingestDocument.getFieldValue("cef.extensions.deviceCustomNumber1Label", String.class), equalTo("cn1Label"));
        assertThat(ingestDocument.getFieldValue("cef.extensions.oldFileType", String.class), equalTo("oldType"));
        assertThat(ingestDocument.getFieldValue("cef.extensions.destinationZoneExternalID", String.class), equalTo("destZoneExtId"));
        assertThat(ingestDocument.getFieldValue("cef.extensions.categoryDeviceType", String.class), equalTo("catDeviceType"));
        assertThat(ingestDocument.getFieldValue("cef.extensions.deviceZoneURI", String.class), equalTo("zoneUri"));
        assertThat(ingestDocument.getFieldValue("cef.extensions.sourceTranslatedZoneExternalID", String.class), equalTo("sourceExtId"));
        assertThat(ingestDocument.getFieldValue("cef.extensions.agentTranslatedAddress", String.class), equalTo("10.0.0.1"));
        assertThat(ingestDocument.getFieldValue("cef.extensions.requestCookies", String.class), equalTo("cookies"));
        assertThat(ingestDocument.getFieldValue("cef.extensions.deviceCustomIPv6Address3", String.class), equalTo("2001:db8::3"));
        assertThat(ingestDocument.getFieldValue("cef.extensions.oldFilePath", String.class), equalTo("/old/path"));
        assertThat(ingestDocument.getFieldValue("cef.extensions.deviceCustomIPv6Address2", String.class), equalTo("2001:db8::2"));
        assertThat(ingestDocument.getFieldValue("cef.extensions.deviceCustomIPv6Address1", String.class), equalTo("2001:db8::1"));
        assertThat(ingestDocument.getFieldValue("cef.extensions.oldFileId", String.class), equalTo("oldId"));
        assertThat(ingestDocument.getFieldValue("cef.extensions.deviceTranslatedZoneExternalID", String.class), equalTo("transExtId"));
        assertThat(ingestDocument.getFieldValue("cef.extensions.deviceCustomFloatingPoint2Label", String.class), equalTo("cfp2Label"));
        assertThat(ingestDocument.getFieldValue("cef.extensions.deviceTranslatedZoneURI", String.class), equalTo("transUri"));
        assertThat(ingestDocument.getFieldValue("cef.extensions.deviceCustomIPv6Address4Label", String.class), equalTo("c6a4Label"));
        assertThat(ingestDocument.getFieldValue("cef.extensions.agentTranslatedZoneURI", String.class), equalTo("uri"));
        assertThat(ingestDocument.getFieldValue("cef.extensions.oldFilePermission", String.class), equalTo("rw-r--r--"));
        assertThat(ingestDocument.getFieldValue("cef.extensions.deviceCustomIPv6Address4", String.class), equalTo("2001:db8::4"));
        assertThat(ingestDocument.getFieldValue("cef.extensions.sourceZoneURI", String.class), equalTo("sourceZoneUri"));
        assertThat(ingestDocument.getFieldValue("cef.extensions.deviceCustomFloatingPoint3Label", String.class), equalTo("cfp3Label"));
        assertThat(ingestDocument.getFieldValue("cef.extensions.agentTranslatedZoneExternalID", String.class), equalTo("ext123"));
        assertThat(ingestDocument.getFieldValue("cef.extensions.destinationZoneURI", String.class), equalTo("destZoneUri"));
        assertThat(ingestDocument.getFieldValue("cef.extensions.categoryDeviceType", String.class), equalTo("catDeviceType"));
        assertThat(ingestDocument.getFieldValue("cef.extensions.flexDate1Label", String.class), equalTo("flexDate1Label"));
        assertThat(ingestDocument.getFieldValue("cef.extensions.agentNtDomain", String.class), equalTo("example.org"));
        assertThat(
            ingestDocument.getFieldValue("cef.extensions.deviceCustomDate2", ZonedDateTime.class),
            equalTo(ZonedDateTime.parse("2021-06-01T11:45Z"))
        );
        assertThat(
            ingestDocument.getFieldValue("cef.extensions.deviceCustomDate1", ZonedDateTime.class),
            equalTo(ZonedDateTime.parse("2021-06-01T11:43:20Z"))
        );
        assertThat(ingestDocument.getFieldValue("cef.extensions.deviceCustomString3Label", String.class), equalTo("cs3Label"));
        assertThat(ingestDocument.getFieldValue("cef.extensions.deviceCustomDate1Label", String.class), equalTo("customDate1Label"));
        assertThat(ingestDocument.getFieldValue("cef.extensions.destinationTranslatedZoneURI", String.class), equalTo("destUri"));
        assertThat(
            ingestDocument.getFieldValue("cef.extensions.oldFileModificationTime", ZonedDateTime.class),
            equalTo(ZonedDateTime.parse("2021-06-01T11:45Z"))
        );
        assertThat(ingestDocument.getFieldValue("cef.extensions.deviceCustomFloatingPoint1Label", String.class), equalTo("cfp1Label"));
        assertThat(ingestDocument.getFieldValue("cef.extensions.deviceCustomIPv6Address3Label", String.class), equalTo("c6a3Label"));
        assertThat(ingestDocument.getFieldValue("cef.extensions.deviceCustomFloatingPoint4Label", String.class), equalTo("cfp4Label"));
        assertThat(ingestDocument.getFieldValue("cef.extensions.oldFileSize", Integer.class), equalTo(2048));
        assertThat(ingestDocument.getFieldValue("cef.extensions.externalId", String.class), equalTo("extId"));
        assertThat(ingestDocument.getFieldValue("cef.extensions.baseEventCount", Integer.class), equalTo(1234));
        assertThat(ingestDocument.getFieldValue("cef.extensions.flexString2", String.class), equalTo("flexString2"));
        assertThat(ingestDocument.getFieldValue("cef.extensions.deviceCustomNumber3Label", String.class), equalTo("cn3Label"));
        assertThat(ingestDocument.getFieldValue("cef.extensions.flexString1", String.class), equalTo("flexString1"));
        assertThat(ingestDocument.getFieldValue("cef.extensions.deviceCustomString4Label", String.class), equalTo("cs4Label"));
        assertThat(ingestDocument.getFieldValue("cef.extensions.flexString2Label", String.class), equalTo("flexString2Label"));
        assertThat(ingestDocument.getFieldValue("cef.extensions.deviceCustomString3", String.class), equalTo("customString3"));
        assertThat(ingestDocument.getFieldValue("cef.extensions.deviceCustomString2", String.class), equalTo("customString2"));
        assertThat(ingestDocument.getFieldValue("cef.extensions.deviceCustomString1Label", String.class), equalTo("cs1Label"));
        assertThat(ingestDocument.getFieldValue("cef.extensions.deviceCustomString5", String.class), equalTo("customString5"));
        assertThat(ingestDocument.getFieldValue("cef.extensions.deviceCustomString4", String.class), equalTo("customString4"));
        assertThat(ingestDocument.getFieldValue("cef.extensions.deviceZoneExternalID", String.class), equalTo("zoneExtId"));
        assertThat(ingestDocument.getFieldValue("cef.extensions.deviceCustomString6", String.class), equalTo("customString6"));
        assertThat(ingestDocument.getFieldValue("cef.extensions.oldFileName", String.class), equalTo("oldFile"));
        assertThat(ingestDocument.getFieldValue("cef.extensions.sourceZoneExternalID", String.class), equalTo("sourceZoneExtId"));
        assertThat(ingestDocument.getFieldValue("cef.extensions.oldFileHash", String.class), equalTo("oldHash"));
        assertThat(ingestDocument.getFieldValue("cef.extensions.sourceTranslatedZoneURI", String.class), equalTo("sourceUri"));
        assertThat(
            ingestDocument.getFieldValue("cef.extensions.oldFileCreateTime", ZonedDateTime.class),
            equalTo(ZonedDateTime.parse("2021-06-01T11:43:20Z"))
        );
        assertThat(ingestDocument.getFieldValue("process.name", String.class), equalTo("procName"));
        assertThat(ingestDocument.getFieldValue("process.pid", Long.class), equalTo(5678L));
        assertThat(ingestDocument.getFieldValue("destination.nat.port", Integer.class), equalTo(8080));
        assertThat(ingestDocument.getFieldValue("destination.nat.ip", String.class), equalTo("10.0.0.2"));
        assertThat(ingestDocument.getFieldValue("destination.geo.location.lon", Double.class), equalTo(-122.4194));
        assertThat(ingestDocument.getFieldValue("destination.geo.location.lat", Double.class), equalTo(37.7749));
        assertThat(ingestDocument.getFieldValue("destination.registered_domain", String.class), equalTo("destNtDomain"));
        assertThat(ingestDocument.getFieldValue("destination.process.name", String.class), equalTo("destProc"));
        assertThat(ingestDocument.getFieldValue("destination.process.pid", Long.class), equalTo(1234L));
        assertThat(ingestDocument.getFieldValue("destination.port", Integer.class), equalTo(80));
        assertThat(ingestDocument.getFieldValue("destination.bytes", Long.class), equalTo(91011L));
        assertThat(ingestDocument.getFieldValue("destination.service.name", String.class), equalTo("destService"));
        assertThat(ingestDocument.getFieldValue("destination.domain", String.class), equalTo("destHost"));
        assertThat(ingestDocument.getFieldValue("destination.ip", String.class), equalTo("192.168.0.2"));
        assertThat(ingestDocument.getFieldValue("destination.user.name", String.class), equalTo("destUser"));
        assertThat(ingestDocument.getFieldValue("destination.user.id", String.class), equalTo("destUserId"));
        assertThat(ingestDocument.getFieldValue("destination.user.group.name", String.class), equalTo("admin"));
        assertThat(ingestDocument.getFieldValue("destination.mac", String.class), equalTo("00:0a:95:9d:68:16"));
        assertThat(ingestDocument.getFieldValue("source.geo.location.lon", Double.class), equalTo(-122.4194));
        assertThat(ingestDocument.getFieldValue("source.geo.location.lat", Double.class), equalTo(37.7749));
        assertThat(ingestDocument.getFieldValue("source.nat.port", Integer.class), equalTo(8081));
        assertThat(ingestDocument.getFieldValue("source.nat.ip", String.class), equalTo("10.0.0.4"));
        assertThat(ingestDocument.getFieldValue("source.registered_domain", String.class), equalTo("sourceNtDomain"));
        assertThat(ingestDocument.getFieldValue("source.process.name", String.class), equalTo("sourceProc"));
        assertThat(ingestDocument.getFieldValue("source.process.pid", Long.class), equalTo(1234L));
        assertThat(ingestDocument.getFieldValue("source.port", Integer.class), equalTo(443));
        assertThat(ingestDocument.getFieldValue("source.service.name", String.class), equalTo("sourceService"));
        assertThat(ingestDocument.getFieldValue("source.bytes", Long.class), equalTo(5678L));
        assertThat(ingestDocument.getFieldValue("source.ip", String.class), equalTo("192.168.0.4"));
        assertThat(ingestDocument.getFieldValue("source.domain", String.class), equalTo("sourceDomain"));
        assertThat(ingestDocument.getFieldValue("source.user.name", String.class), equalTo("sourceUser"));
        assertThat(ingestDocument.getFieldValue("source.user.id", String.class), equalTo("sourceUserId"));
        assertThat(ingestDocument.getFieldValue("source.user.group.name", String.class), equalTo("sourcePriv"));
        assertThat(ingestDocument.getFieldValue("source.mac", String.class), equalTo("00:0a:95:9d:68:16"));
        assertThat(ingestDocument.getFieldValue("message", String.class), equalTo("message"));
        assertThat(ingestDocument.getFieldValue("url.original", String.class), equalTo("url"));
        assertThat(ingestDocument.getFieldValue("network.protocol", String.class), equalTo("HTTP"));
        assertThat(ingestDocument.getFieldValue("network.transport", String.class), equalTo("TCP"));
        assertThat(ingestDocument.getFieldValue("network.direction", String.class), equalTo("inbound"));
        assertThat(ingestDocument.getFieldValue("observer.ingress.interface.name", String.class), equalTo("eth0"));
        assertThat(ingestDocument.getFieldValue("observer.registered_domain", String.class), equalTo("example.com"));
        assertThat(ingestDocument.getFieldValue("observer.product", String.class), equalTo("threatmanager"));
        assertThat(ingestDocument.getFieldValue("observer.hostname", String.class), equalTo("host1"));
        assertThat(ingestDocument.getFieldValue("observer.vendor", String.class), equalTo("security"));
        assertThat(ingestDocument.getFieldValue("observer.ip", String.class), equalTo("192.168.0.3"));
        assertThat(ingestDocument.getFieldValue("observer.name", String.class), equalTo("extId"));
        assertThat(ingestDocument.getFieldValue("observer.version", String.class), equalTo("1.0"));
        assertThat(ingestDocument.getFieldValue("observer.mac", String.class), equalTo("00:0a:95:9d:68:16"));
        assertThat(ingestDocument.getFieldValue("file.inode", String.class), equalTo("5678"));
        assertThat(ingestDocument.getFieldValue("file.path", String.class), equalTo("/path/to/file"));
        assertThat(ingestDocument.getFieldValue("file.size", Long.class), equalTo(1024L));
        assertThat(ingestDocument.getFieldValue("file.created", ZonedDateTime.class), equalTo(ZonedDateTime.parse("2021-06-01T11:43:20Z")));
        assertThat(ingestDocument.getFieldValue("file.name", String.class), equalTo("file.txt"));
        assertThat(ingestDocument.getFieldValue("file.mtime", ZonedDateTime.class), equalTo(ZonedDateTime.parse("2021-06-01T11:45Z")));
        assertThat(ingestDocument.getFieldValue("file.type", String.class), equalTo("txt"));
        assertThat(ingestDocument.getFieldValue("file.hash", String.class), equalTo("abcd1234"));
        assertThat(ingestDocument.getFieldValue("file.group", String.class), equalTo("rw-r--r--"));
        assertThat(ingestDocument.getFieldValue("@timestamp", ZonedDateTime.class), equalTo(ZonedDateTime.parse("2021-06-01T11:43:20Z")));
        assertThat(ingestDocument.getFieldValue("organization.name", String.class), equalTo("custUri"));
        assertThat(ingestDocument.getFieldValue("organization.id", String.class), equalTo("custExtId"));
        assertThat(ingestDocument.getFieldValue("organization.name", String.class), equalTo("custUri"));
        assertThat(ingestDocument.getFieldValue("destination.ip", String.class), equalTo("192.168.0.2"));
        assertThat(ingestDocument.getFieldValue("destination.geo.location.lat", Double.class), equalTo(37.7749));
        assertThat(ingestDocument.getFieldValue("destination.geo.location.lon", Double.class), equalTo(-122.4194));
        assertThat(ingestDocument.getFieldValue("destination.mac", String.class), equalTo("00:0a:95:9d:68:16"));
        assertThat(ingestDocument.getFieldValue("destination.port", Integer.class), equalTo(80));
        assertThat(ingestDocument.getFieldValue("destination.process.pid", Long.class), equalTo(1234L));
        assertThat(ingestDocument.getFieldValue("destination.process.name", String.class), equalTo("destProc"));
        assertThat(ingestDocument.getFieldValue("destination.user.id", String.class), equalTo("destUserId"));
        assertThat(ingestDocument.getFieldValue("destination.user.name", String.class), equalTo("destUser"));
        assertThat(ingestDocument.getFieldValue("destination.user.group.name", String.class), equalTo("admin"));
        assertThat(ingestDocument.getFieldValue("event.action", String.class), equalTo("blocked"));
        assertThat(ingestDocument.getFieldValue("observer.ip", String.class), equalTo("192.168.0.3"));
        assertThat(ingestDocument.getFieldValue("network.direction", String.class), equalTo("inbound"));
        assertThat(ingestDocument.getFieldValue("observer.hostname", String.class), equalTo("host1"));
        assertThat(ingestDocument.getFieldValue("observer.ingress.interface.name", String.class), equalTo("eth0"));
        assertThat(ingestDocument.getFieldValue("observer.mac", String.class), equalTo("00:0a:95:9d:68:16"));
        assertThat(ingestDocument.getFieldValue("observer.egress.interface.name", String.class), equalTo("eth1"));
        assertThat(ingestDocument.getFieldValue("process.pid", Long.class), equalTo(5678L));
        assertThat(ingestDocument.getFieldValue("process.name", String.class), equalTo("procName"));
        assertThat(ingestDocument.getFieldValue("@timestamp", ZonedDateTime.class), equalTo(ZonedDateTime.parse("2021-06-01T11:43:20Z")));
        assertThat(ingestDocument.getFieldValue("event.timezone", String.class), equalTo("UTC"));
        assertThat(ingestDocument.getFieldValue("host.nat.ip", String.class), equalTo("10.0.0.3"));
        assertThat(ingestDocument.getFieldValue("observer.version", String.class), equalTo("1.0"));
        assertThat(ingestDocument.getFieldValue("event.end", ZonedDateTime.class), equalTo(ZonedDateTime.parse("2021-06-01T11:45Z")));
        assertThat(ingestDocument.getFieldValue("event.id", String.class), equalTo("evt123"));
        assertThat(ingestDocument.getFieldValue("event.outcome", String.class), equalTo("success"));
        assertThat(ingestDocument.getFieldValue("file.created", ZonedDateTime.class), equalTo(ZonedDateTime.parse("2021-06-01T11:43:20Z")));
        assertThat(ingestDocument.getFieldValue("file.hash", String.class), equalTo("abcd1234"));
        assertThat(ingestDocument.getFieldValue("file.inode", String.class), equalTo("5678"));
        assertThat(ingestDocument.getFieldValue("file.mtime", ZonedDateTime.class), equalTo(ZonedDateTime.parse("2021-06-01T11:45Z")));
        assertThat(ingestDocument.getFieldValue("file.name", String.class), equalTo("file.txt"));
        assertThat(ingestDocument.getFieldValue("file.path", String.class), equalTo("/path/to/file"));
        assertThat(ingestDocument.getFieldValue("file.group", String.class), equalTo("rw-r--r--"));
        assertThat(ingestDocument.getFieldValue("file.size", Long.class), equalTo(1024L));
        assertThat(ingestDocument.getFieldValue("file.type", String.class), equalTo("txt"));
        assertThat(ingestDocument.getFieldValue("message", String.class), equalTo("message"));
        assertThat(ingestDocument.getFieldValue("event.reason", String.class), equalTo("reason"));
        assertThat(ingestDocument.getFieldValue("user_agent.original", String.class), equalTo("Mozilla"));
        assertThat(ingestDocument.getFieldValue("http.request.referrer", String.class), equalTo("referrer"));
        assertThat(ingestDocument.getFieldValue("http.request.method", String.class), equalTo("GET"));
        assertThat(ingestDocument.getFieldValue("url.original", String.class), equalTo("url"));
        assertThat(ingestDocument.getFieldValue("source.ip", String.class), equalTo("192.168.0.4"));
        assertThat(ingestDocument.getFieldValue("source.geo.location.lat", Double.class), equalTo(37.7749));
        assertThat(ingestDocument.getFieldValue("source.geo.location.lon", Double.class), equalTo(-122.4194));
        assertThat(ingestDocument.getFieldValue("source.domain", String.class), equalTo("sourceDomain"));
        assertThat(ingestDocument.getFieldValue("source.mac", String.class), equalTo("00:0a:95:9d:68:16"));
        assertThat(ingestDocument.getFieldValue("source.port", Integer.class), equalTo(443));
        assertThat(ingestDocument.getFieldValue("source.process.pid", Long.class), equalTo(1234L));
        assertThat(ingestDocument.getFieldValue("source.process.name", String.class), equalTo("sourceProc"));
        assertThat(ingestDocument.getFieldValue("source.service.name", String.class), equalTo("sourceService"));
        assertThat(ingestDocument.getFieldValue("event.start", ZonedDateTime.class), equalTo(ZonedDateTime.parse("2021-06-01T11:43:20Z")));
        assertThat(ingestDocument.getFieldValue("network.transport", String.class), equalTo("TCP"));
        assertThat(
            ingestDocument.getFieldValue("event.ingested", ZonedDateTime.class),
            equalTo(ZonedDateTime.parse("2021-06-01T11:43:20Z"))
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
        assertThrows(IllegalArgumentException.class, () -> parser.toTimestamp(invalidDate));
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

    public void toIP_validIPv4Address() {
        CefParser parser = new CefParser(ZoneId.of("UTC"), true);
        String result = parser.toIP("192.168.1.1");
        assertEquals("192.168.1.1", result);
    }

    public void toIP_validIPv6Address() {
        CefParser parser = new CefParser(ZoneId.of("UTC"), true);
        String result = parser.toIP("2001:0db8:85a3:0000:0000:8a2e:0370:7334");
        assertEquals("2001:db8:85a3::8a2e:370:7334", result);
    }

    public void toIP_invalidIPAddress() {
        CefParser parser = new CefParser(ZoneId.of("UTC"), true);
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> { parser.toIP("invalid_ip"); });
        assertEquals("Invalid IP address format", exception.getMessage());
    }

    public void toIP_emptyString() {
        CefParser parser = new CefParser(ZoneId.of("UTC"), true);
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> { parser.toIP(""); });
        assertEquals("Invalid IP address format", exception.getMessage());
    }

    public void toIP_nullString() {
        CefParser parser = new CefParser(ZoneId.of("UTC"), true);
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> { parser.toIP(null); });
        assertEquals("Invalid IP address format", exception.getMessage());
    }
}
