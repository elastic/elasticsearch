/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.useragent;

import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.RandomDocumentPicks;
import org.elasticsearch.test.ESTestCase;
import org.junit.BeforeClass;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.ingest.IngestDocumentMatcher.assertIngestDocument;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;

public class UserAgentProcessorTests extends ESTestCase {

    private static UserAgentProcessor processor;

    @BeforeClass
    public static void setupProcessor() throws IOException {
        InputStream regexStream = UserAgentProcessor.class.getResourceAsStream("/regexes.yml");
        InputStream deviceTypeRegexStream = UserAgentProcessor.class.getResourceAsStream("/device_type_regexes.yml");

        assertNotNull(regexStream);
        assertNotNull(deviceTypeRegexStream);

        UserAgentParser parser = new UserAgentParser(randomAlphaOfLength(10), regexStream, deviceTypeRegexStream, new UserAgentCache(1000));

        processor = new UserAgentProcessor(
            randomAlphaOfLength(10),
            null,
            "source_field",
            "target_field",
            parser,
            EnumSet.allOf(UserAgentProcessor.Property.class),
            true,
            false
        );
    }

    public void testNullValueWithIgnoreMissing() throws Exception {
        UserAgentProcessor userAgentProcessor = new UserAgentProcessor(
            randomAlphaOfLength(10),
            null,
            "source_field",
            "target_field",
            null,
            EnumSet.allOf(UserAgentProcessor.Property.class),
            false,
            true
        );
        IngestDocument originalIngestDocument = RandomDocumentPicks.randomIngestDocument(
            random(),
            Collections.singletonMap("source_field", null)
        );
        IngestDocument ingestDocument = new IngestDocument(originalIngestDocument);
        userAgentProcessor.execute(ingestDocument);
        assertIngestDocument(originalIngestDocument, ingestDocument);
    }

    public void testNonExistentWithIgnoreMissing() throws Exception {
        UserAgentProcessor userAgentProcessor = new UserAgentProcessor(
            randomAlphaOfLength(10),
            null,
            "source_field",
            "target_field",
            null,
            EnumSet.allOf(UserAgentProcessor.Property.class),
            false,
            true
        );
        IngestDocument originalIngestDocument = RandomDocumentPicks.randomIngestDocument(random(), Collections.emptyMap());
        IngestDocument ingestDocument = new IngestDocument(originalIngestDocument);
        userAgentProcessor.execute(ingestDocument);
        assertIngestDocument(originalIngestDocument, ingestDocument);
    }

    public void testNullWithoutIgnoreMissing() throws Exception {
        UserAgentProcessor userAgentProcessor = new UserAgentProcessor(
            randomAlphaOfLength(10),
            null,
            "source_field",
            "target_field",
            null,
            EnumSet.allOf(UserAgentProcessor.Property.class),
            false,
            false
        );
        IngestDocument originalIngestDocument = RandomDocumentPicks.randomIngestDocument(
            random(),
            Collections.singletonMap("source_field", null)
        );
        IngestDocument ingestDocument = new IngestDocument(originalIngestDocument);
        Exception exception = expectThrows(Exception.class, () -> userAgentProcessor.execute(ingestDocument));
        assertThat(exception.getMessage(), equalTo("field [source_field] is null, cannot parse user-agent."));
    }

    public void testNonExistentWithoutIgnoreMissing() throws Exception {
        UserAgentProcessor userAgentProcessor = new UserAgentProcessor(
            randomAlphaOfLength(10),
            null,
            "source_field",
            "target_field",
            null,
            EnumSet.allOf(UserAgentProcessor.Property.class),
            false,
            false
        );
        IngestDocument originalIngestDocument = RandomDocumentPicks.randomIngestDocument(random(), Collections.emptyMap());
        IngestDocument ingestDocument = new IngestDocument(originalIngestDocument);
        Exception exception = expectThrows(Exception.class, () -> userAgentProcessor.execute(ingestDocument));
        assertThat(exception.getMessage(), equalTo("field [source_field] not present as part of path [source_field]"));
    }

    @SuppressWarnings("unchecked")
    public void testCommonBrowser() throws Exception {
        Map<String, Object> document = new HashMap<>();
        document.put(
            "source_field",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/33.0.1750.149 Safari/537.36"
        );
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);

        processor.execute(ingestDocument);
        Map<String, Object> data = ingestDocument.getSourceAndMetadata();

        assertThat(data, hasKey("target_field"));
        Map<String, Object> target = (Map<String, Object>) data.get("target_field");

        assertThat(target.get("name"), is("Chrome"));
        assertThat(target.get("version"), is("33.0.1750.149"));

        Map<String, String> os = new HashMap<>();
        os.put("name", "Mac OS X");
        os.put("version", "10.9.2");
        os.put("full", "Mac OS X 10.9.2");
        assertThat(target.get("os"), is(os));
        Map<String, String> device = new HashMap<>();
        device.put("name", "Mac");
        device.put("type", "Desktop");
        assertThat(target.get("device"), is(device));
    }

    @SuppressWarnings("unchecked")
    public void testWindowsOS() throws Exception {
        Map<String, Object> document = new HashMap<>();
        document.put(
            "source_field",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.141 Safari/537.36"
        );
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);

        processor.execute(ingestDocument);
        Map<String, Object> data = ingestDocument.getSourceAndMetadata();

        assertThat(data, hasKey("target_field"));
        Map<String, Object> target = (Map<String, Object>) data.get("target_field");

        assertThat(target.get("name"), is("Chrome"));
        assertThat(target.get("version"), is("87.0.4280.141"));

        Map<String, String> os = new HashMap<>();
        os.put("name", "Windows");
        os.put("version", "10");
        os.put("full", "Windows 10");
        assertThat(target.get("os"), is(os));
        Map<String, String> device = new HashMap<>();
        device.put("name", "Other");
        device.put("type", "Desktop");
        assertThat(target.get("device"), is(device));
    }

    @SuppressWarnings("unchecked")
    public void testUncommonDevice() throws Exception {
        Map<String, Object> document = new HashMap<>();
        document.put(
            "source_field",
            "Mozilla/5.0 (Linux; U; Android 3.0; en-us; Xoom Build/HRI39) AppleWebKit/525.10+ "
                + "(KHTML, like Gecko) Version/3.0.4 Mobile Safari/523.12.2"
        );
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);

        processor.execute(ingestDocument);
        Map<String, Object> data = ingestDocument.getSourceAndMetadata();

        assertThat(data, hasKey("target_field"));
        Map<String, Object> target = (Map<String, Object>) data.get("target_field");

        assertThat(target.get("name"), is("Android"));
        assertThat(target.get("version"), is("3.0"));

        Map<String, String> os = new HashMap<>();
        os.put("name", "Android");
        os.put("version", "3.0");
        os.put("full", "Android 3.0");
        assertThat(target.get("os"), is(os));

        Map<String, String> device = new HashMap<>();
        device.put("name", "Motorola Xoom");
        device.put("type", "Phone");
        assertThat(target.get("device"), is(device));
    }

    @SuppressWarnings("unchecked")
    public void testSpider() throws Exception {
        Map<String, Object> document = new HashMap<>();
        document.put("source_field", "Mozilla/5.0 (compatible; EasouSpider; +http://www.easou.com/search/spider.html)");
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);

        processor.execute(ingestDocument);
        Map<String, Object> data = ingestDocument.getSourceAndMetadata();

        assertThat(data, hasKey("target_field"));
        Map<String, Object> target = (Map<String, Object>) data.get("target_field");

        assertThat(target.get("name"), is("EasouSpider"));

        assertNull(target.get("version"));
        assertNull(target.get("os"));

        Map<String, String> device = new HashMap<>();
        device.put("name", "Spider");
        device.put("type", "Robot");
        assertThat(target.get("device"), is(device));
    }

    @SuppressWarnings("unchecked")
    public void testTablet() throws Exception {
        Map<String, Object> document = new HashMap<>();
        document.put(
            "source_field",
            "Mozilla/5.0 (iPad; CPU OS 12_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) "
                + "Version/12.1 Mobile/15E148 Safari/604.1"
        );
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);

        processor.execute(ingestDocument);
        Map<String, Object> data = ingestDocument.getSourceAndMetadata();

        assertThat(data, hasKey("target_field"));
        Map<String, Object> target = (Map<String, Object>) data.get("target_field");

        assertThat(target.get("name"), is("Mobile Safari"));

        assertThat(target.get("version"), is("12.1"));

        Map<String, String> os = new HashMap<>();
        os.put("name", "iOS");
        os.put("version", "12.2");
        os.put("full", "iOS 12.2");
        assertThat(target.get("os"), is(os));

        Map<String, String> device = new HashMap<>();
        device.put("name", "iPad");
        device.put("type", "Tablet");
        assertThat(target.get("device"), is(device));
    }

    @SuppressWarnings("unchecked")
    public void testUnknown() throws Exception {
        Map<String, Object> document = new HashMap<>();
        document.put("source_field", "Something I made up v42.0.1");
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);

        processor.execute(ingestDocument);
        Map<String, Object> data = ingestDocument.getSourceAndMetadata();

        assertThat(data, hasKey("target_field"));
        Map<String, Object> target = (Map<String, Object>) data.get("target_field");

        assertThat(target.get("name"), is("Other"));
        assertNull(target.get("major"));
        assertNull(target.get("minor"));
        assertNull(target.get("patch"));
        assertNull(target.get("build"));

        assertNull(target.get("os"));
        Map<String, String> device = new HashMap<>();
        device.put("name", "Other");
        device.put("type", "Other");
        assertThat(target.get("device"), is(device));
    }

    @SuppressWarnings("unchecked")
    public void testExtractDeviceTypeDisabled() {
        Map<String, Object> document = new HashMap<>();
        document.put("source_field", "Something I made up v42.0.1");
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);

        InputStream regexStream = UserAgentProcessor.class.getResourceAsStream("/regexes.yml");
        InputStream deviceTypeRegexStream = UserAgentProcessor.class.getResourceAsStream("/device_type_regexes.yml");
        UserAgentParser parser = new UserAgentParser(randomAlphaOfLength(10), regexStream, deviceTypeRegexStream, new UserAgentCache(1000));
        UserAgentProcessor userAgentProcessor = new UserAgentProcessor(
            randomAlphaOfLength(10),
            null,
            "source_field",
            "target_field",
            parser,
            EnumSet.allOf(UserAgentProcessor.Property.class),
            false,
            false
        );
        userAgentProcessor.execute(ingestDocument);
        Map<String, Object> data = ingestDocument.getSourceAndMetadata();

        assertThat(data, hasKey("target_field"));
        Map<String, Object> target = (Map<String, Object>) data.get("target_field");

        assertThat(target.get("name"), is("Other"));
        assertNull(target.get("major"));
        assertNull(target.get("minor"));
        assertNull(target.get("patch"));
        assertNull(target.get("build"));

        assertNull(target.get("os"));
        Map<String, String> device = new HashMap<>();
        device.put("name", "Other");
        assertThat(target.get("device"), is(device));
    }
}
