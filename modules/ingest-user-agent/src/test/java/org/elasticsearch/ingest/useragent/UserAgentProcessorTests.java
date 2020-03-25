/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.ingest.useragent;

import org.elasticsearch.ingest.RandomDocumentPicks;
import org.elasticsearch.ingest.IngestDocument;
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
        assertNotNull(regexStream);

        UserAgentParser parser = new UserAgentParser(randomAlphaOfLength(10), regexStream, new UserAgentCache(1000));

        processor = new UserAgentProcessor(randomAlphaOfLength(10), "source_field", "target_field", parser,
                EnumSet.allOf(UserAgentProcessor.Property.class), false);
    }

    public void testNullValueWithIgnoreMissing() throws Exception {
        UserAgentProcessor processor = new UserAgentProcessor(randomAlphaOfLength(10), "source_field", "target_field", null,
            EnumSet.allOf(UserAgentProcessor.Property.class), true);
        IngestDocument originalIngestDocument = RandomDocumentPicks.randomIngestDocument(random(),
            Collections.singletonMap("source_field", null));
        IngestDocument ingestDocument = new IngestDocument(originalIngestDocument);
        processor.execute(ingestDocument);
        assertIngestDocument(originalIngestDocument, ingestDocument);
    }

    public void testNonExistentWithIgnoreMissing() throws Exception {
        UserAgentProcessor processor = new UserAgentProcessor(randomAlphaOfLength(10), "source_field", "target_field", null,
            EnumSet.allOf(UserAgentProcessor.Property.class), true);
        IngestDocument originalIngestDocument = RandomDocumentPicks.randomIngestDocument(random(), Collections.emptyMap());
        IngestDocument ingestDocument = new IngestDocument(originalIngestDocument);
        processor.execute(ingestDocument);
        assertIngestDocument(originalIngestDocument, ingestDocument);
    }

    public void testNullWithoutIgnoreMissing() throws Exception {
        UserAgentProcessor processor = new UserAgentProcessor(randomAlphaOfLength(10), "source_field", "target_field", null,
            EnumSet.allOf(UserAgentProcessor.Property.class), false);
        IngestDocument originalIngestDocument = RandomDocumentPicks.randomIngestDocument(random(),
            Collections.singletonMap("source_field", null));
        IngestDocument ingestDocument = new IngestDocument(originalIngestDocument);
        Exception exception = expectThrows(Exception.class, () -> processor.execute(ingestDocument));
        assertThat(exception.getMessage(), equalTo("field [source_field] is null, cannot parse user-agent."));
    }

    public void testNonExistentWithoutIgnoreMissing() throws Exception {
        UserAgentProcessor processor = new UserAgentProcessor(randomAlphaOfLength(10), "source_field", "target_field", null,
            EnumSet.allOf(UserAgentProcessor.Property.class), false);
        IngestDocument originalIngestDocument = RandomDocumentPicks.randomIngestDocument(random(), Collections.emptyMap());
        IngestDocument ingestDocument = new IngestDocument(originalIngestDocument);
        Exception exception = expectThrows(Exception.class, () -> processor.execute(ingestDocument));
        assertThat(exception.getMessage(), equalTo("field [source_field] not present as part of path [source_field]"));
    }

    @SuppressWarnings("unchecked")
    public void testCommonBrowser() throws Exception {
        Map<String, Object> document = new HashMap<>();
        document.put("source_field",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/33.0.1750.149 Safari/537.36");
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
        device.put("name", "Other");
        assertThat(target.get("device"), is(device));
    }

    @SuppressWarnings("unchecked")
    public void testUncommonDevice() throws Exception {
        Map<String, Object> document = new HashMap<>();
        document.put("source_field",
                "Mozilla/5.0 (Linux; U; Android 3.0; en-us; Xoom Build/HRI39) AppleWebKit/525.10+ "
                + "(KHTML, like Gecko) Version/3.0.4 Mobile Safari/523.12.2");
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
        assertThat(target.get("device"), is(device));
    }

    @SuppressWarnings("unchecked")
    public void testSpider() throws Exception {
        Map<String, Object> document = new HashMap<>();
        document.put("source_field",
            "Mozilla/5.0 (compatible; EasouSpider; +http://www.easou.com/search/spider.html)");
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
        assertThat(target.get("device"), is(device));
    }

    @SuppressWarnings("unchecked")
    public void testUnknown() throws Exception {
        Map<String, Object> document = new HashMap<>();
        document.put("source_field",
            "Something I made up v42.0.1");
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
        assertThat(target.get("device"), is(device));
    }
}
