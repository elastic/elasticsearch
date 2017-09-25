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

package org.elasticsearch.ingest.common;

import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.ingest.RandomDocumentPicks;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.ingest.IngestDocumentMatcher.assertIngestDocument;
import static org.hamcrest.Matchers.equalTo;

public class KeyValueProcessorTests extends ESTestCase {

    public void test() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        String fieldName = RandomDocumentPicks.addRandomField(random(), ingestDocument, "first=hello&second=world&second=universe");
        Processor processor = new KeyValueProcessor(randomAlphaOfLength(10), fieldName, "&", "=", null, null, "target", false);
        processor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue("target.first", String.class), equalTo("hello"));
        assertThat(ingestDocument.getFieldValue("target.second", List.class), equalTo(Arrays.asList("world", "universe")));
    }

    public void testRootTarget() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), Collections.emptyMap());
        ingestDocument.setFieldValue("myField", "first=hello&second=world&second=universe");
        Processor processor = new KeyValueProcessor(randomAlphaOfLength(10), "myField", "&", "=", null, null,null, false);
        processor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue("first", String.class), equalTo("hello"));
        assertThat(ingestDocument.getFieldValue("second", List.class), equalTo(Arrays.asList("world", "universe")));
    }

    public void testKeySameAsSourceField() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), Collections.emptyMap());
        ingestDocument.setFieldValue("first", "first=hello");
        Processor processor = new KeyValueProcessor(randomAlphaOfLength(10), "first", "&", "=", null, null,null, false);
        processor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue("first", List.class), equalTo(Arrays.asList("first=hello", "hello")));
    }

    public void testIncludeKeys() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        String fieldName = RandomDocumentPicks.addRandomField(random(), ingestDocument, "first=hello&second=world&second=universe");
        Processor processor = new KeyValueProcessor(randomAlphaOfLength(10), fieldName, "&", "=",
            Sets.newHashSet("first"), null, "target", false);
        processor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue("target.first", String.class), equalTo("hello"));
        assertFalse(ingestDocument.hasField("target.second"));
    }

    public void testExcludeKeys() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        String fieldName = RandomDocumentPicks.addRandomField(random(), ingestDocument, "first=hello&second=world&second=universe");
        Processor processor = new KeyValueProcessor(randomAlphaOfLength(10), fieldName, "&", "=",
            null, Sets.newHashSet("second"), "target", false);
        processor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue("target.first", String.class), equalTo("hello"));
        assertFalse(ingestDocument.hasField("target.second"));
    }

    public void testIncludeAndExcludeKeys() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        String fieldName = RandomDocumentPicks.addRandomField(random(), ingestDocument,
            "first=hello&second=world&second=universe&third=bar");
        Processor processor = new KeyValueProcessor(randomAlphaOfLength(10), fieldName, "&", "=",
            Sets.newHashSet("first", "second"), Sets.newHashSet("first", "second"), "target", false);
        processor.execute(ingestDocument);
        assertFalse(ingestDocument.hasField("target.first"));
        assertFalse(ingestDocument.hasField("target.second"));
        assertFalse(ingestDocument.hasField("target.third"));
    }

    public void testMissingField() {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), Collections.emptyMap());
        Processor processor = new KeyValueProcessor(randomAlphaOfLength(10), "unknown", "&",
            "=", null, null, "target", false);
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> processor.execute(ingestDocument));
        assertThat(exception.getMessage(), equalTo("field [unknown] not present as part of path [unknown]"));
    }

    public void testNullValueWithIgnoreMissing() throws Exception {
        String fieldName = RandomDocumentPicks.randomFieldName(random());
        IngestDocument originalIngestDocument = RandomDocumentPicks.randomIngestDocument(random(),
            Collections.singletonMap(fieldName, null));
        IngestDocument ingestDocument = new IngestDocument(originalIngestDocument);
        Processor processor = new KeyValueProcessor(randomAlphaOfLength(10), fieldName, "", "", null, null, "target", true);
        processor.execute(ingestDocument);
        assertIngestDocument(originalIngestDocument, ingestDocument);
    }

    public void testNonExistentWithIgnoreMissing() throws Exception {
        IngestDocument originalIngestDocument = RandomDocumentPicks.randomIngestDocument(random(), Collections.emptyMap());
        IngestDocument ingestDocument = new IngestDocument(originalIngestDocument);
        Processor processor = new KeyValueProcessor(randomAlphaOfLength(10), "unknown", "", "", null, null, "target", true);
        processor.execute(ingestDocument);
        assertIngestDocument(originalIngestDocument, ingestDocument);
    }

    public void testFailFieldSplitMatch() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        String fieldName = RandomDocumentPicks.addRandomField(random(), ingestDocument, "first=hello|second=world|second=universe");
        Processor processor = new KeyValueProcessor(randomAlphaOfLength(10), fieldName, "&", "=", null, null, "target", false);
        processor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue("target.first", String.class), equalTo("hello|second=world|second=universe"));
        assertFalse(ingestDocument.hasField("target.second"));
    }

    public void testFailValueSplitMatch() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), Collections.singletonMap("foo", "bar"));
        Processor processor = new KeyValueProcessor(randomAlphaOfLength(10), "foo", "&", "=", null, null, "target", false);
        Exception exception = expectThrows(IllegalArgumentException.class, () -> processor.execute(ingestDocument));
        assertThat(exception.getMessage(), equalTo("field [foo] does not contain value_split [=]"));
    }
}
