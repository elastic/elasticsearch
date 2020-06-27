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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.ingest.IngestDocumentMatcher.assertIngestDocument;
import static org.hamcrest.Matchers.equalTo;

public class KeyValueProcessorTests extends ESTestCase {

    private static final KeyValueProcessor.Factory FACTORY = new KeyValueProcessor.Factory();

    public void test() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        String fieldName = RandomDocumentPicks.addRandomField(random(), ingestDocument, "first=hello&second=world&second=universe");
        Processor processor = createKvProcessor(fieldName, "&", "=", null, null, "target", false);
        processor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue("target.first", String.class), equalTo("hello"));
        assertThat(ingestDocument.getFieldValue("target.second", List.class), equalTo(Arrays.asList("world", "universe")));
    }

    public void testRootTarget() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), Collections.emptyMap());
        ingestDocument.setFieldValue("myField", "first=hello&second=world&second=universe");
        Processor processor = createKvProcessor("myField", "&", "=", null, null,null, false);
        processor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue("first", String.class), equalTo("hello"));
        assertThat(ingestDocument.getFieldValue("second", List.class), equalTo(Arrays.asList("world", "universe")));
    }

    public void testKeySameAsSourceField() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), Collections.emptyMap());
        ingestDocument.setFieldValue("first", "first=hello");
        Processor processor = createKvProcessor("first", "&", "=", null, null,null, false);
        processor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue("first", List.class), equalTo(Arrays.asList("first=hello", "hello")));
    }

    public void testIncludeKeys() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        String fieldName = RandomDocumentPicks.addRandomField(random(), ingestDocument, "first=hello&second=world&second=universe");
        Processor processor = createKvProcessor(fieldName, "&", "=",
            Sets.newHashSet("first"), null, "target", false);
        processor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue("target.first", String.class), equalTo("hello"));
        assertFalse(ingestDocument.hasField("target.second"));
    }

    public void testExcludeKeys() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        String fieldName = RandomDocumentPicks.addRandomField(random(), ingestDocument, "first=hello&second=world&second=universe");
        Processor processor = createKvProcessor(fieldName, "&", "=",
            null, Sets.newHashSet("second"), "target", false);
        processor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue("target.first", String.class), equalTo("hello"));
        assertFalse(ingestDocument.hasField("target.second"));
    }

    public void testIncludeAndExcludeKeys() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        String fieldName = RandomDocumentPicks.addRandomField(random(), ingestDocument,
            "first=hello&second=world&second=universe&third=bar");
        Processor processor = createKvProcessor(fieldName, "&", "=",
            Sets.newHashSet("first", "second"), Sets.newHashSet("first", "second"), "target", false);
        processor.execute(ingestDocument);
        assertFalse(ingestDocument.hasField("target.first"));
        assertFalse(ingestDocument.hasField("target.second"));
        assertFalse(ingestDocument.hasField("target.third"));
    }

    public void testMissingField() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), Collections.emptyMap());
        Processor processor = createKvProcessor("unknown", "&",
            "=", null, null, "target", false);
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> processor.execute(ingestDocument));
        assertThat(exception.getMessage(), equalTo("field [unknown] not present as part of path [unknown]"));
    }

    public void testNullValueWithIgnoreMissing() throws Exception {
        String fieldName = RandomDocumentPicks.randomFieldName(random());
        IngestDocument originalIngestDocument = RandomDocumentPicks.randomIngestDocument(random(),
            Collections.singletonMap(fieldName, null));
        IngestDocument ingestDocument = new IngestDocument(originalIngestDocument);
        Processor processor = createKvProcessor(fieldName, "", "", null, null, "target", true);
        processor.execute(ingestDocument);
        assertIngestDocument(originalIngestDocument, ingestDocument);
    }

    public void testNonExistentWithIgnoreMissing() throws Exception {
        IngestDocument originalIngestDocument = RandomDocumentPicks.randomIngestDocument(random(), Collections.emptyMap());
        IngestDocument ingestDocument = new IngestDocument(originalIngestDocument);
        Processor processor = createKvProcessor("unknown", "", "", null, null, "target", true);
        processor.execute(ingestDocument);
        assertIngestDocument(originalIngestDocument, ingestDocument);
    }

    public void testFailFieldSplitMatch() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        String fieldName = RandomDocumentPicks.addRandomField(random(), ingestDocument, "first=hello|second=world|second=universe");
        Processor processor = createKvProcessor(fieldName, "&", "=", null, null, "target", false);
        processor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue("target.first", String.class), equalTo("hello|second=world|second=universe"));
        assertFalse(ingestDocument.hasField("target.second"));
    }

    public void testFailValueSplitMatch() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), Collections.singletonMap("foo", "bar"));
        Processor processor = createKvProcessor("foo", "&", "=", null, null, "target", false);
        Exception exception = expectThrows(IllegalArgumentException.class, () -> processor.execute(ingestDocument));
        assertThat(exception.getMessage(), equalTo("field [foo] does not contain value_split [=]"));
    }

    public void testTrimKeyAndValue() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        String fieldName = RandomDocumentPicks.addRandomField(random(), ingestDocument, "first= hello &second=world& second =universe");
        Processor processor = createKvProcessor(fieldName, "&", "=", null, null, "target", false, " ", " ", false, null);
        processor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue("target.first", String.class), equalTo("hello"));
        assertThat(ingestDocument.getFieldValue("target.second", List.class), equalTo(Arrays.asList("world", "universe")));
    }

    public void testTrimMultiCharSequence() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        String fieldName = RandomDocumentPicks.addRandomField(random(), ingestDocument,
            "to=<foo@example.com>, orig_to=<bar@example.com>, %+relay=mail.example.com[private/dovecot-lmtp]," +
                " delay=2.2, delays=1.9/0.01/0.01/0.21, dsn=2.0.0, status=sent "
        );
        Processor processor = createKvProcessor(fieldName, " ", "=", null, null, "target", false, "%+", "<>,", false, null);
        processor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue("target.to", String.class), equalTo("foo@example.com"));
        assertThat(ingestDocument.getFieldValue("target.orig_to", String.class), equalTo("bar@example.com"));
        assertThat(ingestDocument.getFieldValue("target.relay", String.class), equalTo("mail.example.com[private/dovecot-lmtp]"));
        assertThat(ingestDocument.getFieldValue("target.delay", String.class), equalTo("2.2"));
        assertThat(ingestDocument.getFieldValue("target.delays", String.class), equalTo("1.9/0.01/0.01/0.21"));
        assertThat(ingestDocument.getFieldValue("target.dsn", String.class), equalTo("2.0.0"));
        assertThat(ingestDocument.getFieldValue("target.status", String.class), equalTo("sent"));
    }

    public void testStripBrackets() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        String fieldName = RandomDocumentPicks.addRandomField(
            random(), ingestDocument, "first=<hello>&second=\"world\"&second=(universe)&third=<foo>&fourth=[bar]&fifth='last'"
        );
        Processor processor = createKvProcessor(fieldName, "&", "=", null, null, "target", false, null, null, true, null);
        processor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue("target.first", String.class), equalTo("hello"));
        assertThat(ingestDocument.getFieldValue("target.second", List.class), equalTo(Arrays.asList("world", "universe")));
        assertThat(ingestDocument.getFieldValue("target.third", String.class), equalTo("foo"));
        assertThat(ingestDocument.getFieldValue("target.fourth", String.class), equalTo("bar"));
        assertThat(ingestDocument.getFieldValue("target.fifth", String.class), equalTo("last"));
    }

    public void testAddPrefix() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        String fieldName = RandomDocumentPicks.addRandomField(random(), ingestDocument, "first=hello&second=world&second=universe");
        Processor processor = createKvProcessor(fieldName, "&", "=", null, null, "target", false, null, null, false, "arg_");
        processor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue("target.arg_first", String.class), equalTo("hello"));
        assertThat(ingestDocument.getFieldValue("target.arg_second", List.class), equalTo(Arrays.asList("world", "universe")));
    }

    private static KeyValueProcessor createKvProcessor(String field, String fieldSplit, String valueSplit, Set<String> includeKeys,
                                                       Set<String> excludeKeys, String targetField,
                                                       boolean ignoreMissing) throws Exception {
        return createKvProcessor(
            field, fieldSplit, valueSplit, includeKeys, excludeKeys, targetField, ignoreMissing, null, null, false, null
        );
    }

    private static KeyValueProcessor createKvProcessor(String field, String fieldSplit, String valueSplit, Set<String> includeKeys,
                                                       Set<String> excludeKeys, String targetField, boolean ignoreMissing,
                                                       String trimKey, String trimValue, boolean stripBrackets,
                                                       String prefix) throws Exception {
        Map<String, Object> config = new HashMap<>();
        config.put("field", field);
        config.put("field_split", fieldSplit);
        config.put("value_split", valueSplit);
        config.put("target_field", targetField);
        if (includeKeys != null) {
            config.put("include_keys", new ArrayList<>(includeKeys));
        }
        if (excludeKeys != null) {
            config.put("exclude_keys", new ArrayList<>(excludeKeys));
        }
        config.put("ignore_missing", ignoreMissing);
        if (trimKey != null) {
            config.put("trim_key", trimKey);
        }
        if (trimValue != null) {
            config.put("trim_value", trimValue);
        }
        config.put("strip_brackets", stripBrackets);
        if (prefix != null) {
            config.put("prefix", prefix);
        }
        return FACTORY.create(null, randomAlphaOfLength(10), null, config);
    }
}
