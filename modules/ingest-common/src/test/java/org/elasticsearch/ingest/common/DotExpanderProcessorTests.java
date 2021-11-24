/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.common;

import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.ingest.TestTemplateService;
import org.elasticsearch.test.ESTestCase;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class DotExpanderProcessorTests extends ESTestCase {

    public void testEscapeFields() throws Exception {
        Map<String, Object> source = new HashMap<>();
        source.put("foo.bar", "baz1");
        IngestDocument document = new IngestDocument(source, Collections.emptyMap());
        DotExpanderProcessor processor = new DotExpanderProcessor("_tag", null, null, "foo.bar");
        processor.execute(document);
        assertThat(document.getFieldValue("foo", Map.class).size(), equalTo(1));
        assertThat(document.getFieldValue("foo.bar", String.class), equalTo("baz1"));

        source = new HashMap<>();
        source.put("foo.bar.baz", "value");
        document = new IngestDocument(source, Collections.emptyMap());
        processor = new DotExpanderProcessor("_tag", null, null, "foo.bar.baz");
        processor.execute(document);
        assertThat(document.getFieldValue("foo", Map.class).size(), equalTo(1));
        assertThat(document.getFieldValue("foo.bar", Map.class).size(), equalTo(1));
        assertThat(document.getFieldValue("foo.bar.baz", String.class), equalTo("value"));

        source = new HashMap<>();
        source.put("foo.bar", "baz1");
        source.put("foo", new HashMap<>(Collections.singletonMap("bar", "baz2")));
        document = new IngestDocument(source, Collections.emptyMap());
        processor = new DotExpanderProcessor("_tag", null, null, "foo.bar");
        processor.execute(document);
        assertThat(document.getSourceAndMetadata().size(), equalTo(1));
        assertThat(document.getFieldValue("foo.bar", List.class).size(), equalTo(2));
        assertThat(document.getFieldValue("foo.bar.0", String.class), equalTo("baz2"));
        assertThat(document.getFieldValue("foo.bar.1", String.class), equalTo("baz1"));

        source = new HashMap<>();
        source.put("foo.bar", "2");
        source.put("foo", new HashMap<>(Collections.singletonMap("bar", 1)));
        document = new IngestDocument(source, Collections.emptyMap());
        processor = new DotExpanderProcessor("_tag", null, null, "foo.bar");
        processor.execute(document);
        assertThat(document.getSourceAndMetadata().size(), equalTo(1));
        assertThat(document.getFieldValue("foo.bar", List.class).size(), equalTo(2));
        assertThat(document.getFieldValue("foo.bar.0", Integer.class), equalTo(1));
        assertThat(document.getFieldValue("foo.bar.1", String.class), equalTo("2"));
    }

    public void testEscapeFields_valueField() throws Exception {
        Map<String, Object> source = new HashMap<>();
        source.put("foo.bar", "baz1");
        source.put("foo", "baz2");
        IngestDocument document1 = new IngestDocument(source, Collections.emptyMap());
        Processor processor1 = new DotExpanderProcessor("_tag", null, null, "foo.bar");
        // foo already exists and if a leaf field and therefor can't be replaced by a map field:
        Exception e = expectThrows(IllegalArgumentException.class, () -> processor1.execute(document1));
        assertThat(e.getMessage(), equalTo("cannot expand [foo.bar], because [foo] is not an object field, but a value field"));

        // so because foo is no branch field but a value field the `foo.bar` field can't be expanded
        // into [foo].[bar], so foo should be renamed first into `[foo].[bar]:
        IngestDocument document = new IngestDocument(source, Collections.emptyMap());
        Processor processor = new RenameProcessor(
            "_tag",
            null,
            new TestTemplateService.MockTemplateScript.Factory("foo"),
            new TestTemplateService.MockTemplateScript.Factory("foo.bar"),
            false
        );
        processor.execute(document);
        processor = new DotExpanderProcessor("_tag", null, null, "foo.bar");
        processor.execute(document);
        assertThat(document.getFieldValue("foo", Map.class).size(), equalTo(1));
        assertThat(document.getFieldValue("foo.bar.0", String.class), equalTo("baz2"));
        assertThat(document.getFieldValue("foo.bar.1", String.class), equalTo("baz1"));

        source = new HashMap<>();
        source.put("foo.bar", "baz1");
        document = new IngestDocument(source, Collections.emptyMap());
        processor = new DotExpanderProcessor("_tag", null, null, "foo.bar");
        processor.execute(document);
        assertThat(document.getFieldValue("foo", Map.class).size(), equalTo(1));
        assertThat(document.getFieldValue("foo.bar", String.class), equalTo("baz1"));

        source = new HashMap<>();
        source.put("foo.bar.baz", "baz1");
        source.put("foo", new HashMap<>(Collections.singletonMap("bar", new HashMap<>())));
        document = new IngestDocument(source, Collections.emptyMap());
        processor = new DotExpanderProcessor("_tag", null, null, "foo.bar.baz");
        processor.execute(document);
        assertThat(document.getFieldValue("foo", Map.class).size(), equalTo(1));
        assertThat(document.getFieldValue("foo.bar", Map.class).size(), equalTo(1));
        assertThat(document.getFieldValue("foo.bar.baz", String.class), equalTo("baz1"));

        source = new HashMap<>();
        source.put("foo.bar.baz", "baz1");
        source.put("foo", new HashMap<>(Collections.singletonMap("bar", "baz2")));
        IngestDocument document2 = new IngestDocument(source, Collections.emptyMap());
        Processor processor2 = new DotExpanderProcessor("_tag", null, null, "foo.bar.baz");
        e = expectThrows(IllegalArgumentException.class, () -> processor2.execute(document2));
        assertThat(e.getMessage(), equalTo("cannot expand [foo.bar.baz], because [foo.bar] is not an object field, but a value field"));
    }

    public void testEscapeFields_path() throws Exception {
        Map<String, Object> source = new HashMap<>();
        source.put("foo", new HashMap<>(Collections.singletonMap("bar.baz", "value")));
        IngestDocument document = new IngestDocument(source, Collections.emptyMap());
        DotExpanderProcessor processor = new DotExpanderProcessor("_tag", null, "foo", "bar.baz");
        processor.execute(document);
        assertThat(document.getFieldValue("foo", Map.class).size(), equalTo(1));
        assertThat(document.getFieldValue("foo.bar", Map.class).size(), equalTo(1));
        assertThat(document.getFieldValue("foo.bar.baz", String.class), equalTo("value"));

        source = new HashMap<>();
        source.put("field", new HashMap<>(Collections.singletonMap("foo.bar.baz", "value")));
        document = new IngestDocument(source, Collections.emptyMap());
        processor = new DotExpanderProcessor("_tag", null, "field", "foo.bar.baz");
        processor.execute(document);
        assertThat(document.getFieldValue("field.foo", Map.class).size(), equalTo(1));
        assertThat(document.getFieldValue("field.foo.bar", Map.class).size(), equalTo(1));
        assertThat(document.getFieldValue("field.foo.bar.baz", String.class), equalTo("value"));
    }

    public void testEscapeFields_doNothingIfFieldNotInSourceDoc() throws Exception {
        // asking to expand a (literal) field that is not present in the source document
        Map<String, Object> source = new HashMap<>();
        source.put("foo.bar", "baz1");
        IngestDocument document = new IngestDocument(source, Collections.emptyMap());
        // abc.def does not exist in source, so don't mutate document
        DotExpanderProcessor processor = new DotExpanderProcessor("_tag", null, null, "abc.def");
        processor.execute(document);
        // hasField returns false since it requires the expanded form, which is not expanded since we did not ask for it to be
        assertFalse(document.hasField("foo.bar"));
        // nothing has changed
        assertEquals(document.getSourceAndMetadata().get("foo.bar"), "baz1");
        // abc.def is not found anywhere
        assertFalse(document.hasField("abc.def"));
        assertFalse(document.getSourceAndMetadata().containsKey("abc"));
        assertFalse(document.getSourceAndMetadata().containsKey("abc.def"));

        // asking to expand a (literal) field that does not exist, but the nested field does exist
        source = new HashMap<>();
        Map<String, Object> inner = new HashMap<>();
        inner.put("bar", "baz1");
        source.put("foo", inner);
        document = new IngestDocument(source, Collections.emptyMap());
        // foo.bar, the literal value (as opposed to nested value) does not exist in source, so don't mutate document
        processor = new DotExpanderProcessor("_tag", null, null, "foo.bar");
        processor.execute(document);
        // hasField returns true because the nested/expanded form exists in the source document
        assertTrue(document.hasField("foo.bar"));
        // nothing changed
        assertThat(document.getFieldValue("foo", Map.class).size(), equalTo(1));
        assertThat(document.getFieldValue("foo.bar", String.class), equalTo("baz1"));
    }

    public void testOverride() throws Exception {
        Map<String, Object> source = new HashMap<>();
        Map<String, Object> inner = new HashMap<>();
        inner.put("bar", "baz1");
        inner.put("qux", "quux");
        source.put("foo", inner);
        source.put("foo.bar", "baz2");
        IngestDocument document = new IngestDocument(source, Map.of());
        DotExpanderProcessor processor = new DotExpanderProcessor("_tag", null, null, "foo.bar", true);
        processor.execute(document);
        assertThat(document.getFieldValue("foo", Map.class).size(), equalTo(2));
        assertThat(document.getFieldValue("foo.bar", String.class), equalTo("baz2"));
        assertThat(document.getFieldValue("foo.qux", String.class), equalTo("quux"));
    }

    public void testWildcard() throws Exception {
        Map<String, Object> source = new HashMap<>();
        source.put("foo.bar", "baz");
        source.put("qux.quux", "corge");
        IngestDocument document = new IngestDocument(source, Map.of());
        DotExpanderProcessor processor = new DotExpanderProcessor("_tag", null, null, "*");
        processor.execute(document);
        assertThat(document.getFieldValue("foo", Map.class).size(), equalTo(1));
        assertThat(document.getFieldValue("foo.bar", String.class), equalTo("baz"));
        assertThat(document.getFieldValue("qux", Map.class).size(), equalTo(1));
        assertThat(document.getFieldValue("qux.quux", String.class), equalTo("corge"));

        source = new HashMap<>();
        Map<String, Object> inner = new HashMap<>();
        inner.put("bar.baz", "qux");
        source.put("foo", inner);
        document = new IngestDocument(source, Map.of());
        processor = new DotExpanderProcessor("_tag", null, "foo", "*");
        processor.execute(document);
        assertThat(document.getFieldValue("foo", Map.class).size(), equalTo(1));
        assertThat(document.getFieldValue("foo.bar", Map.class).size(), equalTo(1));
        assertThat(document.getFieldValue("foo.bar.baz", String.class), equalTo("qux"));

        source = new HashMap<>();
        inner = new HashMap<>();
        inner.put("bar.baz", "qux");
        source.put("foo", inner);
        document = new IngestDocument(source, Map.of());
        processor = new DotExpanderProcessor("_tag", null, null, "*");
        processor.execute(document);
        assertThat(document.getFieldValue("foo", Map.class).size(), equalTo(1));
        IngestDocument finalDocument = document;
        expectThrows(IllegalArgumentException.class, () -> finalDocument.getFieldValue("foo.bar", Map.class));
    }

}
