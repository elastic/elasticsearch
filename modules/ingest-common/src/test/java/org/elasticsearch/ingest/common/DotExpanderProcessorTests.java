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

import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Processor;
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
        DotExpanderProcessor processor = new DotExpanderProcessor("_tag", null, "foo.bar");
        processor.execute(document);
        assertThat(document.getFieldValue("foo", Map.class).size(), equalTo(1));
        assertThat(document.getFieldValue("foo.bar", String.class), equalTo("baz1"));

        source = new HashMap<>();
        source.put("foo.bar.baz", "value");
        document = new IngestDocument(source, Collections.emptyMap());
        processor = new DotExpanderProcessor("_tag", null, "foo.bar.baz");
        processor.execute(document);
        assertThat(document.getFieldValue("foo", Map.class).size(), equalTo(1));
        assertThat(document.getFieldValue("foo.bar", Map.class).size(), equalTo(1));
        assertThat(document.getFieldValue("foo.bar.baz", String.class), equalTo("value"));

        source = new HashMap<>();
        source.put("foo.bar", "baz1");
        source.put("foo", new HashMap<>(Collections.singletonMap("bar", "baz2")));
        document = new IngestDocument(source, Collections.emptyMap());
        processor = new DotExpanderProcessor("_tag", null, "foo.bar");
        processor.execute(document);
        assertThat(document.getSourceAndMetadata().size(), equalTo(1));
        assertThat(document.getFieldValue("foo.bar", List.class).size(), equalTo(2));
        assertThat(document.getFieldValue("foo.bar.0", String.class), equalTo("baz2"));
        assertThat(document.getFieldValue("foo.bar.1", String.class), equalTo("baz1"));

        source = new HashMap<>();
        source.put("foo.bar", "2");
        source.put("foo", new HashMap<>(Collections.singletonMap("bar", 1)));
        document = new IngestDocument(source, Collections.emptyMap());
        processor = new DotExpanderProcessor("_tag", null, "foo.bar");
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
        Processor processor1 = new DotExpanderProcessor("_tag", null, "foo.bar");
        // foo already exists and if a leaf field and therefor can't be replaced by a map field:
        Exception e = expectThrows(IllegalArgumentException.class, () -> processor1.execute(document1));
        assertThat(e.getMessage(), equalTo("cannot expend [foo.bar], because [foo] is not an object field, but a value field"));

        // so because foo is no branch field but a value field the `foo.bar` field can't be expanded
        // into [foo].[bar], so foo should be renamed first into `[foo].[bar]:
        IngestDocument document = new IngestDocument(source, Collections.emptyMap());
        Processor processor = new RenameProcessor("_tag", "foo", "foo.bar", false);
        processor.execute(document);
        processor = new DotExpanderProcessor("_tag", null, "foo.bar");
        processor.execute(document);
        assertThat(document.getFieldValue("foo", Map.class).size(), equalTo(1));
        assertThat(document.getFieldValue("foo.bar.0", String.class), equalTo("baz2"));
        assertThat(document.getFieldValue("foo.bar.1", String.class), equalTo("baz1"));

        source = new HashMap<>();
        source.put("foo.bar", "baz1");
        document = new IngestDocument(source, Collections.emptyMap());
        processor = new DotExpanderProcessor("_tag", null, "foo.bar");
        processor.execute(document);
        assertThat(document.getFieldValue("foo", Map.class).size(), equalTo(1));
        assertThat(document.getFieldValue("foo.bar", String.class), equalTo("baz1"));

        source = new HashMap<>();
        source.put("foo.bar.baz", "baz1");
        source.put("foo", new HashMap<>(Collections.singletonMap("bar", new HashMap<>())));
        document = new IngestDocument(source, Collections.emptyMap());
        processor = new DotExpanderProcessor("_tag", null, "foo.bar.baz");
        processor.execute(document);
        assertThat(document.getFieldValue("foo", Map.class).size(), equalTo(1));
        assertThat(document.getFieldValue("foo.bar", Map.class).size(), equalTo(1));
        assertThat(document.getFieldValue("foo.bar.baz", String.class), equalTo("baz1"));

        source = new HashMap<>();
        source.put("foo.bar.baz", "baz1");
        source.put("foo", new HashMap<>(Collections.singletonMap("bar", "baz2")));
        IngestDocument document2 = new IngestDocument(source, Collections.emptyMap());
        Processor processor2 = new DotExpanderProcessor("_tag", null, "foo.bar.baz");
        e = expectThrows(IllegalArgumentException.class, () -> processor2.execute(document2));
        assertThat(e.getMessage(), equalTo("cannot expend [foo.bar.baz], because [foo.bar] is not an object field, but a value field"));
    }

    public void testEscapeFields_path() throws Exception {
        Map<String, Object> source = new HashMap<>();
        source.put("foo", new HashMap<>(Collections.singletonMap("bar.baz", "value")));
        IngestDocument document = new IngestDocument(source, Collections.emptyMap());
        DotExpanderProcessor processor = new DotExpanderProcessor("_tag", "foo", "bar.baz");
        processor.execute(document);
        assertThat(document.getFieldValue("foo", Map.class).size(), equalTo(1));
        assertThat(document.getFieldValue("foo.bar", Map.class).size(), equalTo(1));
        assertThat(document.getFieldValue("foo.bar.baz", String.class), equalTo("value"));

        source = new HashMap<>();
        source.put("field", new HashMap<>(Collections.singletonMap("foo.bar.baz", "value")));
        document = new IngestDocument(source, Collections.emptyMap());
        processor = new DotExpanderProcessor("_tag", "field", "foo.bar.baz");
        processor.execute(document);
        assertThat(document.getFieldValue("field.foo", Map.class).size(), equalTo(1));
        assertThat(document.getFieldValue("field.foo.bar", Map.class).size(), equalTo(1));
        assertThat(document.getFieldValue("field.foo.bar.baz", String.class), equalTo("value"));
    }

}
