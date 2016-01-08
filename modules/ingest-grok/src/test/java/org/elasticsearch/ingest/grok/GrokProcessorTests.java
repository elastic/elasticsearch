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

package org.elasticsearch.ingest.grok;

import org.elasticsearch.ingest.RandomDocumentPicks;
import org.elasticsearch.ingest.core.IngestDocument;
import org.elasticsearch.ingest.grok.Grok;
import org.elasticsearch.ingest.grok.GrokProcessor;
import org.elasticsearch.test.ESTestCase;

import java.util.Collections;
import java.util.HashMap;

import static org.hamcrest.Matchers.equalTo;


public class GrokProcessorTests extends ESTestCase {

    public void testMatch() throws Exception {
        String fieldName = RandomDocumentPicks.randomFieldName(random());
        IngestDocument doc = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        doc.setFieldValue(fieldName, "1");
        Grok grok = new Grok(Collections.singletonMap("ONE", "1"), "%{ONE:one}");
        GrokProcessor processor = new GrokProcessor(grok, fieldName);
        processor.execute(doc);
        assertThat(doc.getFieldValue("one", String.class), equalTo("1"));
    }

    public void testNoMatch() {
        String fieldName = RandomDocumentPicks.randomFieldName(random());
        IngestDocument doc = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        doc.setFieldValue(fieldName, "23");
        Grok grok = new Grok(Collections.singletonMap("ONE", "1"), "%{ONE:one}");
        GrokProcessor processor = new GrokProcessor(grok, fieldName);
        try {
            processor.execute(doc);
            fail();
        } catch (Exception e) {
            assertThat(e.getMessage(), equalTo("Grok expression does not match field value: [23]"));
        }
    }

    public void testNotStringField() {
        String fieldName = RandomDocumentPicks.randomFieldName(random());
        IngestDocument doc = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        doc.setFieldValue(fieldName, 1);
        Grok grok = new Grok(Collections.singletonMap("ONE", "1"), "%{ONE:one}");
        GrokProcessor processor = new GrokProcessor(grok, fieldName);
        try {
            processor.execute(doc);
            fail();
        } catch (Exception e) {
            assertThat(e.getMessage(), equalTo("field [" + fieldName + "] of type [java.lang.Integer] cannot be cast to [java.lang.String]"));
        }
    }

    public void testMissingField() {
        String fieldName = "foo.bar";
        IngestDocument doc = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        Grok grok = new Grok(Collections.singletonMap("ONE", "1"), "%{ONE:one}");
        GrokProcessor processor = new GrokProcessor(grok, fieldName);
        try {
            processor.execute(doc);
            fail();
        } catch (Exception e) {
            assertThat(e.getMessage(), equalTo("field [foo] not present as part of path [foo.bar]"));
        }
    }
}
