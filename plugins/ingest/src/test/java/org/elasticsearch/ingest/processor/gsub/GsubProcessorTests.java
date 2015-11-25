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

package org.elasticsearch.ingest.processor.gsub;

import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.RandomDocumentPicks;
import org.elasticsearch.ingest.processor.Processor;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Pattern;

import static org.hamcrest.Matchers.equalTo;

public class GsubProcessorTests extends ESTestCase {

    public void testGsub() throws IOException {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        int numFields = randomIntBetween(1, 5);
        List<GsubExpression> expressions = new ArrayList<>();
        for (int i = 0; i < numFields; i++) {
            String fieldName = RandomDocumentPicks.addRandomField(random(), ingestDocument, "127.0.0.1");
            expressions.add(new GsubExpression(fieldName, Pattern.compile("\\."), "-"));
        }
        Processor processor = new GsubProcessor(expressions);
        processor.execute(ingestDocument);
        for (GsubExpression expression : expressions) {
            assertThat(ingestDocument.getFieldValue(expression.getFieldName(), String.class), equalTo("127-0-0-1"));
        }
    }

    public void testGsubNotAStringValue() throws IOException {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        String fieldName = RandomDocumentPicks.randomFieldName(random());
        ingestDocument.setFieldValue(fieldName, 123);
        List<GsubExpression> gsubExpressions = Collections.singletonList(new GsubExpression(fieldName, Pattern.compile("\\."), "-"));
        Processor processor = new GsubProcessor(gsubExpressions);
        try {
            processor.execute(ingestDocument);
            fail("processor execution should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("field [" + fieldName + "] of type [java.lang.Integer] cannot be cast to [java.lang.String]"));
        }
    }

    public void testGsubNullValue() throws IOException {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        String fieldName = RandomDocumentPicks.randomFieldName(random());
        List<GsubExpression> gsubExpressions = Collections.singletonList(new GsubExpression(fieldName, Pattern.compile("\\."), "-"));
        Processor processor = new GsubProcessor(gsubExpressions);
        try {
            processor.execute(ingestDocument);
            fail("processor execution should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("field [" + fieldName + "] is null, cannot match pattern."));
        }
    }
}
