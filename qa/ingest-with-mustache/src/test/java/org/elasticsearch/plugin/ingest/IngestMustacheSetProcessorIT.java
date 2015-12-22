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

package org.elasticsearch.plugin.ingest;


import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.ValueSource;
import org.elasticsearch.ingest.processor.Processor;
import org.elasticsearch.ingest.processor.set.SetProcessor;
import org.hamcrest.Matchers;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;

public class IngestMustacheSetProcessorIT extends AbstractMustacheTests {

    public void testExpression() throws Exception {
        SetProcessor processor = createSetProcessor("_index", "text {{var}}");
        assertThat(processor.getValue(), instanceOf(ValueSource.TemplatedValue.class));
        assertThat(processor.getValue().copyAndResolve(Collections.singletonMap("var", "_value")), equalTo("text _value"));
    }

    public void testSetMetadataWithTemplates() throws Exception {
        IngestDocument.MetaData randomMetaData = randomFrom(IngestDocument.MetaData.values());
        Processor processor = createSetProcessor(randomMetaData.getFieldName(), "_value {{field}}");
        IngestDocument ingestDocument = createIngestDocument(Collections.singletonMap("field", "value"));
        processor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue(randomMetaData.getFieldName(), String.class), Matchers.equalTo("_value value"));
    }

    public void testSetWithTemplates() throws Exception {
        IngestDocument.MetaData randomMetaData = randomFrom(IngestDocument.MetaData.INDEX, IngestDocument.MetaData.TYPE, IngestDocument.MetaData.ID);
        Processor processor = createSetProcessor("field{{_type}}", "_value {{" + randomMetaData.getFieldName() + "}}");
        IngestDocument ingestDocument = createIngestDocument(new HashMap<>());
        processor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue("field_type", String.class), Matchers.equalTo("_value " + ingestDocument.getFieldValue(randomMetaData.getFieldName(), String.class)));
    }

    private SetProcessor createSetProcessor(String fieldName, Object fieldValue) throws Exception {
        SetProcessor.Factory factory = new SetProcessor.Factory(templateService);
        Map<String, Object> config = new HashMap<>();
        config.put("field", fieldName);
        config.put("value", fieldValue);
        return factory.create(config);
    }

    private IngestDocument createIngestDocument(Map<String, Object> source) {
        return new IngestDocument("_index", "_type", "_id", null, null, null, null, source);
    }

}
