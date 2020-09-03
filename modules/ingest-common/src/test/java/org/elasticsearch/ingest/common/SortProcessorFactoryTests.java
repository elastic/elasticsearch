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

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.ingest.RandomDocumentPicks;
import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;

public class SortProcessorFactoryTests extends ESTestCase {

    public void testCreate() throws Exception {
        String processorTag = randomAlphaOfLength(10);
        String fieldName = RandomDocumentPicks.randomFieldName(random());

        Map<String, Object> config = new HashMap<>();
        config.put("field", fieldName);

        SortProcessor.Factory factory = new SortProcessor.Factory();
        SortProcessor processor = factory.create(null, processorTag, null, config);
        assertThat(processor.getTag(), equalTo(processorTag));
        assertThat(processor.getField(), equalTo(fieldName));
        assertThat(processor.getOrder(), equalTo(SortProcessor.SortOrder.ASCENDING));
        assertThat(processor.getTargetField(), equalTo(fieldName));
    }

    public void testCreateWithOrder() throws Exception {
        String processorTag = randomAlphaOfLength(10);
        String fieldName = RandomDocumentPicks.randomFieldName(random());

        Map<String, Object> config = new HashMap<>();
        config.put("field", fieldName);
        config.put("order", "desc");

        SortProcessor.Factory factory = new SortProcessor.Factory();
        SortProcessor processor = factory.create(null, processorTag, null, config);
        assertThat(processor.getTag(), equalTo(processorTag));
        assertThat(processor.getField(), equalTo(fieldName));
        assertThat(processor.getOrder(), equalTo(SortProcessor.SortOrder.DESCENDING));
        assertThat(processor.getTargetField(), equalTo(fieldName));
    }

    public void testCreateWithTargetField() throws Exception {
        String processorTag = randomAlphaOfLength(10);
        String fieldName = RandomDocumentPicks.randomFieldName(random());
        String targetFieldName = RandomDocumentPicks.randomFieldName(random());

        Map<String, Object> config = new HashMap<>();
        config.put("field", fieldName);
        config.put("target_field", targetFieldName);

        SortProcessor.Factory factory = new SortProcessor.Factory();
        SortProcessor processor = factory.create(null, processorTag, null, config);
        assertThat(processor.getTag(), equalTo(processorTag));
        assertThat(processor.getField(), equalTo(fieldName));
        assertThat(processor.getOrder(), equalTo(SortProcessor.SortOrder.ASCENDING));
        assertThat(processor.getTargetField(), equalTo(targetFieldName));
    }

    public void testCreateWithInvalidOrder() throws Exception {
        String processorTag = randomAlphaOfLength(10);
        String fieldName = RandomDocumentPicks.randomFieldName(random());

        Map<String, Object> config = new HashMap<>();
        config.put("field", fieldName);
        config.put("order", "invalid");

        SortProcessor.Factory factory = new SortProcessor.Factory();
        try {
            factory.create(null, processorTag, null, config);
            fail("factory create should have failed");
        } catch (ElasticsearchParseException e) {
            assertThat(e.getMessage(), equalTo("[order] Sort direction [invalid] not recognized. Valid values are: [asc, desc]"));
        }
    }

    public void testCreateMissingField() throws Exception {
        SortProcessor.Factory factory = new SortProcessor.Factory();
        Map<String, Object> config = new HashMap<>();
        try {
            factory.create(null, null, null, config);
            fail("factory create should have failed");
        } catch(ElasticsearchParseException e) {
            assertThat(e.getMessage(), equalTo("[field] required property is missing"));
        }
    }
}
