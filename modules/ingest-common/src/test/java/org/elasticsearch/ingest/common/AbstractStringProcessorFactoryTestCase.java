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
import static org.hamcrest.Matchers.is;

public abstract class AbstractStringProcessorFactoryTestCase extends ESTestCase {

    protected abstract AbstractStringProcessor.Factory newFactory();

    protected Map<String, Object> modifyConfig(Map<String, Object> config) {
        return config;
    }

    protected void assertProcessor(AbstractStringProcessor<?> processor) {}

    public void testCreate() throws Exception {
        AbstractStringProcessor.Factory factory = newFactory();
        String fieldName = RandomDocumentPicks.randomFieldName(random());
        String processorTag = randomAlphaOfLength(10);

        Map<String, Object> config = new HashMap<>();
        config.put("field", fieldName);

        AbstractStringProcessor<?> processor = factory.create(null, processorTag, modifyConfig(config));
        assertThat(processor.getTag(), equalTo(processorTag));
        assertThat(processor.getField(), equalTo(fieldName));
        assertThat(processor.isIgnoreMissing(), is(false));
        assertThat(processor.getTargetField(), equalTo(fieldName));
        assertProcessor(processor);
    }

    public void testCreateWithIgnoreMissing() throws Exception {
        AbstractStringProcessor.Factory factory = newFactory();
        String fieldName = RandomDocumentPicks.randomFieldName(random());
        String processorTag = randomAlphaOfLength(10);

        Map<String, Object> config = new HashMap<>();
        config.put("field", fieldName);
        config.put("ignore_missing", true);

        AbstractStringProcessor<?> processor = factory.create(null, processorTag, modifyConfig(config));
        assertThat(processor.getTag(), equalTo(processorTag));
        assertThat(processor.getField(), equalTo(fieldName));
        assertThat(processor.isIgnoreMissing(), is(true));
        assertThat(processor.getTargetField(), equalTo(fieldName));
        assertProcessor(processor);
    }

    public void testCreateWithTargetField() throws Exception {
        AbstractStringProcessor.Factory factory = newFactory();
        String fieldName = RandomDocumentPicks.randomFieldName(random());
        String targetFieldName = RandomDocumentPicks.randomFieldName(random());
        String processorTag = randomAlphaOfLength(10);

        Map<String, Object> config = new HashMap<>();
        config.put("field", fieldName);
        config.put("target_field", targetFieldName);

        AbstractStringProcessor<?> processor = factory.create(null, processorTag, modifyConfig(config));
        assertThat(processor.getTag(), equalTo(processorTag));
        assertThat(processor.getField(), equalTo(fieldName));
        assertThat(processor.isIgnoreMissing(), is(false));
        assertThat(processor.getTargetField(), equalTo(targetFieldName));
        assertProcessor(processor);
    }

    public void testCreateMissingField() throws Exception {
        AbstractStringProcessor.Factory factory = newFactory();
        Map<String, Object> config = new HashMap<>();
        try {
            factory.create(null, null, config);
            fail("factory create should have failed");
        } catch(ElasticsearchParseException e) {
            assertThat(e.getMessage(), equalTo("[field] required property is missing"));
        }
    }
}
