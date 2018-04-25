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

public class JoinProcessorFactoryTests extends ESTestCase {

    public void testCreate() throws Exception {
        JoinProcessor.Factory factory = new JoinProcessor.Factory();
        Map<String, Object> config = new HashMap<>();
        config.put("field", "field1");
        config.put("separator", "-");
        String processorTag = randomAlphaOfLength(10);
        JoinProcessor joinProcessor = factory.create(null, processorTag, config);
        assertThat(joinProcessor.getTag(), equalTo(processorTag));
        assertThat(joinProcessor.getField(), equalTo("field1"));
        assertThat(joinProcessor.getSeparator(), equalTo("-"));
        assertThat(joinProcessor.getTargetField(), equalTo("field1"));
    }

    public void testCreateNoFieldPresent() throws Exception {
        JoinProcessor.Factory factory = new JoinProcessor.Factory();
        Map<String, Object> config = new HashMap<>();
        config.put("separator", "-");
        try {
            factory.create(null, null, config);
            fail("factory create should have failed");
        } catch (ElasticsearchParseException e) {
            assertThat(e.getMessage(), equalTo("[field] required property is missing"));
        }
    }

    public void testCreateNoSeparatorPresent() throws Exception {
        JoinProcessor.Factory factory = new JoinProcessor.Factory();
        Map<String, Object> config = new HashMap<>();
        config.put("field", "field1");
        try {
            factory.create(null, null, config);
            fail("factory create should have failed");
        } catch (ElasticsearchParseException e) {
            assertThat(e.getMessage(), equalTo("[separator] required property is missing"));
        }
    }

    public void testCreateWithTargetField() throws Exception {
        JoinProcessor.Factory factory = new JoinProcessor.Factory();
        Map<String, Object> config = new HashMap<>();
        config.put("field", "field1");
        config.put("separator", "-");
        config.put("target_field", "target");
        String processorTag = randomAlphaOfLength(10);
        JoinProcessor joinProcessor = factory.create(null, processorTag, config);
        assertThat(joinProcessor.getTag(), equalTo(processorTag));
        assertThat(joinProcessor.getField(), equalTo("field1"));
        assertThat(joinProcessor.getSeparator(), equalTo("-"));
        assertThat(joinProcessor.getTargetField(), equalTo("target"));
    }
}
