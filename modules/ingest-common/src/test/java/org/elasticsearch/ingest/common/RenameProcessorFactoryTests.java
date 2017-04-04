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
import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;

public class RenameProcessorFactoryTests extends ESTestCase {

    public void testCreate() throws Exception {
        RenameProcessor.Factory factory = new RenameProcessor.Factory();
        Map<String, Object> config = new HashMap<>();
        config.put("field", "old_field");
        config.put("target_field", "new_field");
        String processorTag = randomAlphaOfLength(10);
        RenameProcessor renameProcessor = factory.create(null, processorTag, config);
        assertThat(renameProcessor.getTag(), equalTo(processorTag));
        assertThat(renameProcessor.getField(), equalTo("old_field"));
        assertThat(renameProcessor.getTargetField(), equalTo("new_field"));
        assertThat(renameProcessor.isIgnoreMissing(), equalTo(false));
    }

    public void testCreateWithIgnoreMissing() throws Exception {
        RenameProcessor.Factory factory = new RenameProcessor.Factory();
        Map<String, Object> config = new HashMap<>();
        config.put("field", "old_field");
        config.put("target_field", "new_field");
        config.put("ignore_missing", true);
        String processorTag = randomAlphaOfLength(10);
        RenameProcessor renameProcessor = factory.create(null, processorTag, config);
        assertThat(renameProcessor.getTag(), equalTo(processorTag));
        assertThat(renameProcessor.getField(), equalTo("old_field"));
        assertThat(renameProcessor.getTargetField(), equalTo("new_field"));
        assertThat(renameProcessor.isIgnoreMissing(), equalTo(true));
    }

    public void testCreateNoFieldPresent() throws Exception {
        RenameProcessor.Factory factory = new RenameProcessor.Factory();
        Map<String, Object> config = new HashMap<>();
        config.put("target_field", "new_field");
        try {
            factory.create(null, null, config);
            fail("factory create should have failed");
        } catch(ElasticsearchParseException e) {
            assertThat(e.getMessage(), equalTo("[field] required property is missing"));
        }
    }

    public void testCreateNoToPresent() throws Exception {
        RenameProcessor.Factory factory = new RenameProcessor.Factory();
        Map<String, Object> config = new HashMap<>();
        config.put("field", "old_field");
        try {
            factory.create(null, null, config);
            fail("factory create should have failed");
        } catch(ElasticsearchParseException e) {
            assertThat(e.getMessage(), equalTo("[target_field] required property is missing"));
        }
    }
}
