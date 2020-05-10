/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.ingest.common;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;

public class CopyProcessorFactoryTests extends ESTestCase {

    private static final CopyProcessor.Factory FACTORY = new CopyProcessor.Factory();

    public void testCreate() throws Exception {
        String processorTag = randomAlphaOfLength(10);
        String randomField = randomAlphaOfLength(10);
        String randomTargetField = randomAlphaOfLength(5);
        Map<String, Object> config = new HashMap<>();
        config.put("field", randomField);
        config.put("target_field", randomTargetField);
        CopyProcessor copyProcessor = FACTORY.create(null, processorTag, config);
        assertThat(copyProcessor.getTag(), equalTo(processorTag));
        assertThat(copyProcessor.getField(), equalTo(randomField));
        assertThat(copyProcessor.getTargetField(), equalTo(randomTargetField));
    }

    public void testCreateWithAddToRoot() throws Exception {
        String processorTag = randomAlphaOfLength(10);
        String randomField = randomAlphaOfLength(10);
        Map<String, Object> config = new HashMap<>();
        config.put("field", randomField);
        config.put("add_to_root", true);
        CopyProcessor copyProcessor = FACTORY.create(null, processorTag, config);
        assertThat(copyProcessor.getTag(), equalTo(processorTag));
        assertThat(copyProcessor.getField(), equalTo(randomField));
        assertTrue(copyProcessor.isAddToRoot());
    }

    public void testCreateWithMissingField() throws Exception {
        Map<String, Object> config = new HashMap<>();
        String processorTag = randomAlphaOfLength(10);
        ElasticsearchException exception = expectThrows(
            ElasticsearchParseException.class,
            () -> FACTORY.create(null, processorTag, config));
        assertThat(exception.getMessage(), equalTo("[field] required property is missing"));
    }

    public void testCreateWithIgnoreMissing() throws Exception {
        Map<String, Object> config = new HashMap<>();
        config.put("field", "old_field");
        config.put("target_field", "new_field");
        config.put("ignore_missing", true);
        String processorTag = randomAlphaOfLength(10);
        CopyProcessor copyProcessor = FACTORY.create(null, processorTag, config);
        assertThat(copyProcessor.getTag(), equalTo(processorTag));
        assertThat(copyProcessor.getField(), equalTo("old_field"));
        assertThat(copyProcessor.getTargetField(), equalTo("new_field"));
        assertThat(copyProcessor.isIgnoreMissing(), equalTo(true));
    }

    public void testCreateWithoutTargetFieldAndAddToRoot() throws Exception {
        String randomField = randomAlphaOfLength(10);
        Map<String, Object> config = new HashMap<>();
        config.put("field", randomField);
        ElasticsearchException exception = expectThrows(ElasticsearchParseException.class,
            () -> FACTORY.create(null, randomAlphaOfLength(10), config));
        assertThat(exception.getMessage(), equalTo("[target_field] either `target_field` or `add_to_root` must be set"));
    }

    public void testCreateWithBothTargetFieldAndAddToRoot() throws Exception {
        String randomField = randomAlphaOfLength(10);
        String randomTargetField = randomAlphaOfLength(5);
        Map<String, Object> config = new HashMap<>();
        config.put("field", randomField);
        config.put("target_field", randomTargetField);
        config.put("add_to_root", true);
        ElasticsearchException exception = expectThrows(ElasticsearchParseException.class,
            () -> FACTORY.create(null, randomAlphaOfLength(10), config));
        assertThat(exception.getMessage(), equalTo("[target_field] cannot set `target_field` while also setting `add_to_root` to true"));
    }
}
