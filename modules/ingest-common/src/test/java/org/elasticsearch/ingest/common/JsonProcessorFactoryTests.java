/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.common;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;

public class JsonProcessorFactoryTests extends ESTestCase {

    private static final JsonProcessor.Factory FACTORY = new JsonProcessor.Factory();

    public void testCreate() throws Exception {
        String processorTag = randomAlphaOfLength(10);
        String randomField = randomAlphaOfLength(10);
        String randomTargetField = randomAlphaOfLength(5);
        Map<String, Object> config = new HashMap<>();
        config.put("field", randomField);
        config.put("target_field", randomTargetField);
        JsonProcessor jsonProcessor = FACTORY.create(null, processorTag, null, config);
        assertThat(jsonProcessor.getTag(), equalTo(processorTag));
        assertThat(jsonProcessor.getField(), equalTo(randomField));
        assertThat(jsonProcessor.getTargetField(), equalTo(randomTargetField));
    }

    public void testCreateWithAddToRoot() throws Exception {
        String processorTag = randomAlphaOfLength(10);
        String randomField = randomAlphaOfLength(10);
        Map<String, Object> config = new HashMap<>();
        config.put("field", randomField);
        config.put("add_to_root", true);
        JsonProcessor jsonProcessor = FACTORY.create(null, processorTag, null, config);
        assertThat(jsonProcessor.getTag(), equalTo(processorTag));
        assertThat(jsonProcessor.getField(), equalTo(randomField));
        assertThat(jsonProcessor.getTargetField(), equalTo(randomField));
        assertTrue(jsonProcessor.isAddToRoot());
    }

    public void testCreateWithDefaultTarget() throws Exception {
        String processorTag = randomAlphaOfLength(10);
        String randomField = randomAlphaOfLength(10);
        Map<String, Object> config = new HashMap<>();
        config.put("field", randomField);
        JsonProcessor jsonProcessor = FACTORY.create(null, processorTag, null, config);
        assertThat(jsonProcessor.getTag(), equalTo(processorTag));
        assertThat(jsonProcessor.getField(), equalTo(randomField));
        assertThat(jsonProcessor.getTargetField(), equalTo(randomField));
    }

    public void testCreateWithMissingField() throws Exception {
        Map<String, Object> config = new HashMap<>();
        String processorTag = randomAlphaOfLength(10);
        ElasticsearchException exception = expectThrows(
            ElasticsearchParseException.class,
            () -> FACTORY.create(null, processorTag, null, config)
        );
        assertThat(exception.getMessage(), equalTo("[field] required property is missing"));
    }

    public void testCreateWithBothTargetFieldAndAddToRoot() throws Exception {
        String randomField = randomAlphaOfLength(10);
        String randomTargetField = randomAlphaOfLength(5);
        Map<String, Object> config = new HashMap<>();
        config.put("field", randomField);
        config.put("target_field", randomTargetField);
        config.put("add_to_root", true);
        ElasticsearchException exception = expectThrows(
            ElasticsearchParseException.class,
            () -> FACTORY.create(null, randomAlphaOfLength(10), null, config)
        );
        assertThat(exception.getMessage(), equalTo("[target_field] Cannot set a target field while also setting `add_to_root` to true"));
    }

    public void testReplaceMergeStrategy() throws Exception {
        JsonProcessor jsonProcessor = getJsonProcessorWithMergeStrategy(null, true);
        assertThat(jsonProcessor.getAddToRootConflictStrategy(), equalTo(JsonProcessor.ConflictStrategy.REPLACE));

        jsonProcessor = getJsonProcessorWithMergeStrategy("replace", true);
        assertThat(jsonProcessor.getAddToRootConflictStrategy(), equalTo(JsonProcessor.ConflictStrategy.REPLACE));
    }

    public void testRecursiveMergeStrategy() throws Exception {
        JsonProcessor jsonProcessor = getJsonProcessorWithMergeStrategy("merge", true);
        assertThat(jsonProcessor.getAddToRootConflictStrategy(), equalTo(JsonProcessor.ConflictStrategy.MERGE));
    }

    public void testMergeStrategyWithoutAddToRoot() throws Exception {
        ElasticsearchException exception = expectThrows(
            ElasticsearchParseException.class,
            () -> getJsonProcessorWithMergeStrategy("replace", false)
        );
        assertThat(
            exception.getMessage(),
            equalTo("[add_to_root_conflict_strategy] Cannot set `add_to_root_conflict_strategy` if `add_to_root` is false")
        );
    }

    public void testUnknownMergeStrategy() throws Exception {
        ElasticsearchException exception = expectThrows(
            ElasticsearchParseException.class,
            () -> getJsonProcessorWithMergeStrategy("foo", true)
        );
        assertThat(
            exception.getMessage(),
            equalTo("[add_to_root_conflict_strategy] conflict strategy [foo] not supported, cannot convert field.")
        );
    }

    private JsonProcessor getJsonProcessorWithMergeStrategy(String mergeStrategy, boolean addToRoot) throws Exception {
        String randomField = randomAlphaOfLength(10);
        Map<String, Object> config = new HashMap<>();
        config.put("field", randomField);
        config.put("add_to_root", addToRoot);
        if (mergeStrategy != null) {
            config.put("add_to_root_conflict_strategy", mergeStrategy);
        }
        return FACTORY.create(null, randomAlphaOfLength(10), null, config);
    }
}
