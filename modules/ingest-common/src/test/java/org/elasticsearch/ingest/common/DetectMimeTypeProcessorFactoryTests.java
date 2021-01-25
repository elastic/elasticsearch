/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.common;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;
import java.util.HashMap;
import java.util.Map;
import static org.hamcrest.CoreMatchers.equalTo;

public class DetectMimeTypeProcessorFactoryTests extends ESTestCase {

    private DetectMimeTypeProcessor.Factory factory;

    @Before
    public void init() {
        factory = new DetectMimeTypeProcessor.Factory();
    }

    public void testCreate() throws Exception {
        Map<String, Object> config = new HashMap<>();

        String field = randomAlphaOfLength(6);
        config.put("field", field);
        String targetField = randomAlphaOfLength(6);
        config.put("target_field", targetField);
        boolean isBase64 = randomBoolean();
        config.put("base64_encoded", isBase64);
        boolean ignoreMissing = randomBoolean();
        config.put("ignore_missing", ignoreMissing);

        String processorTag = randomAlphaOfLength(10);
        DetectMimeTypeProcessor mimeTypeProcessor = factory.create(null, processorTag, null, config);
        assertThat(mimeTypeProcessor.getTag(), equalTo(processorTag));
        assertThat(mimeTypeProcessor.getField(), equalTo(field));
        assertThat(mimeTypeProcessor.getTargetField(), equalTo(targetField));
        assertThat(mimeTypeProcessor.getIsBase64(), equalTo(isBase64));
        assertThat(mimeTypeProcessor.getIgnoreMissing(), equalTo(ignoreMissing));
    }

    public void testRequiredField() throws Exception {
        HashMap<String, Object> config = new HashMap<>();
        String processorTag = randomAlphaOfLength(10);
        try {
            factory.create(null, processorTag, null, config);
            fail("factory create should have failed");
        } catch (ElasticsearchParseException e) {
            assertThat(e.getMessage(), equalTo("[field] required property is missing"));
        }
    }

    public void testRequiredTargetField() throws Exception {
        HashMap<String, Object> config = new HashMap<>();
        String field = randomAlphaOfLength(6);
        config.put("field", field);

        String processorTag = randomAlphaOfLength(10);
        try {
            factory.create(null, processorTag, null, config);
            fail("factory create should have failed");
        } catch (ElasticsearchParseException e) {
            assertThat(e.getMessage(), equalTo("[target_field] required property is missing"));
        }
    }

    public void testDefaults() throws Exception {
        HashMap<String, Object> config = new HashMap<>();
        String field = randomAlphaOfLength(6);
        config.put("field", field);
        String targetField = randomAlphaOfLength(6);
        config.put("target_field", targetField);

        DetectMimeTypeProcessor mimeTypeProcessor = factory.create(null, null, null, config);
        assertThat(mimeTypeProcessor.getIsBase64(), equalTo(false));
        assertThat(mimeTypeProcessor.getIgnoreMissing(), equalTo(true));
    }
}
