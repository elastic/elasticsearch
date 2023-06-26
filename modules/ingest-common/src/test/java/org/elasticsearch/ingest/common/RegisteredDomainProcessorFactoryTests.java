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

import static org.hamcrest.Matchers.equalTo;

public class RegisteredDomainProcessorFactoryTests extends ESTestCase {

    private RegisteredDomainProcessor.Factory factory;

    @Before
    public void init() {
        factory = new RegisteredDomainProcessor.Factory();
    }

    public void testCreate() throws Exception {
        Map<String, Object> config = new HashMap<>();

        String field = randomAlphaOfLength(6);
        config.put("field", field);
        String targetField = randomAlphaOfLength(6);
        config.put("target_field", targetField);
        boolean ignoreMissing = randomBoolean();
        config.put("ignore_missing", ignoreMissing);

        String processorTag = randomAlphaOfLength(10);
        RegisteredDomainProcessor publicSuffixProcessor = factory.create(null, processorTag, null, config);
        assertThat(publicSuffixProcessor.getTag(), equalTo(processorTag));
        assertThat(publicSuffixProcessor.getTargetField(), equalTo(targetField));
        assertThat(publicSuffixProcessor.getIgnoreMissing(), equalTo(ignoreMissing));
    }

    public void testCreateDefaults() throws Exception {
        Map<String, Object> config = new HashMap<>();

        String field = randomAlphaOfLength(6);
        config.put("field", field);

        String processorTag = randomAlphaOfLength(10);
        RegisteredDomainProcessor publicSuffixProcessor = factory.create(null, processorTag, null, config);
        assertThat(publicSuffixProcessor.getTargetField(), equalTo(RegisteredDomainProcessor.Factory.DEFAULT_TARGET_FIELD));
    }

    public void testFieldRequired() throws Exception {
        HashMap<String, Object> config = new HashMap<>();
        String processorTag = randomAlphaOfLength(10);
        try {
            factory.create(null, processorTag, null, config);
            fail("factory create should have failed");
        } catch (ElasticsearchParseException e) {
            assertThat(e.getMessage(), equalTo("[field] required property is missing"));
        }
    }
}
