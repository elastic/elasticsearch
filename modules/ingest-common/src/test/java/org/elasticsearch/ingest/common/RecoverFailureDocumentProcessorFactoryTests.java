/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.common;

import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class RecoverFailureDocumentProcessorFactoryTests extends ESTestCase {

    private RecoverFailureDocumentProcessor.Factory factory;

    @Before
    public void init() {
        factory = new RecoverFailureDocumentProcessor.Factory();
    }

    public void testCreate() throws Exception {
        RecoverFailureDocumentProcessor.Factory factory = new RecoverFailureDocumentProcessor.Factory();
        Map<String, Object> config = new HashMap<>();

        String processorTag = randomAlphaOfLength(10);
        RecoverFailureDocumentProcessor processor = factory.create(null, processorTag, null, config, null);

        assertThat(processor.getTag(), equalTo(processorTag));
    }

    public void testCreateWithDescription() throws Exception {
        RecoverFailureDocumentProcessor.Factory factory = new RecoverFailureDocumentProcessor.Factory();
        Map<String, Object> config = new HashMap<>();

        String processorTag = randomAlphaOfLength(10);
        String description = randomAlphaOfLength(20);
        RecoverFailureDocumentProcessor processor = factory.create(null, processorTag, description, config, null);

        assertThat(processor.getTag(), equalTo(processorTag));
        assertThat(processor.getDescription(), equalTo(description));
    }

}
