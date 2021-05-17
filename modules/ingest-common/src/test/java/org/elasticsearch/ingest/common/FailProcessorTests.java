/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.common;

import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.ingest.RandomDocumentPicks;
import org.elasticsearch.ingest.TestTemplateService;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;

public class FailProcessorTests extends ESTestCase {

    public void test() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        String message = randomAlphaOfLength(10);
        Processor processor = new FailProcessor(randomAlphaOfLength(10),
                null, new TestTemplateService.MockTemplateScript.Factory(message));
        try {
            processor.execute(ingestDocument);
            fail("fail processor should throw an exception");
        } catch (FailProcessorException e) {
            assertThat(e.getMessage(), equalTo(message));
        }
    }
}
