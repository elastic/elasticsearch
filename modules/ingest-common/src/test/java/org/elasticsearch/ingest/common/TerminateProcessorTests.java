/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.common;

import org.elasticsearch.ingest.CompoundProcessor;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Pipeline;
import org.elasticsearch.ingest.TestTemplateService;
import org.elasticsearch.ingest.ValueSource;
import org.elasticsearch.test.ESTestCase;

import java.util.Map;

import static org.elasticsearch.ingest.RandomDocumentPicks.randomIngestDocument;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class TerminateProcessorTests extends ESTestCase {

    public void testTerminateInPipeline() throws Exception {
        Pipeline pipeline = new Pipeline(
            "my-pipeline",
            null,
            null,
            null,
            new CompoundProcessor(
                new SetProcessor(
                    "before-set",
                    "Sets before field to true",
                    new TestTemplateService.MockTemplateScript.Factory("before"),
                    ValueSource.wrap(true, TestTemplateService.instance()),
                    null
                ),
                new TerminateProcessor("terminate", "terminates the pipeline"),
                new SetProcessor(
                    "after-set",
                    "Sets after field to true",
                    new TestTemplateService.MockTemplateScript.Factory("after"),
                    ValueSource.wrap(true, TestTemplateService.instance()),
                    null
                )
            )
        );
        IngestDocument input = randomIngestDocument(random(), Map.of("foo", "bar"));
        PipelineOutput output = new PipelineOutput();

        pipeline.execute(input, output::set);

        assertThat(output.exception, nullValue());
        // We expect the before-set processor to have run, but not the after-set one:
        assertThat(output.document.getSource(), is(Map.of("foo", "bar", "before", true)));
    }

    private static class PipelineOutput {
        IngestDocument document;
        Exception exception;

        void set(IngestDocument document, Exception exception) {
            this.document = document;
            this.exception = exception;
        }
    }
}
