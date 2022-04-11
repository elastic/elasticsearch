/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest;

import org.elasticsearch.script.TemplateScript;

import java.util.Map;
import java.util.function.BiConsumer;

public class PipelineProcessor extends AbstractProcessor {

    public static final String TYPE = "pipeline";

    private final TemplateScript.Factory pipelineTemplate;
    private final IngestService ingestService;

    PipelineProcessor(String tag, String description, TemplateScript.Factory pipelineTemplate, IngestService ingestService) {
        super(tag, description);
        this.pipelineTemplate = pipelineTemplate;
        this.ingestService = ingestService;
    }

    @Override
    public void execute(IngestDocument ingestDocument, BiConsumer<IngestDocument, Exception> handler) {
        String pipelineName = ingestDocument.renderTemplate(this.pipelineTemplate);
        Pipeline pipeline = ingestService.getPipeline(pipelineName);
        if (pipeline != null) {
            ingestDocument.executePipeline(pipeline, handler);
        } else {
            handler.accept(
                null,
                new IllegalStateException("Pipeline processor configured for non-existent pipeline [" + pipelineName + ']')
            );
        }
    }

    @Override
    public IngestDocument execute(IngestDocument ingestDocument) throws Exception {
        throw new UnsupportedOperationException("this method should not get executed");
    }

    Pipeline getPipeline(IngestDocument ingestDocument) {
        return ingestService.getPipeline(getPipelineToCallName(ingestDocument));
    }

    String getPipelineToCallName(IngestDocument ingestDocument) {
        return ingestDocument.renderTemplate(this.pipelineTemplate);
    }

    @Override
    public String getType() {
        return TYPE;
    }

    TemplateScript.Factory getPipelineTemplate() {
        return pipelineTemplate;
    }

    public static final class Factory implements Processor.Factory {

        private final IngestService ingestService;

        public Factory(IngestService ingestService) {
            this.ingestService = ingestService;
        }

        @Override
        public PipelineProcessor create(
            Map<String, Processor.Factory> registry,
            String processorTag,
            String description,
            Map<String, Object> config
        ) throws Exception {
            TemplateScript.Factory pipelineTemplate = ConfigurationUtils.readTemplateProperty(
                TYPE,
                processorTag,
                config,
                "name",
                ingestService.getScriptService()
            );
            return new PipelineProcessor(processorTag, description, pipelineTemplate, ingestService);
        }
    }
}
