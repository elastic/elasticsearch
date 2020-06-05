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

package org.elasticsearch.ingest;

import org.elasticsearch.script.TemplateScript;

import java.util.Map;
import java.util.function.BiConsumer;

public class PipelineProcessor extends AbstractProcessor {

    public static final String TYPE = "pipeline";

    private final TemplateScript.Factory pipelineTemplate;
    private final IngestService ingestService;

    PipelineProcessor(String tag, TemplateScript.Factory pipelineTemplate, IngestService ingestService) {
        super(tag);
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
            handler.accept(null,
                new IllegalStateException("Pipeline processor configured for non-existent pipeline [" + pipelineName + ']'));
        }
    }

    @Override
    public IngestDocument execute(IngestDocument ingestDocument) throws Exception {
        throw new UnsupportedOperationException("this method should not get executed");
    }

    Pipeline getPipeline(IngestDocument ingestDocument) {
        String pipelineName = ingestDocument.renderTemplate(this.pipelineTemplate);
        return ingestService.getPipeline(pipelineName);
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
        public PipelineProcessor create(Map<String, Processor.Factory> registry, String processorTag,
            Map<String, Object> config) throws Exception {
            TemplateScript.Factory pipelineTemplate =
                ConfigurationUtils.readTemplateProperty(TYPE, processorTag, config, "name", ingestService.getScriptService());
            return new PipelineProcessor(processorTag, pipelineTemplate, ingestService);
        }
    }
}
