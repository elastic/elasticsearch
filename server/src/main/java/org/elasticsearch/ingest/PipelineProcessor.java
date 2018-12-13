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

import java.util.Map;

public class PipelineProcessor extends AbstractProcessor {

    public static final String TYPE = "pipeline";

    private final String pipelineName;

    private final IngestService ingestService;

    private PipelineProcessor(String tag, String pipelineName, IngestService ingestService) {
        super(tag);
        this.pipelineName = pipelineName;
        this.ingestService = ingestService;
    }

    @Override
    public IngestDocument execute(IngestDocument ingestDocument) throws Exception {
        Pipeline pipeline = ingestService.getPipeline(pipelineName);
        if (pipeline == null) {
            throw new IllegalStateException("Pipeline processor configured for non-existent pipeline [" + pipelineName + ']');
        }
        return ingestDocument.executePipeline(pipeline);
    }

    Pipeline getPipeline(){
        return ingestService.getPipeline(pipelineName);
    }

    @Override
    public String getType() {
        return TYPE;
    }

    String getPipelineName() {
        return pipelineName;
    }

    public static final class Factory implements Processor.Factory {

        private final IngestService ingestService;

        public Factory(IngestService ingestService) {
            this.ingestService = ingestService;
        }

        @Override
        public PipelineProcessor create(Map<String, Processor.Factory> registry, String processorTag,
            Map<String, Object> config) throws Exception {
            String pipeline =
                ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "name");
            return new PipelineProcessor(processorTag, pipeline, ingestService);
        }
    }
}
